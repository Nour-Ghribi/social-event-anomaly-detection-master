import os
from datetime import datetime

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, from_json, lit, to_timestamp, window
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from detection import DynamicBaselineDetector, WindowSummary


TARGET_KEYWORD = os.getenv("TARGET_KEYWORD", "manifestation")
FILTER_LANGUAGE = os.getenv("FILTER_LANGUAGE", "fr")
WINDOW_DURATION_SECONDS = int(os.getenv("WINDOW_DURATION_SECONDS", "15"))
WATERMARK_SECONDS = int(os.getenv("WATERMARK_SECONDS", "30"))
BASELINE_WINDOWS = int(os.getenv("BASELINE_WINDOWS", "6"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social-posts")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "social_events"),
    "user": os.getenv("POSTGRES_USER", "social"),
    "password": os.getenv("POSTGRES_PASSWORD", "social"),
}

detector = DynamicBaselineDetector(baseline_windows=BASELINE_WINDOWS)
stop_after_first_anomaly = True


def get_db_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def persist_batch(batch_df, batch_id: int) -> None:
    global stop_after_first_anomaly

    rows = sorted(batch_df.collect(), key=lambda row: row.window.start)
    if not rows:
        return

    conn = get_db_connection()
    anomaly_seen = False

    try:
        with conn:
            with conn.cursor() as cursor:
                for row in rows:
                    summary = WindowSummary(
                        keyword=row.keyword,
                        lang=row.lang,
                        window_start=row.window.start,
                        window_end=row.window.end,
                        event_count=row.event_count,
                    )
                    result = detector.evaluate(summary)
                    cursor.execute(
                        """
                        INSERT INTO window_metrics (
                            keyword, lang, window_start, window_end, event_count,
                            baseline_mean, baseline_stddev, threshold,
                            is_anomaly, detection_reason, processed_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (keyword, lang, window_start, window_end)
                        DO UPDATE SET
                            event_count = EXCLUDED.event_count,
                            baseline_mean = EXCLUDED.baseline_mean,
                            baseline_stddev = EXCLUDED.baseline_stddev,
                            threshold = EXCLUDED.threshold,
                            is_anomaly = EXCLUDED.is_anomaly,
                            detection_reason = EXCLUDED.detection_reason,
                            processed_at = NOW()
                        """,
                        (
                            summary.keyword,
                            summary.lang,
                            summary.window_start,
                            summary.window_end,
                            summary.event_count,
                            result.baseline_mean,
                            result.baseline_stddev,
                            result.threshold,
                            result.is_anomaly,
                            result.detection_reason,
                        ),
                    )

                    if result.is_anomaly:
                        anomaly_seen = True
                        cursor.execute(
                            """
                            INSERT INTO anomalies (
                                keyword, lang, window_start, window_end, event_count,
                                baseline_mean, baseline_stddev, threshold, detection_reason
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                summary.keyword,
                                summary.lang,
                                summary.window_start,
                                summary.window_end,
                                summary.event_count,
                                result.baseline_mean,
                                result.baseline_stddev,
                                result.threshold,
                                result.detection_reason,
                            ),
                        )
                        print(
                            "Anomaly persisted:",
                            {
                                "batch_id": batch_id,
                                "window_start": summary.window_start.isoformat(),
                                "window_end": summary.window_end.isoformat(),
                                "event_count": summary.event_count,
                                "threshold": result.threshold,
                            },
                        )
    finally:
        conn.close()

    if anomaly_seen and stop_after_first_anomaly:
        print("At least one anomaly detected. Stopping streaming query.")
        for stream in spark.streams.active:
            stream.stop()


spark = (
    SparkSession.builder.appName("social-event-anomaly-detection")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                "org.postgresql:postgresql:42.7.3",
            ]
        ),
    )
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("post_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("lang", StringType(), False),
        StructField("text", StringType(), False),
        StructField("contains_target_keyword", BooleanType(), False),
        StructField("source_mode", StringType(), False),
    ]
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

posts = (
    raw_stream.selectExpr("CAST(value AS STRING) AS json_payload")
    .select(from_json(col("json_payload"), schema).alias("payload"))
    .select("payload.*")
    .withColumn("event_ts", to_timestamp("event_time"))
    .withWatermark("event_ts", f"{WATERMARK_SECONDS} seconds")
)

filtered_posts = posts.filter(
    (col("lang") == FILTER_LANGUAGE)
    & (col("contains_target_keyword") == lit(True))
    & (expr(f"lower(text) like '%{TARGET_KEYWORD.lower()}%'"))
)

windowed_counts = (
    filtered_posts.groupBy(
        window(col("event_ts"), f"{WINDOW_DURATION_SECONDS} seconds"),
        col("lang"),
    )
    .agg(count("*").alias("event_count"))
    .withColumn("keyword", lit(TARGET_KEYWORD))
    .select("keyword", "lang", "window", "event_count")
)

query = (
    windowed_counts.writeStream.outputMode("append")
    .foreachBatch(persist_batch)
    .option("checkpointLocation", "/tmp/social_event_checkpoint")
    .trigger(processingTime="5 seconds")
    .start()
)

print(
    "Streaming job started at",
    datetime.utcnow().isoformat(),
    "watching topic",
    KAFKA_TOPIC,
)

query.awaitTermination(timeout=300)
query.stop()
spark.stop()
