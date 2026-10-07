# Streaming Social Event Anomaly Detection

A reproducible streaming data pipeline that detects abnormal social activity from a synthetic multilingual event feed.

The project simulates social media posts, streams them through Kafka, processes them with Spark Structured Streaming, and stores detected anomalies in PostgreSQL.

```text
Python Producer -> Kafka -> Spark Structured Streaming -> PostgreSQL
```

## Project Goal

The goal is to detect a sudden spike in social posts about a specific topic while avoiding false positives from unrelated languages or noisy messages.

The default scenario focuses on French posts containing the keyword:

```text
manifestation
```

During execution, the producer generates normal multilingual traffic, then injects a controlled French-language spike. The streaming job should detect this spike as an anomaly.

## Architecture

```text
Synthetic social posts
        |
        v
Python producer
        |
        v
Kafka topic: social-posts
        |
        v
Spark Structured Streaming
        |
        v
Dynamic baseline anomaly detector
        |
        v
PostgreSQL tables
```

## Tech Stack

- Python 3.11
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Docker and Docker Compose
- `unittest` for validation tests

## Detection Strategy

The detector works with time-based windows:

| Parameter | Default value |
| --- | --- |
| Target keyword | `manifestation` |
| Filtered language | `fr` |
| Window duration | `15 seconds` |
| Watermark | `30 seconds` |
| Baseline history | `6 previous windows` |

For each window, Spark counts the number of French posts containing the target keyword.

The anomaly threshold is calculated dynamically:

```text
max(mean + 3 * stddev, mean * 2, 10)
```

An event window is marked as anomalous when its count is greater than or equal to this threshold after enough historical windows have been collected.

## Database Tables

The PostgreSQL initialization script creates two main tables:

| Table | Description |
| --- | --- |
| `window_metrics` | Stores every processed window with counts, baseline values and detection status |
| `anomalies` | Stores only the detected abnormal windows |

## Project Structure

```text
.
|-- data/
|   `-- config.json              # Languages and phrase templates
|-- db/
|   `-- init.sql                 # PostgreSQL schema
|-- docs/
|   `-- rapport_methode.md       # Method notes
|-- producer/
|   |-- generator.py             # Synthetic post generator
|   `-- run.py                   # Kafka producer entry point
|-- scripts/
|   `-- check_results.sh         # Database result checker
|-- spark_app/
|   |-- detection.py             # Dynamic baseline detector
|   `-- job.py                   # Spark streaming job
|-- tests/
|   `-- test_detection.py        # Unit tests
|-- docker-compose.yml
`-- README.md
```

## How to Run

Start the full pipeline:

```bash
docker compose up -d
```

Follow the producer and Spark logs:

```bash
docker compose logs -f producer spark
```

The Spark job stops after detecting the first anomaly or after the configured timeout.

## Verification

Run the unit tests:

```bash
python -m unittest discover -s tests -v
```

Check the stored PostgreSQL results after the containers have run:

```bash
./scripts/check_results.sh
```

Expected result:

- at least one row in `window_metrics`
- at least one row in `anomalies`
- anomalies should match `keyword=manifestation` and `lang=fr`

## Configuration

The main runtime settings are defined in `docker-compose.yml` through environment variables:

| Variable | Description |
| --- | --- |
| `TARGET_KEYWORD` | Keyword to track |
| `FILTER_LANGUAGE` | Language kept by the streaming filter |
| `WINDOW_DURATION_SECONDS` | Aggregation window duration |
| `WATERMARK_SECONDS` | Late data tolerance |
| `BASELINE_WINDOWS` | Number of previous windows used for the baseline |
| `POSTS_PER_SECOND` | Normal traffic generation rate |
| `SPIKE_START_AFTER_SECONDS` | Delay before injecting the spike |
| `SPIKE_DURATION_SECONDS` | Spike duration |
| `SPIKE_MULTIPLIER` | Traffic multiplier during the spike |

## Clean Up

Stop the containers:

```bash
docker compose down
```

Remove containers and volumes:

```bash
docker compose down -v
```

## What This Project Demonstrates

- real-time ingestion with Kafka
- streaming aggregation with Spark
- event-time processing with watermarks
- dynamic anomaly detection
- persistence of streaming results in PostgreSQL
- reproducible local execution with Docker Compose

## Author

[Nour Ghribi](https://github.com/Nour-Ghribi)
