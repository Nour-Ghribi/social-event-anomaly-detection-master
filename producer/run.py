import json
import os
from pathlib import Path

from kafka import KafkaProducer

from generator import ProducerSettings, SocialPostFactory


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def main() -> None:
    settings = ProducerSettings(
        target_keyword=os.getenv("TARGET_KEYWORD", "manifestation"),
        run_seconds=env_int("RUN_SECONDS", 80),
        posts_per_second=env_int("POSTS_PER_SECOND", 4),
        spike_start_after_seconds=env_int("SPIKE_START_AFTER_SECONDS", 25),
        spike_duration_seconds=env_int("SPIKE_DURATION_SECONDS", 20),
        spike_multiplier=env_int("SPIKE_MULTIPLIER", 6),
        noise_spike_language=os.getenv("NOISE_SPIKE_LANGUAGE", "en"),
    )
    config_path = Path("/app/data/config.json")
    factory = SocialPostFactory(config_path, settings)
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    topic = os.getenv("KAFKA_TOPIC", "social-posts")

    total_messages = 0
    print(f"Starting producer for {settings.run_seconds}s on topic {topic}.")
    for batch in factory.generate():
        for post in batch:
            producer.send(topic, key=post["lang"], value=post)
            total_messages += 1
        producer.flush()
        fr_hits = sum(1 for post in batch if post["lang"] == "fr" and post["contains_target_keyword"])
        print(
            "Published batch:",
            {
                "size": len(batch),
                "fr_keyword_hits": fr_hits,
                "modes": sorted({post["source_mode"] for post in batch}),
            },
        )

    producer.flush()
    producer.close()
    print(f"Producer finished after publishing {total_messages} messages.")


if __name__ == "__main__":
    main()
