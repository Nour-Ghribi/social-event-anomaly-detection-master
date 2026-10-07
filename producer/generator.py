import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(slots=True)
class ProducerSettings:
    target_keyword: str = "manifestation"
    run_seconds: int = 80
    posts_per_second: int = 4
    spike_start_after_seconds: int = 25
    spike_duration_seconds: int = 20
    spike_multiplier: int = 6
    noise_spike_language: str = "en"


class SocialPostFactory:
    def __init__(self, config_path: Path, settings: ProducerSettings) -> None:
        self.settings = settings
        self.config = json.loads(config_path.read_text(encoding="utf-8"))
        self.languages = self.config["languages"]
        self.templates = self.config["phrase_templates"]

    def _in_spike(self, elapsed_seconds: float) -> bool:
        return (
            self.settings.spike_start_after_seconds
            <= elapsed_seconds
            < self.settings.spike_start_after_seconds + self.settings.spike_duration_seconds
        )

    def build_batch(self, elapsed_seconds: float) -> list[dict]:
        in_spike = self._in_spike(elapsed_seconds)
        multiplier = self.settings.spike_multiplier if in_spike else 1
        batch_size = self.settings.posts_per_second * multiplier
        batch: list[dict] = []

        for _ in range(batch_size):
            lang = random.choices(self.languages, weights=[0.55, 0.25, 0.20], k=1)[0]
            source_mode = "baseline"
            text = random.choice(self.templates[lang])
            contains_keyword = lang == "fr" and self.settings.target_keyword in text.lower()

            if in_spike:
                if random.random() < 0.7:
                    lang = "fr"
                    text = (
                        f"Alerte en direct: une {self.settings.target_keyword} massive "
                        "est signalee pres du centre-ville."
                    )
                    contains_keyword = True
                    source_mode = "spike"
                elif random.random() < 0.4:
                    lang = self.settings.noise_spike_language
                    text = "Breaking update: protest chatter is increasing in another language."
                    contains_keyword = False
                    source_mode = "noise_spike"

            batch.append(
                {
                    "post_id": str(uuid.uuid4()),
                    "event_time": datetime.now(timezone.utc).isoformat(),
                    "lang": lang,
                    "text": text,
                    "contains_target_keyword": contains_keyword,
                    "source_mode": source_mode,
                }
            )

        return batch

    def generate(self):
        start = time.monotonic()
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= self.settings.run_seconds:
                break
            yield self.build_batch(elapsed)
            time.sleep(1)
