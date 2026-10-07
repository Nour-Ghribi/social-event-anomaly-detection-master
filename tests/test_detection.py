from datetime import datetime, timedelta
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark_app"))
sys.path.insert(0, str(ROOT / "producer"))

from detection import DynamicBaselineDetector, WindowSummary  # noqa: E402
from generator import ProducerSettings, SocialPostFactory  # noqa: E402


class DynamicBaselineDetectorTests(unittest.TestCase):
    def test_spike_is_detected_after_enough_history(self) -> None:
        detector = DynamicBaselineDetector(baseline_windows=6)
        base_time = datetime(2026, 1, 1, 12, 0, 0)
        counts = [2, 3, 2, 4, 3, 18]
        results = []

        for index, count in enumerate(counts):
            summary = WindowSummary(
                keyword="manifestation",
                lang="fr",
                window_start=base_time + timedelta(seconds=index * 15),
                window_end=base_time + timedelta(seconds=(index + 1) * 15),
                event_count=count,
            )
            results.append(detector.evaluate(summary))

        self.assertFalse(any(result.is_anomaly for result in results[:5]))
        self.assertTrue(results[-1].is_anomaly)
        self.assertGreater(results[-1].threshold, 0)


class SocialPostFactoryTests(unittest.TestCase):
    def test_spike_batch_contains_french_keyword_hits(self) -> None:
        settings = ProducerSettings(
            target_keyword="manifestation",
            run_seconds=5,
            posts_per_second=4,
            spike_start_after_seconds=1,
            spike_duration_seconds=2,
            spike_multiplier=6,
            noise_spike_language="en",
        )
        factory = SocialPostFactory(ROOT / "data" / "config.json", settings)
        batch = factory.build_batch(elapsed_seconds=1.2)
        fr_hits = [
            post
            for post in batch
            if post["lang"] == "fr" and post["contains_target_keyword"]
        ]

        self.assertGreaterEqual(len(batch), 20)
        self.assertGreater(len(fr_hits), 0)


if __name__ == "__main__":
    unittest.main()
