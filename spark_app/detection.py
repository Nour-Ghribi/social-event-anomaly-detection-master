from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class WindowSummary:
    keyword: str
    lang: str
    window_start: datetime
    window_end: datetime
    event_count: int


@dataclass(slots=True)
class DetectionResult:
    baseline_mean: float | None
    baseline_stddev: float | None
    threshold: float
    is_anomaly: bool
    detection_reason: str


class DynamicBaselineDetector:
    def __init__(self, baseline_windows: int = 6) -> None:
        self.baseline_windows = baseline_windows
        self.history: dict[tuple[str, str], deque[int]] = defaultdict(
            lambda: deque(maxlen=self.baseline_windows)
        )

    def evaluate(self, summary: WindowSummary) -> DetectionResult:
        key = (summary.keyword, summary.lang)
        previous_counts = list(self.history[key])

        if previous_counts:
            mean = sum(previous_counts) / len(previous_counts)
            variance = sum((value - mean) ** 2 for value in previous_counts) / len(previous_counts)
            stddev = math.sqrt(variance)
        else:
            mean = None
            stddev = None

        if len(previous_counts) < 4:
            threshold = 10.0
            is_anomaly = False
            detection_reason = "Not enough history for anomaly detection yet."
        else:
            assert mean is not None
            assert stddev is not None
            threshold = max(mean + (3 * stddev), mean * 2, 10)
            is_anomaly = summary.event_count >= threshold
            detection_reason = (
                "Spike detected above dynamic threshold."
                if is_anomaly
                else "Window within expected dynamic baseline."
            )

        self.history[key].append(summary.event_count)
        return DetectionResult(
            baseline_mean=mean,
            baseline_stddev=stddev,
            threshold=threshold,
            is_anomaly=is_anomaly,
            detection_reason=detection_reason,
        )
