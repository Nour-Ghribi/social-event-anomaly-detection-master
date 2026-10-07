#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T postgres psql -U social -d social_events -c \
  "SELECT keyword, lang, window_start, window_end, event_count, threshold, is_anomaly FROM window_metrics ORDER BY window_start;"

docker compose exec -T postgres psql -U social -d social_events -c \
  "SELECT keyword, lang, window_start, window_end, event_count, threshold, detection_reason FROM anomalies ORDER BY detected_at;"
