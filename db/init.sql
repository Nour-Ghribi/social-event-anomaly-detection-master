CREATE TABLE IF NOT EXISTS window_metrics (
    id BIGSERIAL PRIMARY KEY,
    keyword TEXT NOT NULL,
    lang VARCHAR(8) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_count INTEGER NOT NULL,
    baseline_mean DOUBLE PRECISION,
    baseline_stddev DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    detection_reason TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_window_metrics_unique
    ON window_metrics (keyword, lang, window_start, window_end);

CREATE INDEX IF NOT EXISTS idx_window_metrics_lookup
    ON window_metrics (keyword, lang, window_start);

CREATE TABLE IF NOT EXISTS anomalies (
    id BIGSERIAL PRIMARY KEY,
    keyword TEXT NOT NULL,
    lang VARCHAR(8) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_count INTEGER NOT NULL,
    baseline_mean DOUBLE PRECISION,
    baseline_stddev DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    detection_reason TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_lookup
    ON anomalies (keyword, lang, window_start);
