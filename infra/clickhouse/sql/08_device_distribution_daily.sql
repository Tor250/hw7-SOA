CREATE TABLE IF NOT EXISTS analytics.device_distribution_daily
(
    metric_date Date,
    device_type LowCardinality(String),
    active_users UInt64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, device_type)

