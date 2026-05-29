CREATE TABLE IF NOT EXISTS analytics.daily_metrics
(
    metric_date Date,
    metric_name LowCardinality(String),
    metric_value Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name)

