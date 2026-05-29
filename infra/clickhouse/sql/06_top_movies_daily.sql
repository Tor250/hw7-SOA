CREATE TABLE IF NOT EXISTS analytics.top_movies_daily
(
    metric_date Date,
    rank UInt8,
    movie_id String,
    views_count UInt64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, rank)

