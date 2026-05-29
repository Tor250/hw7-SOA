CREATE TABLE IF NOT EXISTS analytics.retention_cohort_daily
(
    cohort_date Date,
    activity_date Date,
    day_number UInt8,
    retained_users UInt64,
    cohort_size UInt64,
    retention_rate Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_number)

