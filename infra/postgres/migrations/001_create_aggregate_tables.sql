CREATE TABLE IF NOT EXISTS metric_values (
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, metric_name)
);

CREATE TABLE IF NOT EXISTS top_movies (
    metric_date DATE NOT NULL,
    rank INTEGER NOT NULL,
    movie_id TEXT NOT NULL,
    views_count BIGINT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, rank)
);

CREATE TABLE IF NOT EXISTS retention_cohort (
    cohort_date DATE NOT NULL,
    activity_date DATE NOT NULL,
    day_number INTEGER NOT NULL,
    retained_users BIGINT NOT NULL,
    cohort_size BIGINT NOT NULL,
    retention_rate DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (cohort_date, day_number)
);

CREATE TABLE IF NOT EXISTS device_distribution (
    metric_date DATE NOT NULL,
    device_type TEXT NOT NULL,
    active_users BIGINT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, device_type)
);

CREATE INDEX IF NOT EXISTS idx_metric_values_date ON metric_values (metric_date);
CREATE INDEX IF NOT EXISTS idx_top_movies_date ON top_movies (metric_date);
CREATE INDEX IF NOT EXISTS idx_retention_cohort_date ON retention_cohort (cohort_date);
CREATE INDEX IF NOT EXISTS idx_device_distribution_date ON device_distribution (metric_date);
