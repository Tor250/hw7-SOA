CREATE TABLE IF NOT EXISTS analytics.movie_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8(
        'VIEW_STARTED' = 1,
        'VIEW_FINISHED' = 2,
        'VIEW_PAUSED' = 3,
        'VIEW_RESUMED' = 4,
        'LIKED' = 5,
        'SEARCHED' = 6
    ),
    event_timestamp DateTime64(3, 'UTC'),
    event_date Date DEFAULT toDate(event_timestamp),
    device_type Enum8(
        'MOBILE' = 1,
        'DESKTOP' = 2,
        'TV' = 3,
        'TABLET' = 4
    ),
    session_id String,
    progress_seconds UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_timestamp, session_id, event_id)
