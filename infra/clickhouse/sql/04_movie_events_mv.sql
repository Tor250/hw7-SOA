CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.movie_events_mv
TO analytics.movie_events
AS
SELECT
    toUUID(event_id) AS event_id,
    user_id,
    movie_id,
    CAST(
        event_type,
        'Enum8(\'VIEW_STARTED\' = 1, \'VIEW_FINISHED\' = 2, \'VIEW_PAUSED\' = 3, \'VIEW_RESUMED\' = 4, \'LIKED\' = 5, \'SEARCHED\' = 6)'
    ) AS event_type,
    timestamp AS event_timestamp,
    CAST(
        device_type,
        'Enum8(\'MOBILE\' = 1, \'DESKTOP\' = 2, \'TV\' = 3, \'TABLET\' = 4)'
    ) AS device_type,
    session_id,
    toUInt32(greatest(progress_seconds, 0)) AS progress_seconds
FROM analytics.movie_events_queue
