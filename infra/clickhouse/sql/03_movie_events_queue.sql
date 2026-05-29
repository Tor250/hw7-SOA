CREATE TABLE IF NOT EXISTS analytics.movie_events_queue
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 3,
    kafka_thread_per_consumer = 0,
    kafka_handle_error_mode = 'stream',
    kafka_commit_every_batch = 1,
    format_avro_schema_registry_url = 'http://schema-registry:8081'

