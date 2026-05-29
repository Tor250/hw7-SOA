from __future__ import annotations

import os
from dataclasses import dataclass

from src.common.config import env_int


@dataclass(slots=True)
class AnalyticsSettings:
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port: int = env_int("CLICKHOUSE_PORT", 8123)
    clickhouse_username: str = os.getenv("CLICKHOUSE_USER", "analytics")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "analytics")
    clickhouse_database: str = os.getenv("CLICKHOUSE_DATABASE", "analytics")
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN",
        "postgresql://analytics:analytics@postgres:5432/analytics",
    )
    aggregation_interval_seconds: int = env_int("AGGREGATION_INTERVAL_SECONDS", 60)
    export_interval_seconds: int = env_int("EXPORT_INTERVAL_SECONDS", 120)
    s3_endpoint_url: str = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "minio")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "minio123")
    s3_bucket: str = os.getenv("S3_BUCKET", "movie-analytics")
    s3_region: str = os.getenv("S3_REGION", "ru-central-1")
