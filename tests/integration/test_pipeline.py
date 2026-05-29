from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import boto3
import clickhouse_connect
import httpx
import psycopg
from botocore.config import Config
from psycopg.rows import dict_row


PRODUCER_URL = os.getenv("PRODUCER_URL", "http://producer:8000")
ANALYTICS_URL = os.getenv("ANALYTICS_URL", "http://analytics-service:8001")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://analytics:analytics@postgres:5432/analytics")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio123")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")
S3_REGION = os.getenv("S3_REGION", "ru-central-1")


def post_event(payload: dict) -> str:
    response = httpx.post(f"{PRODUCER_URL}/events", json=payload, timeout=30.0)
    response.raise_for_status()
    return response.json()["event_id"]


def test_full_pipeline_roundtrip() -> None:
    unique_day_offset = int(uuid4().hex[:8], 16) % 3650
    target_date = datetime(2035, 1, 10, 12, 0, tzinfo=timezone.utc) + timedelta(days=unique_day_offset)
    next_date = target_date + timedelta(days=1)
    movie_id = f"movie-it-{uuid4().hex[:8]}"
    second_movie_id = f"movie-it-{uuid4().hex[:8]}"
    user_one = f"user-it-{uuid4().hex[:8]}"
    user_two = f"user-it-{uuid4().hex[:8]}"

    session_one = f"session-{uuid4()}"
    session_two = f"session-{uuid4()}"

    first_event_id = post_event(
        {
            "user_id": user_one,
            "movie_id": movie_id,
            "event_type": "VIEW_STARTED",
            "timestamp": target_date.isoformat(),
            "device_type": "MOBILE",
            "session_id": session_one,
            "progress_seconds": 0,
        }
    )
    post_event(
        {
            "user_id": user_one,
            "movie_id": movie_id,
            "event_type": "VIEW_FINISHED",
            "timestamp": (target_date + timedelta(minutes=45)).isoformat(),
            "device_type": "MOBILE",
            "session_id": session_one,
            "progress_seconds": 2700,
        }
    )
    post_event(
        {
            "user_id": user_two,
            "movie_id": second_movie_id,
            "event_type": "VIEW_STARTED",
            "timestamp": target_date.isoformat(),
            "device_type": "TV",
            "session_id": session_two,
            "progress_seconds": 0,
        }
    )
    post_event(
        {
            "user_id": user_one,
            "movie_id": movie_id,
            "event_type": "VIEW_STARTED",
            "timestamp": next_date.isoformat(),
            "device_type": "DESKTOP",
            "session_id": f"session-{uuid4()}",
            "progress_seconds": 0,
        }
    )

    clickhouse = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="analytics",
    )
    deadline = time.time() + 90
    found = False
    while time.time() < deadline:
        result = clickhouse.query(
            f"""
            SELECT event_id, user_id, movie_id, event_type, progress_seconds
            FROM analytics.movie_events
            WHERE event_id = toUUID('{first_event_id}')
            """,
        )
        if result.result_rows:
            found = True
            row = result.result_rows[0]
            assert str(row[0]) == first_event_id
            assert row[1] == user_one
            assert row[2] == movie_id
            assert row[3] == "VIEW_STARTED"
            assert row[4] == 0
            break
        time.sleep(2)
    assert found, "event did not appear in ClickHouse"

    aggregation_response = httpx.post(
        f"{ANALYTICS_URL}/aggregation/run",
        params={"date": target_date.date().isoformat()},
        timeout=60.0,
    )
    aggregation_response.raise_for_status()
    aggregation_payload = aggregation_response.json()
    assert aggregation_payload["metric_date"] == target_date.date().isoformat()

    with psycopg.connect(POSTGRES_DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT metric_name, metric_value
                FROM metric_values
                WHERE metric_date = %s
                ORDER BY metric_name
                """,
                (target_date.date(),),
            )
            rows = cur.fetchall()
            metrics = {row["metric_name"]: row["metric_value"] for row in rows}

    assert metrics["dau"] == 2
    assert metrics["avg_watch_time_seconds"] == 2700
    assert round(metrics["view_finish_conversion"], 4) == 0.5
    assert round(metrics["retention_d1"], 4) == 0.5

    export_response = httpx.post(
        f"{ANALYTICS_URL}/export/run",
        params={"date": target_date.date().isoformat()},
        timeout=60.0,
    )
    export_response.raise_for_status()
    export_payload = export_response.json()
    assert export_payload["metric_date"] == target_date.date().isoformat()

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(signature_version="s3v4"),
    )
    obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key=f"daily/{target_date.date().isoformat()}/aggregates.json",
    )
    exported = json.loads(obj["Body"].read().decode("utf-8"))
    assert exported["metric_date"] == target_date.date().isoformat()
    exported_metrics = {item["metric_name"]: item["metric_value"] for item in exported["metrics"]}
    assert exported_metrics["dau"] == 2
    assert any(movie["movie_id"] == movie_id for movie in exported["top_movies"])
