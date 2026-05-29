from __future__ import annotations

import time
from datetime import datetime, timezone
from uuid import uuid4

import clickhouse_connect
import httpx


PRODUCER_URL = "http://producer:8000"
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics"


def post_event(payload: dict) -> str:
    response = httpx.post(f"{PRODUCER_URL}/events", json=payload, timeout=30.0)
    response.raise_for_status()
    body = response.json()
    assert isinstance(body["event_id"], str)
    return body["event_id"]


def delete_event(clickhouse: clickhouse_connect.driver.Client, event_id: str) -> None:
    clickhouse.command(
        f"ALTER TABLE analytics.movie_events DELETE WHERE event_id = toUUID('{event_id}')"
    )
    deadline = time.time() + 90
    while time.time() < deadline:
        result = clickhouse.query(
            f"""
            SELECT count()
            FROM analytics.movie_events
            WHERE event_id = toUUID('{event_id}')
            """
        )
        if result.result_rows and result.result_rows[0][0] == 0:
            return
        time.sleep(2)
    raise AssertionError(f"event {event_id} was not removed from ClickHouse")


def test_event_is_persisted_in_clickhouse() -> None:
    unique_date = datetime(2035, 2, 1, 12, 0, tzinfo=timezone.utc)
    event_id = uuid4()
    movie_id = f"movie-ingestion-{uuid4().hex[:8]}"
    user_id = f"user-ingestion-{uuid4().hex[:8]}"

    clickhouse = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="analytics",
    )
    posted_event_id = post_event(
        {
            "event_id": str(event_id),
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": "VIEW_STARTED",
            "timestamp": unique_date.isoformat(),
            "device_type": "DESKTOP",
            "session_id": f"session-{uuid4().hex[:8]}",
            "progress_seconds": 0,
        }
    )

    try:
        assert posted_event_id == str(event_id)

        deadline = time.time() + 90
        while time.time() < deadline:
            result = clickhouse.query(
                f"""
                SELECT event_id, user_id, movie_id, event_type, progress_seconds
                FROM analytics.movie_events
                WHERE event_id = toUUID('{event_id}')
                """
            )
            if result.result_rows:
                row = result.result_rows[0]
                assert str(row[0]) == str(event_id)
                assert row[1] == user_id
                assert row[2] == movie_id
                assert row[3] == "VIEW_STARTED"
                assert row[4] == 0
                return
            time.sleep(2)

        raise AssertionError("event did not appear in ClickHouse")
    finally:
        delete_event(clickhouse, str(event_id))
