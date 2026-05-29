from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.common.models import DeviceType, EventType, MovieEventIn


def test_movie_event_timestamp_is_normalized_to_utc() -> None:
    payload = MovieEventIn(
        user_id="user-1",
        movie_id="movie-1",
        event_type=EventType.VIEW_STARTED,
        timestamp=datetime(2025, 1, 1, 12, 0),
        device_type=DeviceType.MOBILE,
        session_id="session-1",
    )

    assert payload.timestamp is not None
    assert payload.timestamp.tzinfo == timezone.utc


def test_movie_event_validation_rejects_invalid_payload() -> None:
    with pytest.raises(ValidationError):
        MovieEventIn(
            user_id=" ",
            movie_id="movie-1",
            event_type=EventType.LIKED,
            device_type=DeviceType.DESKTOP,
            session_id="session-1",
            progress_seconds=10,
        )


def test_movie_event_to_record_generates_required_fields() -> None:
    payload = MovieEventIn(
        user_id="user-2",
        movie_id="movie-2",
        event_type=EventType.SEARCHED,
        device_type=DeviceType.TABLET,
        session_id="session-2",
    )

    record = payload.to_record()

    assert record.event_id is not None
    assert record.user_id == "user-2"
    assert record.movie_id == "movie-2"
    assert record.event_type == EventType.SEARCHED
    assert record.device_type == DeviceType.TABLET
    assert record.progress_seconds == 0
