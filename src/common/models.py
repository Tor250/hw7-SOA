from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


VIEWING_EVENTS = {
    EventType.VIEW_STARTED,
    EventType.VIEW_FINISHED,
    EventType.VIEW_PAUSED,
    EventType.VIEW_RESUMED,
}


class MovieEventRecord(BaseModel):
    event_id: UUID
    user_id: str
    movie_id: str
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: str
    progress_seconds: int = Field(ge=0)

    def to_avro_dict(self) -> dict[str, object]:
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": int(self.timestamp.timestamp() * 1000),
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }


class MovieEventIn(BaseModel):
    event_id: UUID | None = None
    user_id: str
    movie_id: str
    event_type: EventType
    timestamp: datetime | None = None
    device_type: DeviceType
    session_id: str
    progress_seconds: int = Field(default=0, ge=0)

    @field_validator("user_id", "movie_id", "session_id")
    @classmethod
    def non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must not be empty")
        return value

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_payload(self) -> "MovieEventIn":
        if self.event_type not in VIEWING_EVENTS and self.progress_seconds != 0:
            raise ValueError("progress_seconds must be 0 for non-viewing events")
        return self

    def to_record(self) -> MovieEventRecord:
        return MovieEventRecord(
            event_id=self.event_id or uuid4(),
            user_id=self.user_id,
            movie_id=self.movie_id,
            event_type=self.event_type,
            timestamp=self.timestamp or datetime.now(tz=timezone.utc),
            device_type=self.device_type,
            session_id=self.session_id,
            progress_seconds=self.progress_seconds,
        )
