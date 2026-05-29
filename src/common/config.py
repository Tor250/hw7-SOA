from __future__ import annotations

import os


MOVIE_EVENTS_TOPIC = os.getenv("MOVIE_EVENTS_TOPIC", "movie-events")
MOVIE_EVENTS_SUBJECT = os.getenv("MOVIE_EVENTS_SUBJECT", "movie-events-value")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)

