from __future__ import annotations

from functools import lru_cache
from pathlib import Path


SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "movie_event.avsc"


@lru_cache(maxsize=1)
def load_movie_event_schema() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")

