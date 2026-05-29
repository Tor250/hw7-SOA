from __future__ import annotations

import logging
import os


def configure_logging(level: str | None = None) -> None:
    resolved_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=resolved_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

