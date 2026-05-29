from __future__ import annotations

import asyncio
import logging
import time
from datetime import date
from typing import Any

from src.analytics.exporter import S3Exporter
from src.analytics.repository import AnalyticsRepository


LOGGER = logging.getLogger(__name__)


class AnalyticsService:
    def __init__(self, repository: AnalyticsRepository, exporter: S3Exporter) -> None:
        self.repository = repository
        self.exporter = exporter
        self._aggregation_lock = asyncio.Lock()
        self._export_lock = asyncio.Lock()

    async def run_aggregation_cycle(self, reason: str, requested_date: date | None = None) -> dict[str, Any]:
        async with self._aggregation_lock:
            started_at = time.perf_counter()
            raw_count = await asyncio.to_thread(self.repository.raw_event_count)
            LOGGER.info("Aggregation cycle started reason=%s requested_date=%s", reason, requested_date)
            affected_dates = await asyncio.to_thread(self.repository.rebuild_clickhouse_aggregates)
            await asyncio.to_thread(self.repository.sync_postgres_from_clickhouse)
            duration_seconds = round(time.perf_counter() - started_at, 3)
            LOGGER.info(
                "Aggregation cycle finished reason=%s processed_records=%s duration_seconds=%s",
                reason,
                raw_count,
                duration_seconds,
            )
            if requested_date is not None:
                snapshot = await asyncio.to_thread(self.repository.metric_snapshot_for_date, requested_date)
                if not snapshot.get("metrics"):
                    snapshot["note"] = "No aggregates found for requested date"
                    snapshot["available_metric_dates"] = [value.isoformat() for value in affected_dates]
            else:
                snapshot = {
                    "affected_dates": [value.isoformat() for value in affected_dates],
                }
            snapshot["processed_records"] = raw_count
            snapshot["duration_seconds"] = duration_seconds
            return snapshot

    async def run_export_cycle(self, reason: str, target_date: date | None = None) -> dict[str, Any]:
        async with self._export_lock:
            export_date = target_date or self.exporter.default_export_date()
            LOGGER.info("Export cycle started reason=%s target_date=%s", reason, export_date)
            key = await asyncio.to_thread(self.exporter.export_date, export_date)
            return {"metric_date": export_date.isoformat(), "s3_key": key}
