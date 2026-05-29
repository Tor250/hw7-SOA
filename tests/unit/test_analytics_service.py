from __future__ import annotations

import asyncio
from datetime import date

from src.analytics.service import AnalyticsService


class FakeRepository:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def raw_event_count(self) -> int:
        self.calls.append("raw_event_count")
        return 7

    def rebuild_clickhouse_aggregates(self) -> list[date]:
        self.calls.append("rebuild_clickhouse_aggregates")
        return [date(2025, 1, 10)]

    def sync_postgres_from_clickhouse(self) -> None:
        self.calls.append("sync_postgres_from_clickhouse")

    def metric_snapshot_for_date(self, target_date: date) -> dict:
        self.calls.append(f"metric_snapshot_for_date:{target_date.isoformat()}")
        return {
            "metric_date": target_date.isoformat(),
            "metrics": [],
            "top_movies": [],
            "retention": [],
        }


class FakeExporter:
    def __init__(self) -> None:
        self.calls: list[date] = []
        self.default_date = date(2025, 1, 9)

    def default_export_date(self) -> date:
        return self.default_date

    def export_date(self, target_date: date) -> str:
        self.calls.append(target_date)
        return f"daily/{target_date.isoformat()}/aggregates.json"


def test_aggregation_cycle_annotates_empty_snapshot() -> None:
    repository = FakeRepository()
    exporter = FakeExporter()
    service = AnalyticsService(repository, exporter)

    result = asyncio.run(
        service.run_aggregation_cycle(
            reason="manual",
            requested_date=date(2025, 1, 10),
        )
    )

    assert result["processed_records"] == 7
    assert result["duration_seconds"] >= 0
    assert result["note"] == "No aggregates found for requested date"
    assert result["available_metric_dates"] == ["2025-01-10"]
    assert repository.calls == [
        "raw_event_count",
        "rebuild_clickhouse_aggregates",
        "sync_postgres_from_clickhouse",
        "metric_snapshot_for_date:2025-01-10",
    ]


def test_export_cycle_uses_default_date_when_not_provided() -> None:
    repository = FakeRepository()
    exporter = FakeExporter()
    service = AnalyticsService(repository, exporter)

    result = asyncio.run(service.run_export_cycle(reason="scheduled"))

    assert result["metric_date"] == "2025-01-09"
    assert result["s3_key"] == "daily/2025-01-09/aggregates.json"
    assert exporter.calls == [date(2025, 1, 9)]
