from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import date

from fastapi import FastAPI, Query

from src.analytics.exporter import S3Exporter
from src.analytics.repository import AnalyticsRepository
from src.analytics.service import AnalyticsService
from src.analytics.settings import AnalyticsSettings
from src.common.logging_utils import configure_logging


configure_logging()
settings = AnalyticsSettings()


async def periodic_runner(interval_seconds: int, callback, label: str, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await callback()
        except Exception:
            import logging

            logging.getLogger(__name__).exception("Scheduled task failed: %s", label)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    repository = AnalyticsRepository(settings)
    repository.apply_postgres_migrations()
    exporter = S3Exporter(settings, repository)
    service = AnalyticsService(repository, exporter)
    stop_event = asyncio.Event()

    aggregation_task = asyncio.create_task(
        periodic_runner(
            settings.aggregation_interval_seconds,
            lambda: service.run_aggregation_cycle(reason="scheduled"),
            "aggregation",
            stop_event,
        ),
        name="aggregation-scheduler",
    )
    export_task = asyncio.create_task(
        periodic_runner(
            settings.export_interval_seconds,
            lambda: service.run_export_cycle(reason="scheduled"),
            "export",
            stop_event,
        ),
        name="export-scheduler",
    )

    app.state.repository = repository
    app.state.service = service
    app.state.stop_event = stop_event
    app.state.background_tasks = [aggregation_task, export_task]
    try:
        yield
    finally:
        stop_event.set()
        for task in app.state.background_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        repository.close()


app = FastAPI(title="analytics-service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/aggregation/run")
async def run_aggregation(target_date: date = Query(..., alias="date")) -> dict:
    return await app.state.service.run_aggregation_cycle(reason="manual", requested_date=target_date)


@app.post("/export/run")
async def run_export(target_date: date = Query(..., alias="date")) -> dict:
    return await app.state.service.run_export_cycle(reason="manual", target_date=target_date)
