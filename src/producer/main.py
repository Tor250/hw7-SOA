from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from src.common.logging_utils import configure_logging
from src.common.models import MovieEventIn
from src.producer.generator import SyntheticEventGenerator
from src.producer.publisher import KafkaMovieEventPublisher
from src.producer.settings import ProducerSettings


configure_logging()
settings = ProducerSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = KafkaMovieEventPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        schema_registry_url=settings.schema_registry_url,
    )
    generator = SyntheticEventGenerator(publisher, settings) if settings.enable_generator else None
    app.state.publisher = publisher
    app.state.generator = generator
    if generator is not None:
        await generator.start()
    try:
        yield
    finally:
        if generator is not None:
            await generator.stop()
        publisher.close()


app = FastAPI(title="movie-producer", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events")
async def publish_event(payload: MovieEventIn) -> dict[str, Any]:
    event = payload.to_record()
    try:
        event_id = await asyncio.to_thread(app.state.publisher.publish, event)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"event_id": event_id}
