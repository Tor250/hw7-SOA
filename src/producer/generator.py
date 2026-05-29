from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from src.common.models import DeviceType, EventType, MovieEventRecord
from src.producer.publisher import KafkaMovieEventPublisher
from src.producer.settings import ProducerSettings


LOGGER = logging.getLogger(__name__)

MOVIES = [f"movie-{index:03d}" for index in range(1, 31)]
USERS = [f"user-{index:04d}" for index in range(1, 401)]


@dataclass(slots=True)
class SessionPlan:
    user_id: str
    movie_id: str
    device_type: DeviceType
    session_id: str
    started_at: datetime
    paused_at_seconds: int
    resumed_at_seconds: int
    finished_at_seconds: int
    include_pause: bool
    include_like: bool
    include_search: bool


class SyntheticEventGenerator:
    def __init__(self, publisher: KafkaMovieEventPublisher, settings: ProducerSettings) -> None:
        self.publisher = publisher
        self.settings = settings
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._seed_completed = False

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="synthetic-event-generator")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        try:
            await self._seed_history()
            while not self._stop.is_set():
                plans = [self._build_plan() for _ in range(self.settings.generator_batch_size)]
                for plan in plans:
                    for event in self._events_for_plan(plan):
                        await asyncio.to_thread(self.publisher.publish, event)
                await self._sleep_or_stop(self.settings.generator_live_interval_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Synthetic generator failed")

    async def _seed_history(self) -> None:
        if self._seed_completed:
            return
        now = datetime.now(tz=timezone.utc)
        for day_offset in range(self.settings.generator_seed_days, 0, -1):
            day_base = (now - timedelta(days=day_offset)).replace(
                hour=8,
                minute=0,
                second=0,
                microsecond=0,
            )
            plans = [
                self._build_plan(day_base=day_base + timedelta(minutes=random.randint(0, 900)))
                for _ in range(self.settings.generator_sessions_per_day)
            ]
            for plan in plans:
                for event in self._events_for_plan(plan):
                    await asyncio.to_thread(self.publisher.publish, event)
        self._seed_completed = True
        LOGGER.info("Seeded synthetic history for %s days", self.settings.generator_seed_days)

    def _build_plan(self, day_base: datetime | None = None) -> SessionPlan:
        started_at = day_base or (datetime.now(tz=timezone.utc) - timedelta(minutes=random.randint(0, 30)))
        paused_at_seconds = random.randint(120, 900)
        resumed_at_seconds = paused_at_seconds + random.randint(15, 90)
        finished_at_seconds = resumed_at_seconds + random.randint(600, 3600)
        return SessionPlan(
            user_id=random.choice(USERS),
            movie_id=random.choice(MOVIES),
            device_type=random.choice(list(DeviceType)),
            session_id=str(uuid4()),
            started_at=started_at,
            paused_at_seconds=paused_at_seconds,
            resumed_at_seconds=resumed_at_seconds,
            finished_at_seconds=finished_at_seconds,
            include_pause=random.random() < 0.6,
            include_like=random.random() < 0.35,
            include_search=random.random() < 0.4,
        )

    def _events_for_plan(self, plan: SessionPlan) -> list[MovieEventRecord]:
        events: list[MovieEventRecord] = []
        if plan.include_search:
            events.append(
                self._build_event(
                    plan,
                    EventType.SEARCHED,
                    timestamp=plan.started_at - timedelta(seconds=random.randint(20, 120)),
                    progress_seconds=0,
                )
            )

        events.append(self._build_event(plan, EventType.VIEW_STARTED, plan.started_at, 0))

        if plan.include_pause:
            pause_timestamp = plan.started_at + timedelta(seconds=plan.paused_at_seconds)
            events.append(
                self._build_event(
                    plan,
                    EventType.VIEW_PAUSED,
                    timestamp=pause_timestamp,
                    progress_seconds=plan.paused_at_seconds,
                )
            )
            events.append(
                self._build_event(
                    plan,
                    EventType.VIEW_RESUMED,
                    timestamp=plan.started_at + timedelta(seconds=plan.resumed_at_seconds),
                    progress_seconds=plan.resumed_at_seconds,
                )
            )

        events.append(
            self._build_event(
                plan,
                EventType.VIEW_FINISHED,
                timestamp=plan.started_at + timedelta(seconds=plan.finished_at_seconds),
                progress_seconds=plan.finished_at_seconds,
            )
        )

        if plan.include_like:
            events.append(
                self._build_event(
                    plan,
                    EventType.LIKED,
                    timestamp=plan.started_at + timedelta(seconds=plan.finished_at_seconds + 10),
                    progress_seconds=0,
                )
            )

        return sorted(events, key=lambda event: event.timestamp)

    @staticmethod
    def _build_event(
        plan: SessionPlan,
        event_type: EventType,
        timestamp: datetime,
        progress_seconds: int,
    ) -> MovieEventRecord:
        return MovieEventRecord(
            event_id=uuid4(),
            user_id=plan.user_id,
            movie_id=plan.movie_id,
            event_type=event_type,
            timestamp=timestamp,
            device_type=plan.device_type,
            session_id=plan.session_id,
            progress_seconds=progress_seconds,
        )

    async def _sleep_or_stop(self, seconds: int) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return
