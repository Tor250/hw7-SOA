from __future__ import annotations

import os
from dataclasses import dataclass

from src.common.config import env_bool, env_int


@dataclass(slots=True)
class ProducerSettings:
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-1:9092,kafka-2:9092,kafka-3:9092",
    )
    schema_registry_url: str = os.getenv(
        "SCHEMA_REGISTRY_URL",
        "http://schema-registry:8081",
    )
    enable_generator: bool = env_bool("PRODUCER_ENABLE_GENERATOR", True)
    generator_seed_days: int = env_int("GENERATOR_SEED_DAYS", 10)
    generator_sessions_per_day: int = env_int("GENERATOR_SESSIONS_PER_DAY", 80)
    generator_live_interval_seconds: int = env_int("GENERATOR_LIVE_INTERVAL_SECONDS", 5)
    generator_batch_size: int = env_int("GENERATOR_BATCH_SIZE", 3)
