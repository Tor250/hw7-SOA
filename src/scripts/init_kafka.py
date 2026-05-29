from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from src.common.config import MOVIE_EVENTS_SUBJECT, MOVIE_EVENTS_TOPIC
from src.common.logging_utils import configure_logging
from src.common.schema_loader import load_movie_event_schema


configure_logging()
LOGGER = logging.getLogger(__name__)


def wait_for_kafka(bootstrap_servers: str, attempts: int = 60) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    for attempt in range(1, attempts + 1):
        try:
            admin.list_topics(timeout=5)
            LOGGER.info("Kafka is ready after %s attempts", attempt)
            return
        except Exception:
            LOGGER.info("Waiting for Kafka cluster (%s/%s)", attempt, attempts)
            time.sleep(2)
    raise TimeoutError("Kafka cluster is not ready")


def wait_for_schema_registry(url: str, attempts: int = 60) -> None:
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(f"{url}/subjects", timeout=5) as response:
                if response.status == 200:
                    LOGGER.info("Schema Registry is ready after %s attempts", attempt)
                    return
        except Exception:
            LOGGER.info("Waiting for Schema Registry (%s/%s)", attempt, attempts)
            time.sleep(2)
    raise TimeoutError("Schema Registry is not ready")


def create_topic(bootstrap_servers: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics(
        [
            NewTopic(
                topic=MOVIE_EVENTS_TOPIC,
                num_partitions=3,
                replication_factor=2,
                config={"min.insync.replicas": "1"},
            )
        ]
    )
    future = futures[MOVIE_EVENTS_TOPIC]
    try:
        future.result()
        LOGGER.info("Created topic %s", MOVIE_EVENTS_TOPIC)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" in str(exc):
            LOGGER.info("Topic %s already exists", MOVIE_EVENTS_TOPIC)
        else:
            raise


def register_schema(schema_registry_url: str) -> None:
    client = SchemaRegistryClient({"url": schema_registry_url})
    schema_id = client.register_schema(
        MOVIE_EVENTS_SUBJECT,
        Schema(load_movie_event_schema(), schema_type="AVRO"),
    )
    LOGGER.info("Registered schema subject=%s schema_id=%s", MOVIE_EVENTS_SUBJECT, schema_id)

    request = urllib.request.Request(
        f"{schema_registry_url}/config/{MOVIE_EVENTS_SUBJECT}",
        data=json.dumps({"compatibility": "BACKWARD"}).encode("utf-8"),
        method="PUT",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            LOGGER.info("Schema compatibility response=%s", response.status)
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise


def main() -> None:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-1:9092,kafka-2:9092,kafka-3:9092",
    )
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    wait_for_kafka(bootstrap_servers)
    wait_for_schema_registry(schema_registry_url)
    create_topic(bootstrap_servers)
    register_schema(schema_registry_url)


if __name__ == "__main__":
    main()
