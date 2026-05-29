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


def create_topic(
    bootstrap_servers: str,
    topic_name: str,
    num_partitions: int,
    replication_factor: int,
    config: dict[str, str] | None = None,
) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=config or {"min.insync.replicas": "1"},
            )
        ]
    )
    future = futures[topic_name]
    try:
        future.result()
        LOGGER.info("Created topic %s", topic_name)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" in str(exc):
            LOGGER.info("Topic %s already exists", topic_name)
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
    create_schema_registry_topic = os.getenv("CREATE_SCHEMA_REGISTRY_TOPIC", "false").lower() == "true"
    create_movie_events_topic = os.getenv("CREATE_MOVIE_EVENTS_TOPIC", "true").lower() == "true"
    should_register_schema = os.getenv("REGISTER_SCHEMA", "true").lower() == "true"

    wait_for_kafka(bootstrap_servers)
    if create_schema_registry_topic:
        create_topic(
            bootstrap_servers,
            "_schemas",
            1,
            3,
            config={
                "cleanup.policy": "compact",
                "min.insync.replicas": "1",
            },
        )

    if create_movie_events_topic:
        create_topic(bootstrap_servers, MOVIE_EVENTS_TOPIC, 3, 2)

    if should_register_schema:
        wait_for_schema_registry(schema_registry_url)
        register_schema(schema_registry_url)


if __name__ == "__main__":
    main()
