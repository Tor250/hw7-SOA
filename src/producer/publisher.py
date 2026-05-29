from __future__ import annotations

import logging
from threading import Event as ThreadEvent

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.common.config import MOVIE_EVENTS_SUBJECT, MOVIE_EVENTS_TOPIC
from src.common.models import MovieEventRecord
from src.common.schema_loader import load_movie_event_schema


LOGGER = logging.getLogger(__name__)


class KafkaMovieEventPublisher:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str) -> None:
        self.topic = MOVIE_EVENTS_TOPIC
        self.schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
        self.schema = Schema(load_movie_event_schema(), schema_type="AVRO")
        self.schema_registry_client.register_schema(MOVIE_EVENTS_SUBJECT, self.schema)
        self.value_serializer = AvroSerializer(
            self.schema_registry_client,
            load_movie_event_schema(),
            to_dict=lambda value, _: value,
        )
        self.key_serializer = StringSerializer("utf_8")
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
                "socket.timeout.ms": 10000,
                "message.timeout.ms": 30000,
                "request.timeout.ms": 10000,
            }
        )

    @retry(
        retry=retry_if_exception_type((KafkaException, BufferError, TimeoutError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def publish(self, event: MovieEventRecord) -> str:
        delivery_event = ThreadEvent()
        delivery_state: dict[str, object] = {}
        key = self.key_serializer(
            event.user_id,
            SerializationContext(self.topic, MessageField.KEY),
        )
        value = self.value_serializer(
            event.to_avro_dict(),
            SerializationContext(self.topic, MessageField.VALUE),
        )

        def on_delivery(err, msg) -> None:
            delivery_state["error"] = err
            delivery_state["message"] = msg
            delivery_event.set()

        self.producer.produce(self.topic, key=key, value=value, on_delivery=on_delivery)
        self.producer.poll(0)

        if not delivery_event.wait(timeout=10):
            self.producer.flush(timeout=10)
            if not delivery_event.wait(timeout=5):
                raise TimeoutError("Timed out waiting for Kafka delivery report")

        error = delivery_state.get("error")
        if error is not None:
            raise KafkaException(error)

        LOGGER.info(
            "Published event_id=%s event_type=%s timestamp=%s",
            event.event_id,
            event.event_type.value,
            event.timestamp.isoformat(),
        )
        return str(event.event_id)

    def close(self) -> None:
        self.producer.flush(timeout=10)
