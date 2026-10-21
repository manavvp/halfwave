"""Shared Kafka plumbing: an idempotent producer and Schema Registry-backed Avro serialization."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from producers.config import require_env

log = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "schemas"


def load_schema(topic: str) -> str:
    """Schemas live in schemas/<topic>.avsc, one per topic."""
    return (SCHEMAS_DIR / f"{topic}.avsc").read_text()


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": require_env("KAFKA_API_KEY"),
            "sasl.password": require_env("KAFKA_API_SECRET"),
            # The broker dedups the producer's own network retries (sequence numbers per
            # partition). Implies acks=all. Source-level duplicates are silver's job (D003).
            "enable.idempotence": True,
            "client.id": "halfwave-producer",
        }
    )


def make_avro_serializer(topic: str) -> AvroSerializer:
    registry = SchemaRegistryClient(
        {
            "url": require_env("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": (
                f"{require_env('SCHEMA_REGISTRY_API_KEY')}:"
                f"{require_env('SCHEMA_REGISTRY_API_SECRET')}"
            ),
        }
    )
    # Registers under subject '<topic>-value' (TopicNameStrategy). The registry rejects a new
    # version that breaks the subject's compatibility mode (Confluent default: BACKWARD).
    return AvroSerializer(registry, load_schema(topic))


def produce_records(
    producer: Producer,
    topic: str,
    records: Iterable[dict[str, Any]],
    *,
    key_field: str,
    value_serializer: AvroSerializer,
    flush_timeout_s: float = 30.0,
) -> list[str]:
    """Produce records keyed by `record[key_field]`. Returns delivery errors (empty = success)."""
    key_serializer = StringSerializer("utf_8")
    errors: list[str] = []

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            errors.append(f"{msg.key()!r}: {err}")
        else:
            log.info(
                "delivered key=%s partition=%s offset=%s",
                msg.key().decode(),
                msg.partition(),
                msg.offset(),
            )

    for record in records:
        producer.produce(
            topic,
            key=key_serializer(record[key_field], SerializationContext(topic, MessageField.KEY)),
            value=value_serializer(record, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=on_delivery,
        )
        producer.poll(0)  # serve delivery callbacks as we go

    undelivered = producer.flush(flush_timeout_s)
    if undelivered:
        errors.append(f"{undelivered} message(s) still undelivered after {flush_timeout_s}s")
    return errors
