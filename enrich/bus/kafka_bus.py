"""Kafka-backed bus using the pure-Python ``kafka-python`` client.

Thin adapters that map ``kafka-python`` onto the :mod:`enrich.bus.base`
interface, plus small operational helpers (wait for the broker, create topics).

Notes on the delivery guarantee:
* The producer uses ``acks="all"`` + ``retries`` so a write is durable once the
  broker acknowledges it.
* The consumer disables auto-commit; the worker commits **after** results are
  produced, giving **at-least-once** processing. ``kafka-python`` has no
  idempotent-producer support, so exactly-once is out of scope by design —
  dedupe-by-key in the worker makes reprocessing safe instead.
"""

from __future__ import annotations

import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

from ..config import Settings
from .base import Consumer, Producer, Record


def broker_reachable(bootstrap_servers: str, timeout_ms: int = 3000) -> bool:
    """True if a broker answers a metadata request — used to auto-skip tests."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            api_version_auto_timeout_ms=timeout_ms,
            consumer_timeout_ms=timeout_ms,
        )
        consumer.topics()
        consumer.close()
        return True
    except Exception:
        return False


def wait_for_broker(bootstrap_servers: str, timeout: float = 60.0, interval: float = 1.0) -> None:
    """Block until the broker is reachable or raise ``TimeoutError``."""
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers, api_version_auto_timeout_ms=3000
            )
            consumer.topics()
            consumer.close()
            return
        except (NoBrokersAvailable, Exception) as exc:  # noqa: BLE001 - report last error
            last_err = exc
            time.sleep(interval)
    raise TimeoutError(f"Kafka not reachable at {bootstrap_servers} after {timeout:.0f}s: {last_err}")


def ensure_topics(settings: Settings) -> None:
    """Create the pipeline's topics if they don't exist (idempotent)."""
    admin = KafkaAdminClient(bootstrap_servers=settings.bootstrap_servers)
    wanted = [
        NewTopic(settings.topic_in, settings.partitions_in, 1),
        NewTopic(settings.topic_out, settings.partitions_out, 1),
        NewTopic(settings.topic_dlq, settings.partitions_dlq, 1),
    ]
    try:
        for topic in wanted:
            try:
                admin.create_topics([topic])
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def connect_producer(settings: Settings) -> "KafkaProducerAdapter":
    producer = KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        acks="all",
        retries=5,
        linger_ms=20,  # allow the client to batch records before sending
        max_in_flight_requests_per_connection=1,  # preserve per-key ordering under retry
    )
    return KafkaProducerAdapter(producer)


def connect_consumer(settings: Settings, *topics: str) -> "KafkaConsumerAdapter":
    subscribe = topics or (settings.topic_in,)
    consumer = KafkaConsumer(
        *subscribe,
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.group_id,
        enable_auto_commit=False,  # manual commit → at-least-once
        auto_offset_reset="earliest",
        max_poll_records=settings.batch_size,
    )
    return KafkaConsumerAdapter(consumer)


class KafkaProducerAdapter(Producer):
    def __init__(self, producer: KafkaProducer) -> None:
        self._producer = producer

    def send(self, topic, value, key=None, headers=None) -> None:
        self._producer.send(topic, value=value, key=key, headers=headers or [])

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()


class KafkaConsumerAdapter(Consumer):
    def __init__(self, consumer: KafkaConsumer) -> None:
        self._consumer = consumer

    def poll(self, timeout_ms: int = 1000, max_records: int = 100) -> list[Record]:
        batches = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        records: list[Record] = []
        for _tp, messages in batches.items():
            for m in messages:
                records.append(
                    Record(
                        value=m.value,
                        key=m.key,
                        topic=m.topic,
                        partition=m.partition,
                        offset=m.offset,
                        headers=list(m.headers or []),
                    )
                )
        return records

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()
