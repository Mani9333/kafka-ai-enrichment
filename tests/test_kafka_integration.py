"""Integration test for the real Kafka path.

Auto-skips unless a broker is reachable, so the default `pytest` run stays
hermetic. To run it:

    docker compose up -d
    pytest tests/test_kafka_integration.py
"""

import json
import os
import uuid

import pytest

from enrich.bus.kafka_bus import (
    broker_reachable,
    connect_consumer,
    connect_producer,
    ensure_topics,
)
from enrich.config import Settings
from enrich.llm import get_chat_model
from enrich.worker import EnrichmentWorker

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

pytestmark = pytest.mark.skipif(
    not broker_reachable(BOOTSTRAP),
    reason="Kafka not reachable — run `docker compose up -d`",
)


def _read_all(topic: str, group: str, timeout_ms: int = 5000) -> list[dict]:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
    )
    out = [json.loads(m.value) for m in consumer]
    consumer.close()
    return out


def test_end_to_end_round_trip():
    suffix = uuid.uuid4().hex[:8]
    settings = Settings(
        bootstrap_servers=BOOTSTRAP,
        topic_in=f"it.in.{suffix}",
        topic_out=f"it.out.{suffix}",
        topic_dlq=f"it.dlq.{suffix}",
        group_id=f"it-grp-{suffix}",
        batch_size=10,
        partitions_in=1,
        partitions_out=1,
        partitions_dlq=1,
    )
    ensure_topics(settings)

    # Produce one good event and one deliberately malformed record.
    producer = connect_producer(settings)
    producer.send(
        settings.topic_in,
        value=json.dumps({"id": "it-1", "text": "I love this, it works great!"}).encode(),
        key=b"it-1",
    )
    producer.send(settings.topic_in, value=b"not-valid-json", key=b"it-bad")
    producer.flush()
    producer.close()

    # Consume + enrich until the input drains.
    consumer = connect_consumer(settings, settings.topic_in)
    worker = EnrichmentWorker(
        consumer, connect_producer(settings), get_chat_model(), settings, sleep=lambda _s: None
    )
    metrics = worker.run(max_idle=5)
    consumer.close()

    assert metrics.enriched >= 1
    assert metrics.dlq >= 1

    # The enriched result really landed in events.out ...
    enriched = _read_all(settings.topic_out, f"verify-out-{suffix}")
    assert any(e["id"] == "it-1" for e in enriched)
    good = next(e for e in enriched if e["id"] == "it-1")
    assert good["enrichment"]["sentiment"] == "positive"

    # ... and the malformed record really landed in events.dlq.
    dead = _read_all(settings.topic_dlq, f"verify-dlq-{suffix}")
    assert any(d["reason"] == "decode_error" for d in dead)
