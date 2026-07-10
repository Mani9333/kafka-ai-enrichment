"""The enrichment worker, driven entirely by the in-memory bus (no Kafka).

Covers the production behaviours: DLQ routing (decode/validation/enrichment),
idempotent dedupe, at-least-once re-delivery, and retries with backoff.
"""

import json

import pytest

from enrich.bus.memory import InMemoryBus
from enrich.config import Settings
from enrich.llm import ChatModel
from enrich.llm.mock import MockEnrichmentModel
from enrich.worker import EnrichmentWorker

SETTINGS = Settings(
    topic_in="in",
    topic_out="out",
    topic_dlq="dlq",
    batch_size=50,
    max_retries=2,
    retry_backoff_ms=0,
)


def _harness(model=None, settings=SETTINGS):
    bus = InMemoryBus()
    producer = bus.producer()
    consumer = bus.consumer(settings.topic_in, group_id="g")
    worker = EnrichmentWorker(
        consumer, producer, model or MockEnrichmentModel(), settings, sleep=lambda _s: None
    )
    return bus, consumer, worker


def _seed(bus, event: dict, key: str | None = None):
    key = key if key is not None else event.get("id", "k")
    bus.producer().send(
        SETTINGS.topic_in, value=json.dumps(event).encode(), key=key.encode()
    )


def _seed_raw(bus, raw: bytes, key: str = "raw"):
    bus.producer().send(SETTINGS.topic_in, value=raw, key=key.encode())


def _drain(consumer, worker):
    worker.process_batch(consumer.poll(max_records=100))
    consumer.commit()


def _out(bus):
    return [json.loads(v) for v in bus.values("out")]


def _dlq(bus):
    return [json.loads(v) for v in bus.values("dlq")]


def test_happy_path_routes_to_out():
    bus, consumer, worker = _harness()
    _seed(bus, {"id": "e1", "text": "I love this product, it works great!"})
    _seed(bus, {"id": "e2", "text": "The item arrived broken and I want a refund ASAP."})
    _drain(consumer, worker)

    out = _out(bus)
    assert {o["id"] for o in out} == {"e1", "e2"}
    assert worker.metrics.enriched == 2
    assert worker.metrics.dlq == 0
    assert _dlq(bus) == []


def test_undecodable_record_goes_to_dlq():
    bus, consumer, worker = _harness()
    _seed_raw(bus, b'{"id": "broken", not valid json', key="broken")
    _drain(consumer, worker)

    dlq = _dlq(bus)
    assert len(dlq) == 1
    assert dlq[0]["reason"] == "decode_error"
    assert _out(bus) == []


def test_missing_text_goes_to_dlq():
    bus, consumer, worker = _harness()
    _seed(bus, {"id": "e-empty", "text": "   "})
    _drain(consumer, worker)

    dlq = _dlq(bus)
    assert len(dlq) == 1
    assert dlq[0]["reason"] == "validation_error"
    assert dlq[0]["event_id"] == "e-empty"


def test_duplicate_ids_are_deduped():
    bus, consumer, worker = _harness()
    _seed(bus, {"id": "dup", "text": "first copy"})
    _seed(bus, {"id": "dup", "text": "second copy"})
    _drain(consumer, worker)

    assert len(_out(bus)) == 1
    assert worker.metrics.enriched == 1
    assert worker.metrics.duplicates == 1


def test_at_least_once_redelivery_does_not_duplicate_output():
    bus, consumer, worker = _harness()
    _seed(bus, {"id": "a", "text": "great, thanks!"})
    _seed(bus, {"id": "b", "text": "the app keeps crashing"})

    # Process the batch but DO NOT commit — simulate a crash before commit.
    worker.process_batch(consumer.poll(max_records=100))
    assert len(_out(bus)) == 2

    # Re-delivery of the uncommitted batch (rebalance/restart).
    consumer.reset_to_committed()
    worker.process_batch(consumer.poll(max_records=100))
    consumer.commit()

    # Still exactly two outputs: dedupe made reprocessing safe.
    assert len(_out(bus)) == 2
    assert worker.metrics.duplicates == 2


class _FlakyModel(ChatModel):
    """Raises a transient error `fails` times, then delegates to the mock."""

    name = "flaky"

    def __init__(self, fails: int) -> None:
        self.fails = fails
        self.calls = 0

    def complete(self, messages, *, temperature=0.0, max_tokens=1024):
        self.calls += 1
        if self.calls <= self.fails:
            raise RuntimeError("transient upstream error")
        return MockEnrichmentModel().complete(messages, temperature=temperature, max_tokens=max_tokens)


def test_transient_errors_are_retried_then_succeed():
    bus, consumer, worker = _harness(model=_FlakyModel(fails=2))
    _seed(bus, {"id": "r1", "text": "I love it, works great"})
    _drain(consumer, worker)

    assert len(_out(bus)) == 1
    assert worker.metrics.retries == 2
    assert worker.metrics.dlq == 0


def test_exhausted_retries_go_to_dlq():
    bus, consumer, worker = _harness(model=_FlakyModel(fails=99))
    _seed(bus, {"id": "r2", "text": "anything"})
    _drain(consumer, worker)

    dlq = _dlq(bus)
    assert len(dlq) == 1
    assert dlq[0]["reason"] == "enrichment_failed"
    assert worker.metrics.retries == SETTINGS.max_retries


class _BadJsonModel(ChatModel):
    name = "badjson"

    def complete(self, messages, *, temperature=0.0, max_tokens=1024):
        return "definitely not json"


def test_permanent_enrichment_error_is_not_retried():
    bus, consumer, worker = _harness(model=_BadJsonModel())
    _seed(bus, {"id": "p1", "text": "anything"})
    _drain(consumer, worker)

    dlq = _dlq(bus)
    assert len(dlq) == 1
    assert dlq[0]["reason"] == "enrichment_error"
    assert worker.metrics.retries == 0  # permanent errors skip the retry budget
