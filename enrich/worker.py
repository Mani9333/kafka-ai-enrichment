"""The enrichment worker — the consumer's brain, with no Kafka import in sight.

Given any :class:`~enrich.bus.base.Consumer` and :class:`~enrich.bus.base.Producer`
(real Kafka or the in-memory fake), it implements the production concerns:

* **batching** — processes whatever a poll returns, then commits once.
* **at-least-once** — commit happens only *after* results are produced and
  flushed; a crash before commit re-delivers the batch.
* **retries with backoff** — transient enrichment errors are retried with
  exponential backoff; a permanent :class:`EnrichmentError` is not.
* **dead-letter queue** — undecodable, invalid, or un-enrichable records are
  routed to the DLQ with a structured reason instead of blocking the stream.
* **idempotency** — a seen-id set makes re-delivery safe (no duplicate output).

The class is pure logic and fully unit-tested against the in-memory bus.
"""

from __future__ import annotations

import json
import time
from collections import OrderedDict
from datetime import datetime, timezone

from .bus.base import Consumer, Producer, Record
from .config import Settings
from .enrichment import EnrichmentError, enrich_event
from .llm import ChatModel
from .logging_setup import get_logger
from .metrics import Metrics
from .models import Event, ValidationError

log = get_logger("enrich.worker")


class SeenStore:
    """Bounded LRU set of processed event ids for idempotent dedupe.

    In-memory by design (per worker instance): it makes re-delivery within a
    session safe and drops duplicate inputs. A production deployment would back
    this with a persistent/compacted store (RocksDB, Redis, a compacted topic)
    to dedupe across restarts.
    """

    def __init__(self, maxsize: int = 100_000) -> None:
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self.maxsize = maxsize

    def seen(self, key: str) -> bool:
        return key in self._seen

    def add(self, key: str) -> None:
        self._seen[key] = None
        self._seen.move_to_end(key)
        while len(self._seen) > self.maxsize:
            self._seen.popitem(last=False)


class EnrichmentWorker:
    def __init__(
        self,
        consumer: Consumer,
        producer: Producer,
        model: ChatModel,
        settings: Settings,
        *,
        metrics: Metrics | None = None,
        seen: SeenStore | None = None,
        sleep=time.sleep,
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.model = model
        self.settings = settings
        self.metrics = metrics or Metrics()
        self.seen = seen or SeenStore()
        self._sleep = sleep
        self._stop = False

    # -- lifecycle ---------------------------------------------------------
    def stop(self) -> None:
        self._stop = True

    def run(self, max_idle: float | None = None) -> Metrics:
        """Poll → process → commit until stopped (or idle for ``max_idle`` s)."""
        idle_since: float | None = None
        while not self._stop:
            records = self.consumer.poll(
                timeout_ms=self.settings.poll_timeout_ms,
                max_records=self.settings.batch_size,
            )
            if records:
                idle_since = None
                self.process_batch(records)
                self.consumer.commit()  # at-least-once: commit AFTER producing
                log.info("batch.committed", extra={**self.metrics.snapshot(), "batch": len(records)})
            elif max_idle is not None:
                idle_since = idle_since or time.monotonic()
                if time.monotonic() - idle_since >= max_idle:
                    log.info("idle.exit", extra=self.metrics.snapshot())
                    break
        return self.metrics

    # -- batch processing --------------------------------------------------
    def process_batch(self, records: list[Record]) -> None:
        for record in records:
            self._process_one(record)
        self.producer.flush()  # make every result durable before we commit offsets

    def _process_one(self, record: Record) -> None:
        self.metrics.incr("processed")
        key_id = record.key.decode("utf-8", "replace") if record.key else None

        # 1) decode
        try:
            payload = json.loads(record.value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._to_dlq(record, "decode_error", str(exc), key_id)
            return

        # 2) validate → Event
        try:
            event = Event.from_dict(payload)
        except ValidationError as exc:
            self._to_dlq(record, "validation_error", str(exc), key_id or _maybe_id(payload))
            return

        # 3) idempotency
        if self.seen.seen(event.id):
            self.metrics.incr("duplicates")
            log.info("duplicate.skip", extra={"event_id": event.id})
            return

        # 4) enrich (retry transient, DLQ permanent)
        try:
            enriched = self._enrich_with_retry(event)
        except EnrichmentError as exc:
            self._to_dlq(record, "enrichment_error", str(exc), event.id)
            self.seen.add(event.id)
            return
        except Exception as exc:  # noqa: BLE001 - transient budget exhausted
            self._to_dlq(record, "enrichment_failed", str(exc), event.id)
            self.seen.add(event.id)
            return

        # 5) emit enriched result
        self.producer.send(
            self.settings.topic_out,
            value=json.dumps(enriched.to_dict()).encode("utf-8"),
            key=event.id.encode("utf-8"),
            headers=[
                ("model", self.model.name.encode()),
                ("category", enriched.enrichment.category.encode()),
                ("sentiment", enriched.enrichment.sentiment.encode()),
            ],
        )
        self.seen.add(event.id)
        self.metrics.incr("enriched")
        log.info(
            "enriched",
            extra={
                "event_id": event.id,
                "sentiment": enriched.enrichment.sentiment,
                "category": enriched.enrichment.category,
                "priority": enriched.enrichment.priority,
                "latency_ms": enriched.latency_ms,
            },
        )

    def _enrich_with_retry(self, event: Event):
        attempt = 0
        while True:
            try:
                return enrich_event(event, self.model)
            except EnrichmentError:
                raise  # permanent — do not retry, straight to DLQ
            except Exception as exc:  # noqa: BLE001 - transient (network/timeout)
                attempt += 1
                if attempt > self.settings.max_retries:
                    raise
                self.metrics.incr("retries")
                backoff = self.settings.retry_backoff_ms * (2 ** (attempt - 1)) / 1000.0
                log.warning(
                    "enrich.retry",
                    extra={"event_id": event.id, "attempt": attempt, "backoff_s": backoff, "error": str(exc)},
                )
                self._sleep(backoff)

    def _to_dlq(self, record: Record, reason: str, error: str, event_id: str | None) -> None:
        envelope = {
            "reason": reason,
            "error": error,
            "event_id": event_id,
            "original_topic": record.topic,
            "original_partition": record.partition,
            "original_offset": record.offset,
            "original_key": record.key.decode("utf-8", "replace") if record.key else None,
            "payload": record.value.decode("utf-8", "replace"),
            "failed_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        self.producer.send(
            self.settings.topic_dlq,
            value=json.dumps(envelope).encode("utf-8"),
            key=(event_id or "unknown").encode("utf-8"),
            headers=[("reason", reason.encode()), ("error", error[:200].encode("utf-8", "replace"))],
        )
        self.metrics.incr("dlq")
        log.warning("dead_letter", extra={"event_id": event_id, "reason": reason, "error": error})


def _maybe_id(payload) -> str | None:
    return payload.get("id") if isinstance(payload, dict) and isinstance(payload.get("id"), str) else None
