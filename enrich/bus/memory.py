"""An in-memory bus that models Kafka's semantics for tests.

Each topic is an append-only log (a list). A consumer keeps a *read position*
and a separately-tracked *committed offset*, exactly like Kafka. That lets unit
tests exercise real behaviours without a broker:

* **at-least-once**: :meth:`InMemoryConsumer.reset_to_committed` re-delivers any
  records that were processed but not yet committed (a crash/rebalance).
* **lag**: :meth:`InMemoryConsumer.lag` = log end offset − committed offset.

Tests can also inspect the output directly, e.g. ``bus.values("events.out")``.
"""

from __future__ import annotations

from collections import defaultdict

from .base import Consumer, Producer, Record


class InMemoryBus:
    def __init__(self) -> None:
        self.topics: dict[str, list[Record]] = defaultdict(list)

    def producer(self) -> "InMemoryProducer":
        return InMemoryProducer(self)

    def consumer(self, *topics: str, group_id: str = "g") -> "InMemoryConsumer":
        return InMemoryConsumer(self, list(topics), group_id)

    def _append(self, topic, value, key, headers) -> Record:
        log = self.topics[topic]
        record = Record(
            value=value,
            key=key,
            topic=topic,
            partition=0,
            offset=len(log),
            headers=list(headers or []),
        )
        log.append(record)
        return record

    def records(self, topic: str) -> list[Record]:
        return list(self.topics.get(topic, []))

    def values(self, topic: str) -> list[bytes]:
        return [r.value for r in self.topics.get(topic, [])]


class InMemoryProducer(Producer):
    def __init__(self, bus: InMemoryBus) -> None:
        self.bus = bus

    def send(self, topic, value, key=None, headers=None) -> None:
        self.bus._append(topic, value, key, headers)

    def flush(self) -> None:
        pass


class InMemoryConsumer(Consumer):
    def __init__(self, bus: InMemoryBus, topics: list[str], group_id: str) -> None:
        self.bus = bus
        self.topics = topics
        self.group_id = group_id
        self.position: dict[str, int] = {t: 0 for t in topics}
        self.committed: dict[str, int] = {t: 0 for t in topics}

    def poll(self, timeout_ms: int = 0, max_records: int = 100) -> list[Record]:
        out: list[Record] = []
        for topic in self.topics:
            log = self.bus.topics.get(topic, [])
            while self.position[topic] < len(log) and len(out) < max_records:
                out.append(log[self.position[topic]])
                self.position[topic] += 1
        return out

    def commit(self) -> None:
        self.committed = dict(self.position)

    def reset_to_committed(self) -> None:
        """Simulate a crash/rebalance: re-deliver uncommitted records."""
        self.position = dict(self.committed)

    def lag(self) -> int:
        return sum(
            len(self.bus.topics.get(t, [])) - self.committed[t] for t in self.topics
        )
