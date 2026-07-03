"""A tiny message-bus interface.

The consumer logic depends only on these two small abstractions — never on
``kafka`` directly — so it can be driven by a real Kafka broker in production
and by an in-memory fake in unit tests. The surface is intentionally minimal:
just enough to poll batches, produce results, and commit offsets manually
(at-least-once).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class Record:
    """One message on the bus (mirrors a Kafka ConsumerRecord)."""

    value: bytes
    key: bytes | None = None
    topic: str = ""
    partition: int = 0
    offset: int = 0
    headers: list[tuple[str, bytes]] = field(default_factory=list)


class Producer(ABC):
    @abstractmethod
    def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        ...

    @abstractmethod
    def flush(self) -> None:
        """Block until all buffered records are acknowledged."""

    def close(self) -> None:  # pragma: no cover - trivial
        pass


class Consumer(ABC):
    @abstractmethod
    def poll(self, timeout_ms: int = 1000, max_records: int = 100) -> list[Record]:
        """Return up to ``max_records`` records, advancing the read position."""

    @abstractmethod
    def commit(self) -> None:
        """Commit the current read position as the group's offset."""

    def close(self) -> None:  # pragma: no cover - trivial
        pass
