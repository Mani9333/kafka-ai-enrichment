"""In-process counters for the enrichment consumer.

Small and thread-safe. The consumer updates these as it works and logs a
``snapshot()`` periodically (processed / enriched / dlq counts and throughput).
The live dashboard derives its own view directly from Kafka, so these counters
stay simple and dependency-free.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class Metrics:
    processed: int = 0
    enriched: int = 0
    dlq: int = 0
    retries: int = 0
    duplicates: int = 0
    started_at: float = field(default_factory=time.monotonic)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def incr(self, field_name: str, amount: int = 1) -> None:
        with self._lock:
            setattr(self, field_name, getattr(self, field_name) + amount)

    @property
    def elapsed_s(self) -> float:
        return max(time.monotonic() - self.started_at, 1e-9)

    @property
    def throughput(self) -> float:
        return self.processed / self.elapsed_s

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "processed": self.processed,
                "enriched": self.enriched,
                "dlq": self.dlq,
                "retries": self.retries,
                "duplicates": self.duplicates,
                "elapsed_s": round(self.elapsed_s, 2),
                "throughput_per_s": round(self.throughput, 2),
            }
