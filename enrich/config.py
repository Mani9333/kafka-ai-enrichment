"""Environment-driven settings (all have sensible offline defaults).

Everything about the pipeline — broker address, topic names, consumer group,
batch size, retry policy — is configured here so the producer, consumer, and
dashboard share one source of truth and nothing is hard-coded.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic_in: str = os.getenv("TOPIC_IN", "events.in")
    topic_out: str = os.getenv("TOPIC_OUT", "events.out")
    topic_dlq: str = os.getenv("TOPIC_DLQ", "events.dlq")
    group_id: str = os.getenv("CONSUMER_GROUP", "enrichment-workers")

    batch_size: int = _int("BATCH_SIZE", 100)
    poll_timeout_ms: int = _int("POLL_TIMEOUT_MS", 1000)
    max_retries: int = _int("MAX_RETRIES", 3)
    retry_backoff_ms: int = _int("RETRY_BACKOFF_MS", 200)

    dashboard_port: int = _int("DASHBOARD_PORT", 8101)

    # Partition counts used when the topics are auto-created on first connect.
    # events.in has multiple partitions to make consumer groups / lag meaningful.
    partitions_in: int = _int("PARTITIONS_IN", 3)
    partitions_out: int = _int("PARTITIONS_OUT", 3)
    partitions_dlq: int = _int("PARTITIONS_DLQ", 1)

    @staticmethod
    def load() -> "Settings":
        return Settings()
