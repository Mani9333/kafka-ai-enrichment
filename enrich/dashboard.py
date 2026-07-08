"""A small FastAPI live dashboard (default port 8101).

It reads the pipeline's state **directly from Kafka** — so it reflects reality no
matter which process produced the data:

* a background thread tails ``events.out`` and ``events.dlq`` into rolling buffers
  (the "store of recent results"),
* end offsets give the total enriched / error counts,
* the consumer group's committed offsets vs. the log-end offsets give real
  **consumer lag** on ``events.in``,
* throughput is computed from the timestamps of recently enriched events.

    uvicorn enrich.dashboard:app --port 8101      # or: python -m enrich.dashboard
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

from .config import Settings
from .logging_setup import get_logger

log = get_logger("enrich.dashboard")
_STATIC = Path(__file__).resolve().parent.parent / "static"


class DashboardMonitor:
    """Background Kafka reader that maintains the dashboard's view of the world."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.Lock()
        self.enriched: deque[dict] = deque(maxlen=250)
        self.dlq: deque[dict] = deque(maxlen=100)
        self.connected = False
        self.enriched_total = 0
        self.error_total = 0
        self.lag: int | None = None
        self._stop = threading.Event()
        self._tail_thread: threading.Thread | None = None
        self._stats_thread: threading.Thread | None = None

    def start(self) -> None:
        self._tail_thread = threading.Thread(target=self._tail_loop, daemon=True)
        self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
        self._tail_thread.start()
        self._stats_thread.start()

    def stop(self) -> None:
        self._stop.set()

    # -- background loops --------------------------------------------------
    def _tail_loop(self) -> None:
        from kafka import KafkaConsumer

        consumer = None
        while not self._stop.is_set():
            try:
                if consumer is None:
                    consumer = KafkaConsumer(
                        self.settings.topic_out,
                        self.settings.topic_dlq,
                        bootstrap_servers=self.settings.bootstrap_servers,
                        group_id=f"dashboard-{uuid4().hex[:8]}",
                        auto_offset_reset="earliest",
                        enable_auto_commit=False,
                        api_version_auto_timeout_ms=3000,
                    )
                    self.connected = True
                batches = consumer.poll(timeout_ms=1000, max_records=500)
                for _tp, messages in batches.items():
                    for m in messages:
                        self._ingest(m.topic, m.value)
            except Exception as exc:  # noqa: BLE001 - broker may be down; retry
                self.connected = False
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                consumer = None
                log.warning("dashboard.tail_retry", extra={"error": str(exc)})
                self._stop.wait(2.0)

    def _ingest(self, topic: str, value: bytes) -> None:
        try:
            data = json.loads(value)
        except Exception:
            return
        with self._lock:
            if topic == self.settings.topic_out:
                enr = data.get("enrichment", {})
                self.enriched.append(
                    {
                        "id": data.get("id"),
                        "type": data.get("type"),
                        "sentiment": enr.get("sentiment"),
                        "category": enr.get("category"),
                        "priority": enr.get("priority"),
                        "entities": enr.get("entities", []),
                        "summary": enr.get("summary", ""),
                        "model": data.get("model"),
                        "latency_ms": data.get("latency_ms"),
                        "enriched_at": data.get("enriched_at"),
                        "_epoch": _epoch(data.get("enriched_at")),
                    }
                )
            else:
                self.dlq.append(
                    {
                        "event_id": data.get("event_id"),
                        "reason": data.get("reason"),
                        "error": data.get("error"),
                        "failed_at": data.get("failed_at"),
                    }
                )

    def _stats_loop(self) -> None:
        while not self._stop.is_set():
            self._refresh_offsets()
            self._stop.wait(2.0)

    def _refresh_offsets(self) -> None:
        from kafka import KafkaAdminClient, KafkaConsumer
        from kafka.structs import TopicPartition

        consumer = None
        admin = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.settings.bootstrap_servers,
                api_version_auto_timeout_ms=3000,
            )
            consumer.topics()  # force a metadata refresh

            def end_offsets(topic: str) -> dict:
                parts = consumer.partitions_for_topic(topic) or set()
                tps = [TopicPartition(topic, p) for p in parts]
                return consumer.end_offsets(tps) if tps else {}

            out_ends = end_offsets(self.settings.topic_out)
            dlq_ends = end_offsets(self.settings.topic_dlq)
            in_ends = end_offsets(self.settings.topic_in)

            admin = KafkaAdminClient(bootstrap_servers=self.settings.bootstrap_servers)
            committed = admin.list_consumer_group_offsets(self.settings.group_id)

            lag = 0
            for tp, end in in_ends.items():
                com = committed.get(tp)
                lag += max(end - (com.offset if com else 0), 0)

            with self._lock:
                self.enriched_total = sum(out_ends.values())
                self.error_total = sum(dlq_ends.values())
                self.lag = lag
                self.connected = True
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self.connected = False
            log.warning("dashboard.stats_retry", extra={"error": str(exc)})
        finally:
            for client in (consumer, admin):
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass

    # -- snapshot for the API ---------------------------------------------
    def state(self) -> dict:
        now = time.time()
        window = 10.0
        with self._lock:
            # Copy (without the internal _epoch) so we never mutate the buffer.
            recent = [
                {k: v for k, v in e.items() if k != "_epoch"}
                for e in list(self.enriched)[-25:][::-1]
            ]
            recent_dlq = [dict(d) for d in list(self.dlq)[-10:][::-1]]
            throughput = sum(
                1 for e in self.enriched if e["_epoch"] and now - e["_epoch"] <= window
            ) / window
            enriched_total = self.enriched_total
            error_total = self.error_total
            lag = self.lag
            connected = self.connected
        return {
            "connected": connected,
            "broker": self.settings.bootstrap_servers,
            "group": self.settings.group_id,
            "topics": {
                "in": self.settings.topic_in,
                "out": self.settings.topic_out,
                "dlq": self.settings.topic_dlq,
            },
            "processed": enriched_total + error_total,
            "enriched": enriched_total,
            "errors": error_total,
            "lag": lag,
            "throughput_per_s": round(throughput, 2),
            "recent": recent,
            "recent_dlq": recent_dlq,
        }


def _epoch(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


load_dotenv()
_settings = Settings.load()
_monitor = DashboardMonitor(_settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _monitor.start()  # background threads tail Kafka + refresh offsets
    try:
        yield
    finally:
        _monitor.stop()


app = FastAPI(title="kafka-ai-enrichment dashboard", version="0.1.0", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


@app.get("/api/state")
def api_state() -> JSONResponse:
    return JSONResponse(_monitor.state())


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "connected": _monitor.connected, "broker": _settings.bootstrap_servers}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=_settings.dashboard_port)


if __name__ == "__main__":
    main()
