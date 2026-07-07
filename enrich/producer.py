"""Publish sample events to the input topic.

    python -m enrich.producer                 # send the bundled sample dataset
    python -m enrich.producer --repeat 50      # 50x the dataset (throughput demo)
    python -m enrich.producer --no-malformed   # skip the deliberately-bad record

By default it also emits one **malformed** (non-JSON) record so you can watch it
land in the dead-letter topic. Each event is keyed by its id, so all events for
the same id hash to the same partition (ordering + dedupe behave as expected).
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from dotenv import load_dotenv

from .bus.kafka_bus import connect_producer, ensure_topics, wait_for_broker
from .config import Settings
from .logging_setup import get_logger, setup_logging

log = get_logger("enrich.producer")

_DEFAULT_FILE = Path(__file__).resolve().parent.parent / "data" / "sample_events.jsonl"


def _load_events(path: Path) -> list[dict]:
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            events.append(json.loads(line))
    return events


def produce(settings: Settings, file: Path, repeat: int, malformed: bool) -> int:
    ensure_topics(settings)
    producer = connect_producer(settings)
    sent = 0
    try:
        base = _load_events(file)
        for r in range(repeat):
            for event in base:
                event_id = event["id"] if repeat == 1 else f"{event['id']}-r{r}"
                record = dict(event, id=event_id)
                producer.send(
                    settings.topic_in,
                    value=json.dumps(record).encode("utf-8"),
                    key=event_id.encode("utf-8"),
                )
                sent += 1
                log.info("produced", extra={"event_id": event_id, "topic": settings.topic_in})

        if malformed:
            producer.send(
                settings.topic_in,
                value=b'{"id": "evt-broken", "text": "this payload is not valid json',  # truncated on purpose
                key=b"evt-broken",
            )
            sent += 1
            log.warning("produced.malformed", extra={"event_id": "evt-broken", "topic": settings.topic_in})

        producer.flush()
    finally:
        producer.close()
    log.info("produce.done", extra={"sent": sent, "topic": settings.topic_in})
    return sent


def main() -> None:
    load_dotenv()
    setup_logging()
    parser = argparse.ArgumentParser(description="Produce sample events to Kafka.")
    parser.add_argument("--file", type=Path, default=_DEFAULT_FILE)
    parser.add_argument("--repeat", type=int, default=1, help="send the dataset N times")
    parser.add_argument("--no-malformed", dest="malformed", action="store_false")
    parser.add_argument("--wait", action="store_true", help="wait for the broker first")
    parser.add_argument("--bootstrap", default=None, help="override KAFKA_BOOTSTRAP")
    args = parser.parse_args()

    settings = Settings.load()
    if args.bootstrap:
        settings = Settings(bootstrap_servers=args.bootstrap)
    if args.wait:
        wait_for_broker(settings.bootstrap_servers)

    start = time.perf_counter()
    sent = produce(settings, args.file, args.repeat, args.malformed)
    elapsed = time.perf_counter() - start
    print(f"produced {sent} events to {settings.topic_in} in {elapsed:.2f}s ({sent / max(elapsed, 1e-9):.0f}/s)")


if __name__ == "__main__":
    main()
