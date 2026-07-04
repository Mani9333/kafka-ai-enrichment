"""Run the enrichment consumer against a real Kafka broker.

    python -m enrich.consumer                 # run until Ctrl-C (graceful commit on exit)
    python -m enrich.consumer --max-idle 8      # exit after 8s with no new messages (demo)

Wires the Kafka consumer/producer to the provider-agnostic :class:`EnrichmentWorker`
and the LLM selected by ``LLM_PROVIDER`` (default: offline mock). Reads from
``events.in`` as part of a consumer group, writes enriched results to
``events.out`` and failures to ``events.dlq``.
"""

from __future__ import annotations

import argparse
import signal

from dotenv import load_dotenv

from .bus.kafka_bus import connect_consumer, connect_producer, ensure_topics, wait_for_broker
from .config import Settings
from .llm import get_chat_model
from .logging_setup import get_logger, setup_logging
from .worker import EnrichmentWorker

log = get_logger("enrich.consumer")


def main() -> None:
    load_dotenv()
    setup_logging()
    parser = argparse.ArgumentParser(description="Run the enrichment consumer.")
    parser.add_argument("--max-idle", type=float, default=None, help="exit after N idle seconds")
    parser.add_argument("--wait", action="store_true", default=True, help="wait for the broker first")
    parser.add_argument("--no-wait", dest="wait", action="store_false")
    parser.add_argument("--bootstrap", default=None, help="override KAFKA_BOOTSTRAP")
    args = parser.parse_args()

    settings = Settings.load()
    if args.bootstrap:
        settings = Settings(bootstrap_servers=args.bootstrap)

    if args.wait:
        wait_for_broker(settings.bootstrap_servers)
    ensure_topics(settings)

    model = get_chat_model()
    producer = connect_producer(settings)
    consumer = connect_consumer(settings, settings.topic_in)
    worker = EnrichmentWorker(consumer, producer, model, settings)

    # Graceful shutdown: finish the loop, commit, and flush before exiting.
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: worker.stop())

    log.info(
        "consumer.start",
        extra={
            "provider": model.name,
            "group": settings.group_id,
            "topic_in": settings.topic_in,
            "topic_out": settings.topic_out,
            "topic_dlq": settings.topic_dlq,
            "bootstrap": settings.bootstrap_servers,
        },
    )
    try:
        metrics = worker.run(max_idle=args.max_idle)
    finally:
        producer.flush()
        consumer.close()
        producer.close()
    print("consumer stopped:", metrics.snapshot())


if __name__ == "__main__":
    main()
