"""Block until the Kafka broker is reachable (used by `make demo`/`make up`)."""

from __future__ import annotations

import sys

from dotenv import load_dotenv

from .bus.kafka_bus import wait_for_broker
from .config import Settings


def main() -> None:
    load_dotenv()
    settings = Settings.load()
    timeout = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
    print(f"waiting for Kafka at {settings.bootstrap_servers} (up to {timeout:.0f}s)...")
    wait_for_broker(settings.bootstrap_servers, timeout=timeout)
    print("Kafka is ready.")


if __name__ == "__main__":
    main()
