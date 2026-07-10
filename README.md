# kafka-ai-enrichment — real-time event enrichment with an LLM

A streaming pipeline that reads events off a **Kafka** topic, enriches each one
with an **LLM** (sentiment + category + priority + extracted entities → structured
JSON), and writes the results back to Kafka. It's built to show the *production*
mechanics of stream processing — consumer groups, batching, **at-least-once**
delivery with manual offset commits, retries with backoff, a **dead-letter
queue**, idempotent dedupe, structured logging, and live metrics — with clean,
swappable parts rather than a wall of framework glue.

It runs **offline with zero API keys** (a deterministic, rule-based mock model)
and upgrades to a real model (OpenAI / Anthropic / Ollama) through a single
environment variable. A bundled **Kafka UI** and a small **FastAPI dashboard**
make the whole thing easy to *watch*.

## Architecture

```
   ┌───────────┐    events.in     ┌────────────┐    events.out    ┌─────────────────────┐
   │  producer │ ───────────────▶ │  consumer  │ ───────────────▶ │   enriched events    │
   └───────────┘   3 partitions   └─────┬──────┘                  │   → events.out       │
                                        │                         └─────────────────────┘
                                        │          events.dlq      ┌─────────────────────┐
                                        └────────────────────────▶ │   dead-letter queue  │
                                                                   │   reason + payload   │
                                                                   └─────────────────────┘
   consumer = group "enrichment-workers"; for each event it runs:
       decode → validate → dedupe(by id) → enrich(LLM) → route to events.out | events.dlq
                                               │
   LLM layer (swappable, offline by default):  mock | openai | anthropic | ollama

   watch it live:
       Kafka UI    http://localhost:8080    topics · live messages · consumer-group lag
       dashboard   http://localhost:8101    processed · errors · throughput · lag · rows
```

Two things make it realistic: the consumer commits offsets **only after** results
are safely produced (so a crash re-delivers, never silently drops — *at-least-once*),
and anything it can't decode, validate, or enrich is routed to a **dead-letter
queue** with a structured reason instead of blocking the stream.

## Quick start (local, no keys)

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

# 1) start Kafka (KRaft, no ZooKeeper) + Kafka UI
docker compose up -d

# 2) run the whole demo: wait for the broker, produce sample events, enrich them
make demo
```

`make demo` publishes 12 sample events plus one deliberately-malformed record,
then runs the consumer until the topic drains. You'll see structured logs like:

```json
{"ts":"…","level":"INFO","logger":"enrich.worker","event":"enriched","event_id":"evt-001","sentiment":"negative","category":"product_quality","priority":"high","latency_ms":0.13}
{"ts":"…","level":"WARNING","logger":"enrich.worker","event":"dead_letter","event_id":"evt-broken","reason":"decode_error","error":"Unterminated string …"}
{"ts":"…","level":"INFO","logger":"enrich.worker","event":"batch.committed","processed":13,"enriched":11,"dlq":2,"throughput_per_s":15.6}
```

Then watch it live:

```bash
# Kafka UI — browse topics, live messages, and consumer-group lag
open http://localhost:8080

# the project's own dashboard — counters, throughput, lag, latest enriched rows
make dashboard        # then open http://localhost:8101
```

Tear everything down (removes the broker's data):

```bash
docker compose down -v
```

## New to Kafka? The 60-second model

Kafka is a **distributed append-only log**. A few terms are all you need here:

- **Topic** — a named, ordered, append-only log of messages. This project uses
  three: `events.in` (raw), `events.out` (enriched), `events.dlq` (failures).
- **Partition** — a topic is split into partitions so it can scale and preserve
  order *within* a partition. A message's **key** (here, the event id) decides
  its partition, so all messages for one id stay ordered. `events.in` has 3.
- **Offset** — a message's position in a partition (0, 1, 2, …). Immutable.
- **Producer** — appends messages to a topic (our `producer.py`).
- **Consumer group** — one or more consumers that split a topic's partitions
  between them. Each partition is owned by exactly one member, so you scale
  throughput by adding consumers (up to the partition count). Our group is
  `enrichment-workers`.
- **Committed offset** — the last offset a group has *acknowledged*. On restart,
  it resumes from there.
- **Lag** — `log-end offset − committed offset`: how far behind the consumer is.
  The dashboard and Kafka UI both show it; it's the #1 health signal for a
  streaming job.
- **Dead-letter queue (DLQ)** — a separate topic where a consumer parks messages
  it can't process, so one poison message doesn't wedge the pipeline.

## Repository layout

```
enrich/
├── config.py         # env-driven settings (broker, topics, batch, retries)
├── models.py         # Event (in) and EnrichedEvent (out) dataclasses + validation
├── prompts.py        # the single enrichment prompt (strict JSON schema)
├── enrichment.py     # Event + ChatModel -> EnrichedEvent  (no Kafka; unit-tested)
├── worker.py         # the consumer's brain: batch, retry, DLQ, dedupe, commit  ← core
├── producer.py       # publish sample events to events.in
├── consumer.py       # wire Kafka to the worker + run the loop (CLI)
├── dashboard.py      # FastAPI live dashboard (reads state straight from Kafka)
├── metrics.py        # in-process counters (processed / enriched / dlq / throughput)
├── logging_setup.py  # one-JSON-object-per-line structured logging
├── bus/              # tiny message-bus interface so the worker is Kafka-agnostic
│   ├── base.py       #   Record + Producer/Consumer ABCs
│   ├── kafka_bus.py  #   kafka-python implementation + wait/ensure-topics helpers
│   └── memory.py     #   in-memory fake (append-only logs) for hermetic tests
└── llm/              # provider-agnostic text-in/text-out model layer (shared design)
    ├── base.py       #   Message + ChatModel ABC
    ├── providers.py  #   OpenAI / Anthropic / Ollama over httpx (no vendor SDKs)
    ├── mock.py       #   deterministic rule-based enrichment (the offline default)
    └── factory.py    #   get_chat_model() from LLM_PROVIDER
data/sample_events.jsonl   # customer messages + product reviews (+ one bad event)
static/index.html          # the dashboard's single page
tests/                     # hermetic suite + one auto-skipping Kafka integration test
docker-compose.yml         # Kafka (KRaft) + Kafka UI
Makefile                   # up / down / produce / consume / dashboard / demo / test
```

## How it works

**Producing.** `producer.py` reads `data/sample_events.jsonl` and publishes each
event to `events.in`, **keyed by event id** (so an id always lands on the same
partition). It also emits one non-JSON record on purpose so you can watch the DLQ.

**Enriching.** For each incoming record the worker (`worker.py`) runs a small,
explicit pipeline:

1. **decode** the JSON payload — failure → DLQ (`decode_error`).
2. **validate** it into an `Event` (needs a non-empty `id` and `text`) —
   failure → DLQ (`validation_error`).
3. **dedupe** by event id — a already-seen id is skipped (idempotency).
4. **enrich**: build the prompt, call the model, parse + validate the returned
   JSON into an `Enrichment`. Transient errors (network/timeout) are **retried
   with exponential backoff**; a permanent bad-output error → DLQ
   (`enrichment_error`); exhausted retries → DLQ (`enrichment_failed`).
5. **emit** the `EnrichedEvent` to `events.out`, keyed by id, with `model` /
   `category` / `sentiment` in the Kafka headers.

After the whole batch is produced and **flushed**, the worker **commits** the
consumer offsets — that ordering is what makes it at-least-once.

**Enrichment output** (what lands in `events.out`):

```json
{
  "id": "evt-001",
  "type": "support_message",
  "text": "My order #100582 arrived cracked and the screen is broken. I need a refund ASAP.",
  "enrichment": {
    "sentiment": "negative",
    "category": "product_quality",
    "priority": "high",
    "entities": ["#100582"],
    "summary": "My order #100582 arrived cracked and the screen is broken.",
    "sentiment_score": -3
  },
  "model": "mock",
  "enriched_at": "2026-01-01T00:00:00.000+00:00",
  "latency_ms": 0.13,
  "customer_id": "cust-4021"
}
```

## Local setup (all options)

Everything is chosen by environment variables — no code changes. The defaults
(`LLM_PROVIDER=mock`, broker on `localhost:9092`) need **no keys and no cloud**.
All settings live in [`.env.example`](.env.example) and load automatically from
a `.env`.

### The Kafka stack (Docker)

```bash
docker compose up -d          # Kafka in KRaft mode (port 9092) + Kafka UI (port 8080)
docker compose ps             # both should be "healthy" / "running"
docker compose logs -f kafka  # broker logs
docker compose down -v        # stop and delete the broker's data
```

The broker runs a single node in **KRaft mode** (no ZooKeeper) with two
listeners: `localhost:9092` for host clients (this app) and `kafka:19092` inside
the Docker network (Kafka UI). Topics are auto-created on first connect —
`events.in` with 3 partitions (so consumer groups and lag are meaningful),
`events.out` with 3, and `events.dlq` with 1.

### Run the pieces individually

```bash
make produce                  # publish the sample events to events.in
make consume                  # run the enrichment consumer (Ctrl-C for a graceful commit + exit)
make dashboard                # live dashboard on http://localhost:8101
make test                     # hermetic test suite (no Kafka required)

# or drive the CLIs directly for more control:
python -m enrich.producer --repeat 50 --no-malformed   # 50x the dataset (throughput demo)
python -m enrich.consumer --max-idle 8                 # exit after 8s idle (used by `make demo`)
```

Scale the consumer group by running `make consume` in **two terminals**: Kafka
rebalances the 3 partitions of `events.in` across the two workers and you'll see
each one own a subset.

### Enrichment model — pick one

```bash
# a) Offline (default): deterministic, rule-based, zero setup
export LLM_PROVIDER=mock

# b) Ollama — local & free. Install from https://ollama.com, then:
ollama pull llama3.1
export LLM_PROVIDER=ollama OLLAMA_MODEL=llama3.1

# c) Hosted API (needs a key)
export LLM_PROVIDER=openai    OPENAI_API_KEY=sk-...          # or:
export LLM_PROVIDER=anthropic ANTHROPIC_API_KEY=sk-ant-...
```

Every provider gets the same prompt, which pins the output to the exact JSON
schema the mock produces — so `events.out` looks identical regardless of model,
and only the *quality* of the labels changes.

## Watching it work

**Kafka UI** (`http://localhost:8080`) is the general-purpose view: click into
`events.in` / `events.out` / `events.dlq` to see live messages, and open
**Consumers → enrichment-workers** to watch per-partition lag drop to zero as the
consumer catches up.

**The project dashboard** (`make dashboard` → `http://localhost:8101`) is a
focused, auto-refreshing view built for this pipeline. It reads its state
*directly from Kafka* (end offsets for totals, committed-vs-end offsets for lag),
so it's accurate no matter which process produced the data:

```
 Processed   Enriched   Dead-letter   Throughput/s   Consumer lag
    52          44           8            3.3             0

 Latest enriched events (events.out)
 evt-011  support   NEUTRAL   shipping   low    Dyson V15    Do you ship to Canada?     0.04ms
 evt-010  review    NEGATIVE  product…   high                The blender stopped …      0.08ms
 …
```

## Testing

```bash
pip install -r requirements-dev.txt
pytest
```

The suite is **hermetic and offline** — it never touches a network or a broker.
The consumer logic is tested against the **in-memory bus** (`enrich/bus/memory.py`),
which models Kafka's semantics (append-only logs, read position vs. committed
offset) closely enough to prove the interesting behaviours:

- enrichment + entity extraction on the deterministic mock,
- DLQ routing for `decode_error`, `validation_error`, and enrichment failures,
- **idempotent dedupe** (a repeated id produces one output),
- **at-least-once re-delivery** (reprocessing an uncommitted batch doesn't
  duplicate output),
- **retries with backoff** (transient errors recover; a permanent error doesn't
  waste the retry budget).

There's also an **integration test** that **auto-skips** unless a broker is
reachable — start Kafka first to run it (it does a real produce → enrich →
consume round-trip and asserts the good event lands in `events.out` and the bad
one in `events.dlq`):

```bash
docker compose up -d
pytest tests/test_kafka_integration.py
```

## Design decisions & tradeoffs

- **At-least-once, not exactly-once.** The consumer disables auto-commit and
  commits offsets only *after* results are produced and flushed. If it crashes
  mid-batch, the batch is re-delivered — never silently lost. True exactly-once
  needs the idempotent/transactional producer, which `kafka-python` doesn't
  offer; rather than pretend, this project makes reprocessing **safe** with
  dedupe-by-key. Honest and simple beats a leaky abstraction.
- **A dead-letter queue instead of crashing or dropping.** A single poison
  message (bad JSON, missing fields, a model that won't return valid JSON) must
  not wedge the stream or vanish. It's parked in `events.dlq` with a structured
  reason + the original payload, so it's debuggable and replayable. Failures are
  classified as *permanent* (→ DLQ immediately) vs. *transient* (→ retry, then
  DLQ) so the retry budget isn't wasted on hopeless records.
- **Batch, then commit.** The worker processes whatever a poll returns and
  commits once per batch. Fewer commits and producer round-trips = higher
  throughput; the batch size (`BATCH_SIZE`) trades latency for throughput.
- **In-memory dedupe (with eyes open).** The seen-id set makes re-delivery and
  duplicate inputs safe *within a running worker*. It is intentionally not
  durable across restarts — the production upgrade is a compacted topic /
  RocksDB / Redis. Called out rather than hidden.
- **A tiny message-bus interface.** The worker depends on a two-method
  `Consumer`/`Producer` abstraction, never on `kafka` directly. That's what lets
  the entire consumer be unit-tested with an in-memory fake and keeps the door
  open for another broker later.
- **Mock model by default.** So the pipeline is genuinely runnable with zero
  keys and the tests stay deterministic. It's rule-based (keyword sentiment,
  regex entities) — not as nuanced as a real model, but it exercises the exact
  prompt → JSON → parse → route path, so switching to `LLM_PROVIDER=openai`
  changes only label quality, not plumbing.
- **The dashboard reads from Kafka, not shared memory.** Producer, consumer, and
  dashboard are separate processes; deriving the dashboard's view from Kafka
  offsets means it's always correct and requires no cross-process coupling.

## Notes

A focused project to demonstrate real-time enrichment on Kafka end-to-end.
Deliberately out of scope: a schema registry / Avro, multi-broker replication,
exactly-once transactions, DLQ auto-replay, and autoscaling. No LangGraph is used
or needed — the value here is the streaming and reliability plumbing.
