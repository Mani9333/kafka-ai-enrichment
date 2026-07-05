"""Core enrichment: Event + ChatModel -> EnrichedEvent.

This is the heart of the pipeline and is deliberately free of any Kafka imports
so it can be unit-tested directly. The consumer calls :func:`enrich_event` for
each message; a permanent failure (unparseable/invalid model output) raises
:class:`EnrichmentError` and the consumer routes the record to the DLQ.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone

from .llm import ChatModel
from .models import Enrichment, EnrichedEvent, Event
from .prompts import CATEGORIES, build_enrichment_messages

_VALID_SENTIMENT = {"positive", "neutral", "negative"}
_VALID_PRIORITY = {"low", "medium", "high"}
_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


class EnrichmentError(Exception):
    """Permanent enrichment failure (bad/invalid model output) — route to DLQ.

    Distinct from transient errors (network/timeout) which the consumer retries.
    """


def _parse_json(raw: str) -> dict:
    text = _FENCE.sub("", raw.strip())
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise EnrichmentError(f"model did not return valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise EnrichmentError("model JSON is not an object")
    return data


def _coerce_enrichment(data: dict) -> Enrichment:
    sentiment = str(data.get("sentiment", "")).lower().strip()
    category = str(data.get("category", "")).lower().strip()
    priority = str(data.get("priority", "low")).lower().strip()

    if sentiment not in _VALID_SENTIMENT:
        raise EnrichmentError(f"invalid sentiment: {sentiment!r}")
    if category not in CATEGORIES:
        category = "other"  # tolerate a novel label rather than dead-lettering
    if priority not in _VALID_PRIORITY:
        priority = "low"

    entities_raw = data.get("entities", [])
    if not isinstance(entities_raw, list):
        raise EnrichmentError("entities must be a list")
    entities = [str(e).strip() for e in entities_raw if str(e).strip()][:12]

    return Enrichment(
        sentiment=sentiment,
        category=category,
        priority=priority,
        entities=entities,
        summary=str(data.get("summary", ""))[:280],
        sentiment_score=int(data.get("sentiment_score", 0)) if str(data.get("sentiment_score", "0")).lstrip("-").isdigit() else 0,
    )


def enrich_event(event: Event, model: ChatModel) -> EnrichedEvent:
    """Enrich a single event. Raises :class:`EnrichmentError` on bad model output."""
    messages = build_enrichment_messages(event.text)
    start = time.perf_counter()
    raw = model.complete(messages, temperature=0.0, max_tokens=512)
    latency_ms = round((time.perf_counter() - start) * 1000, 2)

    enrichment = _coerce_enrichment(_parse_json(raw))
    return EnrichedEvent(
        id=event.id,
        type=event.type,
        text=event.text,
        enrichment=enrichment,
        model=model.name,
        enriched_at=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        latency_ms=latency_ms,
        customer_id=event.customer_id,
    )
