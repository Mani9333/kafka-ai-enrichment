"""Domain types: the raw event that comes in and the enriched event that goes out.

Plain dataclasses (no framework types) so the core is easy to test and the JSON
shape on the wire is obvious.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


class ValidationError(ValueError):
    """Raised when an incoming event is missing required fields.

    Non-retryable: the record is bad, so it goes straight to the dead-letter
    queue rather than being retried.
    """


@dataclass
class Event:
    id: str
    text: str
    type: str = "support_message"
    customer_id: str | None = None
    ts: str | None = None
    extra: dict = field(default_factory=dict)

    @staticmethod
    def from_dict(data: dict) -> "Event":
        if not isinstance(data, dict):
            raise ValidationError("event payload is not a JSON object")
        event_id = data.get("id")
        text = data.get("text")
        if not event_id or not isinstance(event_id, str):
            raise ValidationError("event is missing a string 'id'")
        if not text or not isinstance(text, str) or not text.strip():
            raise ValidationError("event is missing non-empty 'text'")
        known = {"id", "text", "type", "customer_id", "ts"}
        return Event(
            id=event_id,
            text=text,
            type=data.get("type", "support_message"),
            customer_id=data.get("customer_id"),
            ts=data.get("ts"),
            extra={k: v for k, v in data.items() if k not in known},
        )


@dataclass
class Enrichment:
    sentiment: str  # positive | neutral | negative
    category: str
    priority: str  # low | medium | high
    entities: list[str]
    summary: str
    sentiment_score: int = 0


@dataclass
class EnrichedEvent:
    id: str
    type: str
    text: str
    enrichment: Enrichment
    model: str
    enriched_at: str
    latency_ms: float
    customer_id: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)
