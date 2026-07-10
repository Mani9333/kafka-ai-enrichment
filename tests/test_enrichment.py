"""Core enrichment: valid model output becomes an EnrichedEvent; bad output raises."""

import json

import pytest

from enrich.enrichment import EnrichmentError, enrich_event
from enrich.llm import ChatModel, Message
from enrich.llm.mock import MockEnrichmentModel
from enrich.models import Event


class _CannedModel(ChatModel):
    name = "canned"

    def __init__(self, reply: str) -> None:
        self._reply = reply

    def complete(self, messages, *, temperature=0.0, max_tokens=1024):
        return self._reply


def test_enrich_event_happy_path():
    event = Event(id="e1", text="The blender stopped working after three days. Very disappointed.")
    enriched = enrich_event(event, MockEnrichmentModel())
    assert enriched.id == "e1"
    assert enriched.model == "mock"
    assert enriched.enrichment.sentiment == "negative"
    assert enriched.enrichment.category == "product_quality"
    assert enriched.latency_ms >= 0
    # round-trips to JSON (this is what gets written to events.out)
    assert json.loads(json.dumps(enriched.to_dict()))["enrichment"]["category"] == "product_quality"


def test_invalid_json_raises_enrichment_error():
    with pytest.raises(EnrichmentError):
        enrich_event(Event(id="e2", text="hi"), _CannedModel("this is not json"))


def test_json_fences_are_tolerated():
    reply = '```json\n{"sentiment":"positive","category":"praise","priority":"low","entities":[],"summary":"ok"}\n```'
    enriched = enrich_event(Event(id="e3", text="great"), _CannedModel(reply))
    assert enriched.enrichment.sentiment == "positive"


def test_invalid_sentiment_raises():
    reply = '{"sentiment":"ecstatic","category":"praise","priority":"low","entities":[],"summary":"x"}'
    with pytest.raises(EnrichmentError):
        enrich_event(Event(id="e4", text="great"), _CannedModel(reply))


def test_unknown_category_is_coerced_to_other():
    reply = '{"sentiment":"neutral","category":"warp_core","priority":"low","entities":[],"summary":"x"}'
    enriched = enrich_event(Event(id="e5", text="hmm"), _CannedModel(reply))
    assert enriched.enrichment.category == "other"
