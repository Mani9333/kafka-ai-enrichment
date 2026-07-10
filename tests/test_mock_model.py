"""The offline mock must be deterministic and return well-formed enrichment JSON."""

import json

from enrich.llm.mock import MockEnrichmentModel
from enrich.prompts import CATEGORIES, build_enrichment_messages


def classify(text: str) -> dict:
    model = MockEnrichmentModel()
    return json.loads(model.complete(build_enrichment_messages(text)))


def test_output_is_wellformed_json_with_required_keys():
    result = classify("The app keeps crashing when I log in on my Pixel 8.")
    assert set(result) >= {"sentiment", "category", "priority", "entities", "summary"}
    assert result["sentiment"] in {"positive", "neutral", "negative"}
    assert result["category"] in CATEGORIES
    assert result["priority"] in {"low", "medium", "high"}
    assert isinstance(result["entities"], list)


def test_negative_complaint_is_high_priority_product_quality():
    result = classify("My order #100582 arrived cracked and the screen is broken. I need a refund ASAP.")
    assert result["sentiment"] == "negative"
    assert result["category"] == "product_quality"
    assert result["priority"] == "high"
    assert "#100582" in result["entities"]


def test_positive_review_is_praise():
    result = classify("Absolutely love the Sonos Roam. Sound quality is amazing and setup was fast.")
    assert result["sentiment"] == "positive"
    assert result["category"] == "praise"
    assert "Sonos Roam" in result["entities"]


def test_billing_double_charge_extracts_amount():
    result = classify("I was charged twice for my subscription. Please refund the extra $14.99.")
    assert result["sentiment"] == "negative"
    assert result["category"] == "billing"
    assert "$14.99" in result["entities"]


def test_email_is_extracted():
    result = classify("Please cancel my account. Reach me at jane.doe@example.com.")
    assert "jane.doe@example.com" in result["entities"]


def test_deterministic():
    text = "Where is my package? Tracking says delivered but I never received order ORD-77213."
    assert classify(text) == classify(text)
