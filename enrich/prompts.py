"""The enrichment prompt.

One prompt drives every provider. It pins the output to a strict JSON schema and
enumerates the allowed label values so a real model's output lines up with the
deterministic mock's. The message text to classify always follows a ``Message:``
line — the single contract the mock relies on to find the text to score.
"""

from __future__ import annotations

from .llm import Message

CATEGORIES = (
    "billing",
    "shipping",
    "product_quality",
    "technical_support",
    "account",
    "praise",
    "other",
)

_SYSTEM = (
    "You are an event-enrichment engine for a customer-experience pipeline. "
    "Read one customer message and classify it. Respond with ONLY a compact JSON "
    "object (no prose, no markdown fences) with exactly these keys:\n"
    '  "sentiment": one of ["positive","neutral","negative"],\n'
    '  "category": one of ["' + '","'.join(CATEGORIES) + '"],\n'
    '  "priority": one of ["low","medium","high"],\n'
    '  "entities": array of short strings (order ids, products, amounts, emails),\n'
    '  "summary": a one-sentence summary (<= 140 chars).\n'
    "Pick the single best category. Use high priority only for urgent negative issues."
)


def build_enrichment_messages(text: str) -> list[Message]:
    user = f"Classify the customer message below.\n\nMessage:\n{text}"
    return [Message(role="system", content=_SYSTEM), Message(role="user", content=user)]
