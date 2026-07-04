"""Deterministic, offline "model" for event enrichment.

Real enrichment is done by an LLM that reads a message and returns structured
JSON (sentiment, category, priority, entities, summary). To keep the pipeline
runnable with **zero keys**, this mock produces the *same shape* of JSON using
transparent, rule-based keyword scoring and regex entity extraction.

It's not as nuanced as a real model — that's what
``LLM_PROVIDER=openai|anthropic|ollama`` is for — but it exercises the entire
produce → enrich → route path end to end and keeps every test deterministic.

The contract with :mod:`enrich.prompts` is a single marker: the message text to
classify follows a line ``Message:`` in the user turn. The real providers get
the same prompt and are instructed to return only JSON, so the parsing in
:mod:`enrich.enrichment` is identical regardless of provider.
"""

from __future__ import annotations

import json
import re

from .base import ChatModel, Message

_MESSAGE = re.compile(r"Message:\n(.*)\Z", re.DOTALL)
_SENTENCE = re.compile(r"(?<=[.!?])\s+")

# --- sentiment lexicon (lowercased, word-boundary matched) -------------------
_NEGATIVE = {
    "broke", "broken", "terrible", "worst", "awful", "poor", "horrible", "angry",
    "disappointed", "disappointing", "refund", "damaged", "defective", "late",
    "delay", "delayed", "missing", "wrong", "unacceptable", "frustrated", "hate",
    "useless", "slow", "cancel", "cancelled", "overcharged", "unauthorized",
    "scam", "never", "cracked", "faulty", "stuck", "waited", "waiting",
}
_NEGATIVE_PHRASES = (
    "not working", "doesn't work", "does not work", "won't turn on", "stopped working",
    "no response", "still waiting", "charged twice", "keeps crashing", "not happy",
)
_POSITIVE = {
    "love", "loved", "great", "excellent", "amazing", "perfect", "thanks", "thank",
    "happy", "awesome", "fast", "works", "best", "recommend", "fantastic",
    "wonderful", "smooth", "flawless", "brilliant", "pleased", "delighted",
}
_URGENT = (
    "urgent", "asap", "immediately", "right now", "unauthorized", "charged twice",
    "escalate", "refund", "stopped working", "still waiting", "cancel my", "unacceptable",
)

# --- category lexicon: keyword -> category ------------------------------------
# Scanned in order; on a tie the earlier category wins, so product condition
# issues outrank logistics when a message mentions both (e.g. "arrived broken").
_CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "billing": ("refund", "charge", "charged", "invoice", "payment", "bill", "billed",
                "price", "overcharged", "subscription fee", "coupon", "discount"),
    "product_quality": ("broke", "broken", "defective", "damaged", "quality", "cracked",
                         "faulty", "stopped working", "malfunction", "scratch"),
    "shipping": ("ship", "shipping", "shipped", "deliver", "delivery", "delivered",
                 "tracking", "package", "parcel", "arrive", "arrived", "courier", "order"),
    "technical_support": ("login", "log in", "password", "app", "error", "bug", "crash",
                          "crashing", "website", "install", "update", "connect", "wifi", "sync"),
    "account": ("account", "profile", "cancel", "subscription", "unsubscribe", "email address",
                "sign in", "sign up", "reset"),
    "praise": ("love", "great", "excellent", "amazing", "awesome", "recommend", "thank",
               "fantastic", "wonderful", "perfect"),
}

# --- entity extraction -------------------------------------------------------
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_MONEY = re.compile(r"\$\d+(?:\.\d{1,2})?")
_ORDER = re.compile(r"(?:order|ord|tracking|invoice|ref)\s*#?\s*([A-Za-z0-9][A-Za-z0-9-]{2,})", re.I)
_HASHNUM = re.compile(r"#\d{3,}")
# Proper-noun runs: 2–4 TitleCase words (skips sentence-initial single words).
_PROPER = re.compile(r"\b[A-Z][a-zA-Z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+){1,3}\b")
# Leading words that are only capitalised because they start a sentence.
_STOPWORD_LEADERS = {
    "The", "A", "An", "This", "That", "These", "Those", "My", "Your", "Our",
    "His", "Her", "Their", "It", "I", "We", "You", "He", "She", "They",
    "Please", "Thanks", "Thank", "Hi", "Hello", "Do", "Where", "When", "Why", "How",
}


class MockEnrichmentModel(ChatModel):
    name = "mock"

    def complete(self, messages: list[Message], *, temperature: float = 0.0, max_tokens: int = 1024) -> str:
        user = next((m.content for m in reversed(messages) if m.role == "user"), "")
        match = _MESSAGE.search(user)
        text = (match.group(1) if match else user).strip()

        sentiment, score = _sentiment(text)
        category = _category(text, sentiment)
        priority = _priority(text, sentiment)
        entities = _entities(text)
        summary = _summary(text)

        # Real providers return a JSON string; the mock does too, so the parsing
        # and validation downstream is exercised identically.
        return json.dumps(
            {
                "sentiment": sentiment,
                "category": category,
                "priority": priority,
                "entities": entities,
                "summary": summary,
                "sentiment_score": score,
            }
        )


def _words(text: str) -> list[str]:
    return re.findall(r"[a-z']+", text.lower())


def _sentiment(text: str) -> tuple[str, int]:
    low = text.lower()
    words = set(_words(text))
    score = len(words & _POSITIVE) - len(words & _NEGATIVE)
    score -= sum(1 for p in _NEGATIVE_PHRASES if p in low)
    if score > 0:
        return "positive", score
    if score < 0:
        return "negative", score
    return "neutral", 0


def _category(text: str, sentiment: str) -> str:
    low = text.lower()
    tokens = set(_words(text))
    best, best_hits = "other", 0
    for category, keywords in _CATEGORY_KEYWORDS.items():
        # Single-word keywords match whole tokens (so "broke" != "broken" and
        # "arrive" != "arrived"); multi-word keywords match as substrings.
        hits = sum(1 for kw in keywords if (kw in low if " " in kw else kw in tokens))
        if hits > best_hits:
            best, best_hits = category, hits
    if best_hits == 0:
        return "praise" if sentiment == "positive" else "other"
    # A clearly positive message about a product reads as praise, not a complaint.
    if sentiment == "positive" and best == "product_quality":
        return "praise"
    return best


def _priority(text: str, sentiment: str) -> str:
    low = text.lower()
    urgent = any(kw in low for kw in _URGENT)
    if sentiment == "negative" and urgent:
        return "high"
    if sentiment == "negative":
        return "medium"
    return "low"


def _entities(text: str) -> list[str]:
    found: list[str] = []
    for pattern in (_MONEY, _EMAIL, _HASHNUM):
        found.extend(pattern.findall(text))
    found.extend(m.strip() for m in _ORDER.findall(text))
    for phrase in _PROPER.findall(text):
        words = phrase.split()
        if words and words[0] in _STOPWORD_LEADERS:
            words = words[1:]
        if len(words) >= 1 and words[0][:1].isupper():
            found.append(" ".join(words))
    # Trim trailing punctuation, preserve first-seen order, drop duplicates, cap.
    seen: set[str] = set()
    unique: list[str] = []
    for item in found:
        item = item.strip().rstrip(".,;:!?")
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            unique.append(item)
    return unique[:8]


def _summary(text: str, limit: int = 140) -> str:
    text = " ".join(text.split())
    if not text:
        return ""
    first = _SENTENCE.split(text)[0]
    return first if len(first) <= limit else first[: limit - 1].rstrip() + "…"
