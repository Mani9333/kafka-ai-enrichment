"""Select a chat model from ``LLM_PROVIDER`` (default: offline mock)."""

from __future__ import annotations

import os

from .base import ChatModel
from .mock import MockEnrichmentModel
from .providers import AnthropicChatModel, OpenAIChatModel, OllamaChatModel


def get_chat_model() -> ChatModel:
    provider = os.getenv("LLM_PROVIDER", "mock").strip().lower()
    if provider in ("", "mock", "offline"):
        return MockEnrichmentModel()
    if provider == "openai":
        return OpenAIChatModel()
    if provider == "anthropic":
        return AnthropicChatModel()
    if provider == "ollama":
        return OllamaChatModel()
    raise ValueError(f"Unknown LLM_PROVIDER={provider!r}. Use: mock, openai, anthropic, ollama.")
