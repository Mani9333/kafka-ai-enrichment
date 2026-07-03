"""Minimal, provider-agnostic chat interface (text in, text out).

Kept tiny so the offline mock, a local Ollama model, and hosted APIs
(OpenAI / Anthropic) are interchangeable — swapping providers is pure config.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Message:
    role: str  # system | user | assistant
    content: str


class ChatModel(ABC):
    name: str = "chat"

    @abstractmethod
    def complete(
        self,
        messages: list[Message],
        *,
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> str:
        raise NotImplementedError
