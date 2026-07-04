"""Hosted / local chat model adapters (OpenAI, Anthropic, Ollama).

Each speaks the provider's HTTP API directly via ``httpx`` — no vendor SDKs —
and implements the same tiny :class:`ChatModel` contract. This is the exact same
model layer used across the portfolio's projects, so enrichment can target a
real model by only setting ``LLM_PROVIDER``.
"""

from __future__ import annotations

import os

import httpx

from .base import ChatModel, Message


class OpenAIChatModel(ChatModel):
    name = "openai"

    def __init__(self, model=None, base_url=None, api_key=None, timeout=60.0):
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.base_url = (base_url or os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")).rstrip("/")
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is not set (needed for LLM_PROVIDER=openai).")
        self.timeout = timeout

    def complete(self, messages, *, temperature=0.0, max_tokens=1024):
        payload = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        resp = httpx.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


class AnthropicChatModel(ChatModel):
    name = "anthropic"

    def __init__(self, model=None, api_key=None, timeout=60.0):
        self.model = model or os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set (needed for LLM_PROVIDER=anthropic).")
        self.timeout = timeout

    def complete(self, messages, *, temperature=0.0, max_tokens=1024):
        system = "\n\n".join(m.content for m in messages if m.role == "system")
        chat = [{"role": m.role, "content": m.content} for m in messages if m.role != "system"]
        payload = {"model": self.model, "messages": chat, "temperature": temperature, "max_tokens": max_tokens}
        if system:
            payload["system"] = system
        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": self.api_key, "anthropic-version": "2023-06-01"},
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return "".join(block.get("text", "") for block in resp.json().get("content", []))


class OllamaChatModel(ChatModel):
    name = "ollama"

    def __init__(self, model=None, host=None, timeout=120.0):
        self.model = model or os.getenv("OLLAMA_MODEL", "llama3.1")
        self.host = (host or os.getenv("OLLAMA_HOST", "http://localhost:11434")).rstrip("/")
        self.timeout = timeout

    def complete(self, messages, *, temperature=0.0, max_tokens=1024):
        payload = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "stream": False,
            "options": {"temperature": temperature, "num_predict": max_tokens},
        }
        resp = httpx.post(f"{self.host}/api/chat", json=payload, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()["message"]["content"]
