from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


class ProviderError(RuntimeError):
    """Base error for optional AI provider failures."""


class ProviderDisabledError(ProviderError):
    pass


@dataclass(frozen=True)
class ProviderInfo:
    name: str
    model: Optional[str]
    enabled: bool


class AIProvider(ABC):
    name = "base"
    model: Optional[str] = None
    enabled = True

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(self.name, self.model, self.enabled)

    @abstractmethod
    def complete_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        raise NotImplementedError


class DisabledProvider(AIProvider):
    name = "disabled"
    model = None
    enabled = False

    def complete_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        raise ProviderDisabledError(
            "Generative AI is disabled. Set STREAMETL_AI_PROVIDER=ollama or openai."
        )


class OllamaProvider(AIProvider):
    name = "ollama"

    def __init__(self) -> None:
        self.host = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
        self.model = os.getenv("STREAMETL_AI_MODEL", "llama3.1:8b")
        self.timeout = float(os.getenv("STREAMETL_AI_TIMEOUT_SECONDS", "180"))

    def complete_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        try:
            response = requests.post(
    f"{self.host}/api/chat",
    json={
        "model": self.model,
        "stream": False,
        "format": "json",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {"temperature": 0},
    },
    timeout=self.timeout,
)
            response.raise_for_status()
            data = response.json()
            content = data.get("message", {}).get("content", "")
            return _parse_json_object(content)
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ProviderError(f"Ollama request failed: {exc}") from exc


class OpenAIProvider(AIProvider):
    """Minimal HTTP client for the OpenAI Responses API.

    The model is configurable to keep StreamETL independent of a single model
    release. The default favors a lower-cost current model suitable for JSON
    synthesis, and can be overridden with STREAMETL_AI_MODEL.
    """

    name = "openai"

    def __init__(self) -> None:
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ProviderError("OPENAI_API_KEY is required for the OpenAI provider")
        self.model = os.getenv("STREAMETL_AI_MODEL", "gpt-5.6-luna")
        self.timeout = float(os.getenv("STREAMETL_AI_TIMEOUT_SECONDS", "180"))

    def complete_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "input": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            # Responses API JSON mode: Pydantic remains the authoritative
            # native-DAG validator after the provider returns.
            "text": {"format": {"type": "json_object"}},
            "store": False,
        }
        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers=headers,
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            text = _extract_openai_output_text(data)
            return _parse_json_object(text)
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ProviderError(f"OpenAI request failed: {exc}") from exc


def _extract_openai_output_text(data: Dict[str, Any]) -> str:
    # Some client/proxy layers expose a convenience field; use it when present.
    if isinstance(data.get("output_text"), str):
        return data["output_text"]

    chunks: list[str] = []
    for item in data.get("output", []) or []:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            if isinstance(content, dict) and content.get("type") in {"output_text", "text"}:
                text = content.get("text")
                if isinstance(text, str):
                    chunks.append(text)
    if not chunks:
        raise ProviderError("OpenAI response did not contain text output")
    return "".join(chunks)


def _parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise ValueError("AI provider did not return a JSON object")

    text = value.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
        if text.lower().startswith("json"):
            text = text[4:].lstrip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Defensive extraction for providers that wrap the object in prose.
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(text[start : end + 1])

    if not isinstance(parsed, dict):
        raise ValueError("AI provider JSON root must be an object")
    return parsed


def get_ai_provider() -> AIProvider:
    provider_name = os.getenv("STREAMETL_AI_PROVIDER", "disabled").strip().lower()
    if provider_name in {"", "disabled", "none", "off"}:
        return DisabledProvider()
    if provider_name == "ollama":
        return OllamaProvider()
    if provider_name == "openai":
        return OpenAIProvider()
    raise ProviderError(
        f"Unsupported STREAMETL_AI_PROVIDER={provider_name!r}; expected disabled, ollama, or openai"
    )
