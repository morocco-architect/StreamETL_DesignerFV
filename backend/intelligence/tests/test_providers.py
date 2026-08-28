from __future__ import annotations

from intelligence.providers import OllamaProvider, OpenAIProvider


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_ollama_provider_uses_configured_timeout(monkeypatch):
    monkeypatch.setenv("OLLAMA_HOST", "http://localhost:11434")
    monkeypatch.setenv("STREAMETL_AI_MODEL", "llama3.1:8b")
    monkeypatch.setenv("STREAMETL_AI_TIMEOUT_SECONDS", "180")
    captured = {}

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured["timeout"] = kwargs["timeout"]
        return _FakeResponse({"message": {"content": '{"nodes": [], "edges": []}'}})

    monkeypatch.setattr("intelligence.providers.requests.post", fake_post)
    provider = OllamaProvider()
    result = provider.complete_json("system", "user")

    assert captured["url"] == "http://localhost:11434/api/chat"
    assert captured["timeout"] == 180.0
    assert result == {"nodes": [], "edges": []}


def test_openai_provider_uses_instance_timeout(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("STREAMETL_AI_MODEL", "test-model")
    monkeypatch.setenv("STREAMETL_AI_TIMEOUT_SECONDS", "90")
    captured = {}

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured["timeout"] = kwargs["timeout"]
        return _FakeResponse({"output_text": '{"nodes": [], "edges": []}'})

    monkeypatch.setattr("intelligence.providers.requests.post", fake_post)
    provider = OpenAIProvider()
    result = provider.complete_json("system", "user")

    assert captured["url"] == "https://api.openai.com/v1/responses"
    assert captured["timeout"] == 90.0
    assert result == {"nodes": [], "edges": []}
