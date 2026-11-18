"""Offline regression tests for StreamETL's optional AI integration.

Copy to backend/intelligence/tests/test_ai_regression.py.
No real API calls, credentials, Spark, or Kafka are required.
"""
import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from intelligence import providers
from intelligence.pipeline_assistant import PipelineAssistant
from intelligence.providers import (
    AIProvider,
    DisabledProvider,
    OllamaProvider,
    OpenAIProvider,
    ProviderDisabledError,
    ProviderError,
    _extract_openai_output_text,
    _parse_json_object,
    get_ai_provider,
)

ROOT = Path(__file__).resolve().parents[3]


class FakeProvider(AIProvider):
    name = 'fake'
    model = 'offline-test'

    def __init__(self, result):
        self.result = result
        self.calls = []

    def complete_json(self, system_prompt, user_prompt):
        self.calls.append((system_prompt, user_prompt))
        return copy.deepcopy(self.result)


@pytest.fixture
def valid_dag():
    return json.loads((ROOT / 'examples' / 'filter_orders.json').read_text(encoding='utf-8'))


@pytest.mark.parametrize('raw', ['{"nodes":[],"edges":[]}', '```json\n{"nodes":[],"edges":[]}\n```', 'Explanation: {"nodes":[],"edges":[]}'])
def test_json_object_parsing_supported_wrappers(raw):
    assert _parse_json_object(raw) == {'nodes': [], 'edges': []}


@pytest.mark.parametrize('raw', ['[]', 123, 'not valid json'])
def test_json_object_parsing_rejects_invalid_data(raw):
    with pytest.raises(ValueError):
        _parse_json_object(raw)


def test_openai_output_text_field():
    assert _extract_openai_output_text({'output_text': '{"ok":true}'}) == '{"ok":true}'


def test_openai_output_text_chunks():
    response = {'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': '{"ok":'}, {'type': 'output_text', 'text': 'true}'}]}]}
    assert _extract_openai_output_text(response) == '{"ok":true}'


def test_openai_missing_output_rejected():
    with pytest.raises(ProviderError, match='did not contain text'):
        _extract_openai_output_text({'output': []})


@pytest.mark.parametrize('value', ['disabled', 'none', 'off', ''])
def test_disabled_provider_selector(monkeypatch, value):
    monkeypatch.setenv('STREAMETL_AI_PROVIDER', value)
    assert isinstance(get_ai_provider(), DisabledProvider)


def test_disabled_provider_cannot_generate():
    with pytest.raises(ProviderDisabledError):
        DisabledProvider().complete_json('system', 'user')


def test_unknown_provider_rejected(monkeypatch):
    monkeypatch.setenv('STREAMETL_AI_PROVIDER', 'unsupported-test')
    with pytest.raises(ProviderError, match='Unsupported'):
        get_ai_provider()


def test_missing_openai_key_rejected(monkeypatch):
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    with pytest.raises(ProviderError, match='OPENAI_API_KEY'):
        OpenAIProvider()


def test_ollama_request_mocked(monkeypatch):
    monkeypatch.setenv('STREAMETL_AI_TIMEOUT_SECONDS', '12')
    fake_response = Mock()
    fake_response.json.return_value = {'message': {'content': '{"nodes":[],"edges":[]}'}}
    mock_post = Mock(return_value=fake_response)
    monkeypatch.setattr(providers.requests, 'post', mock_post)
    result = OllamaProvider().complete_json('system', 'user')
    assert result == {'nodes': [], 'edges': []}
    assert mock_post.call_args.kwargs['timeout'] == 12
    assert mock_post.call_args.kwargs['json']['options']['temperature'] == 0


def test_ollama_bad_json_is_wrapped(monkeypatch):
    fake_response = Mock()
    fake_response.json.return_value = {'message': {'content': 'not json'}}
    monkeypatch.setattr(providers.requests, 'post', Mock(return_value=fake_response))
    with pytest.raises(ProviderError, match='Ollama request failed'):
        OllamaProvider().complete_json('system', 'user')


def test_openai_request_mocked_and_timeout(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'fake-key-for-test')
    monkeypatch.setenv('STREAMETL_AI_TIMEOUT_SECONDS', '13')
    fake_response = Mock()
    fake_response.json.return_value = {'output_text': '{"nodes":[],"edges":[]}'}
    mock_post = Mock(return_value=fake_response)
    monkeypatch.setattr(providers.requests, 'post', mock_post)
    result = OpenAIProvider().complete_json('system', 'user')
    assert result == {'nodes': [], 'edges': []}
    assert mock_post.call_args.kwargs['timeout'] == 13
    assert mock_post.call_args.kwargs['json']['store'] is False


def test_assistant_valid_dag_and_deterministic_layout(valid_dag):
    first = FakeProvider(valid_dag)
    dag1, report1, info1 = PipelineAssistant.generate_dag_from_prompt('filter completed orders', first)
    dag2, report2, info2 = PipelineAssistant.generate_dag_from_prompt('filter completed orders', FakeProvider(valid_dag))
    assert dag1.model_dump() == dag2.model_dump()
    assert len(first.calls) == 1
    assert info1.name == info2.name == 'fake'
    assert all(node.position is not None for node in dag1.nodes)
    assert all(edge.id for edge in dag1.edges)
    assert report1 is not None and report2 is not None


def test_assistant_invalid_dag_is_rejected(valid_dag):
    valid_dag['nodes'][0]['type'] = 'unrecognized-node-type'
    with pytest.raises(ValidationError):
        PipelineAssistant.generate_dag_from_prompt('invalid', FakeProvider(valid_dag))


def test_assistant_semantic_gate_reports_invalidity(valid_dag):
    # Preserve the native contract but introduce a semantic error (missing ParseJSON schema).
    for node in valid_dag['nodes']:
        if node['data'].get('transformType') == 'ParseJSON':
            node['data']['params'].pop('schema', None)
    _, report, _ = PipelineAssistant.generate_dag_from_prompt('invalid schema', FakeProvider(valid_dag))
    assert report is not None
    assert 'error' in str(report).lower()
