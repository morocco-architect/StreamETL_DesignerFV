import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Keep compiler/API tests independent of a running PostgreSQL service.
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("STREAMETL_AI_PROVIDER", "disabled")

from main import app  # noqa: E402


client = TestClient(app)


def _base_source(label="orders"):
    return {"id": "src", "type": "kafkaTopic", "data": {"label": label, "params": {"readMode": "stream"}}}


def _kafka_sink(label="out"):
    return {"id": "sink", "type": "kafkaTopic", "data": {"label": label, "params": {}}}


def test_simple_pipeline_generates_compilable_python(tmp_path):
    payload = {
        "nodes": [
            _base_source(),
            {"id": "parse", "type": "sparkTransform", "data": {"transformType": "ParseJSON", "params": {"schema": "id STRING, amount DOUBLE"}}},
            {"id": "filter", "type": "sparkTransform", "data": {"transformType": "Filter", "params": {"condition": "amount > 100"}}},
            _kafka_sink(),
        ],
        "edges": [
            {"source": "src", "target": "parse"},
            {"source": "parse", "target": "filter"},
            {"source": "filter", "target": "sink"},
        ],
    }
    response = client.post("/api/pipeline/generate", json=payload)
    assert response.status_code == 200
    code = response.json()["code"]
    compile(code, "generated_pipeline.py", "exec")
    assert ".filter('amount > 100')" in code
    assert response.json()["validation"]["valid"] is True


def test_invalid_kafka_topic_label_is_rejected_before_codegen():
    malicious = "orders'); __import__('os').system('echo injected'); #"
    payload = {
        "nodes": [
            _base_source(malicious),
            {"id": "parse", "type": "sparkTransform", "data": {"transformType": "ParseJSON", "params": {"schema": "id STRING"}}},
            _kafka_sink("safe-out"),
        ],
        "edges": [
            {"source": "src", "target": "parse"},
            {"source": "parse", "target": "sink"},
        ],
    }
    response = client.post("/api/pipeline/generate", json=payload)
    assert response.status_code == 422
    assert "Kafka topic names" in str(response.json())


def test_cycle_is_rejected_before_compilation():
    payload = {
        "nodes": [
            _base_source(),
            {"id": "a", "type": "sparkTransform", "data": {"transformType": "Filter", "params": {"condition": "id IS NOT NULL"}}},
            {"id": "b", "type": "sparkTransform", "data": {"transformType": "Filter", "params": {"condition": "id IS NOT NULL"}}},
            _kafka_sink(),
        ],
        "edges": [
            {"source": "src", "target": "a"},
            {"source": "a", "target": "b"},
            {"source": "b", "target": "a"},
            {"source": "b", "target": "sink"},
        ],
    }
    response = client.post("/api/pipeline/generate", json=payload)
    assert response.status_code == 422


def test_nonwindowed_aggregation_to_parquet_is_rejected():
    payload = {
        "nodes": [
            _base_source(),
            {"id": "gb", "type": "sparkTransform", "data": {"transformType": "GroupBy", "params": {"columns": "user_id"}}},
            {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {"aggregations": [{"func": "sum", "col": "amount", "alias": "total"}]}}},
            {"id": "sink", "type": "parquetSink", "data": {"path": "/tmp/out", "params": {}}},
        ],
        "edges": [
            {"source": "src", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    response = client.post("/api/pipeline/generate", json=payload)
    assert response.status_code == 400
    assert "PARQUET_AGGREGATION_UNSUPPORTED_BY_CURRENT_GENERATOR" in str(response.json())


def test_windowed_watermarked_aggregation_to_parquet_uses_append():
    payload = {
        "nodes": [
            _base_source(),
            {
                "id": "gb",
                "type": "sparkTransform",
                "data": {
                    "transformType": "GroupBy",
                    "params": {
                        "columns": "user_id",
                        "windowEnabled": True,
                        "windowDuration": "10 minutes",
                        "timestampCol": "event_time",
                        "watermarkDelay": "10 minutes",
                    },
                },
            },
            {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {"aggregations": [{"func": "sum", "col": "amount", "alias": "total"}]}}},
            {"id": "sink", "type": "parquetSink", "data": {"path": "/tmp/out", "params": {}}},
        ],
        "edges": [
            {"source": "src", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    response = client.post("/api/pipeline/generate", json=payload)
    assert response.status_code == 200
    code = response.json()["code"]
    assert ".format('parquet')" in code
    assert ".outputMode('append')" in code
    assert ".outputMode('complete')" not in code


def _stream_stream_payload(join_params):
    return {
        "nodes": [
            {"id": "left", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "right", "type": "kafkaTopic", "data": {"label": "events", "params": {"readMode": "stream"}}},
            {"id": "join", "type": "sparkTransform", "data": {"transformType": "Join", "params": join_params}},
            _kafka_sink("joined"),
        ],
        "edges": [
            {"source": "left", "target": "join"},
            {"source": "right", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }


def test_unbounded_stream_stream_join_is_rejected():
    response = client.post(
        "/api/pipeline/generate",
        json=_stream_stream_payload({"joinKey": "user_id", "joinType": "inner"}),
    )
    assert response.status_code == 400
    assert "STREAM_STREAM_JOIN_NO_TIME_BOUND" in str(response.json())


def test_bounded_stream_stream_join_generates_temporal_condition():
    params = {
        "joinKey": "user_id",
        "joinType": "inner",
        "enableWatermarks": True,
        "leftWatermarkCol": "event_time",
        "leftWatermarkDelay": "10 minutes",
        "rightWatermarkCol": "event_time",
        "rightWatermarkDelay": "10 minutes",
        "timeBoundEnabled": True,
        "timeRangeBefore": "10 minutes",
        "timeRangeAfter": "5 minutes",
    }
    response = client.post("/api/pipeline/generate", json=_stream_stream_payload(params))
    assert response.status_code == 200
    code = response.json()["code"]
    compile(code, "generated_pipeline.py", "exec")
    assert "INTERVAL 10 MINUTES" in code
    assert "INTERVAL 5 MINUTES" in code
    assert ".withWatermark('event_time', '10 minutes')" in code


def test_unsupported_aggregation_function_is_rejected():
    payload = {
        "nodes": [
            _base_source(),
            {"id": "gb", "type": "sparkTransform", "data": {"transformType": "GroupBy", "params": {"columns": "user_id"}}},
            {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {"aggregations": [{"func": "__import__", "col": "amount"}]}}},
            _kafka_sink(),
        ],
        "edges": [
            {"source": "src", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    response = client.post("/api/pipeline/generate", json=payload)
    assert response.status_code == 400
    assert "unsupported aggregation function" in str(response.json()).lower()
