import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from intelligence.router import router as intelligence_router


app = FastAPI()
app.include_router(intelligence_router, prefix="/api/intelligence", tags=["intelligence"])
client = TestClient(app)


@pytest.fixture(autouse=True)
def _disable_ai(monkeypatch):
    monkeypatch.setenv("STREAMETL_AI_PROVIDER", "disabled")


def _filter_pipeline():
    return {
        "nodes": [
            {
                "id": "src",
                "type": "kafkaTopic",
                "data": {"label": "raw_orders", "params": {"readMode": "stream"}},
            },
            {
                "id": "parse",
                "type": "sparkTransform",
                "data": {
                    "transformType": "ParseJSON",
                    "params": {"schema": "order_id STRING, amount DOUBLE"},
                },
            },
            {
                "id": "filter",
                "type": "sparkTransform",
                "data": {"transformType": "Filter", "params": {"condition": "amount > 100"}},
            },
            {
                "id": "sink",
                "type": "kafkaTopic",
                "data": {"label": "filtered_orders", "params": {}},
            },
        ],
        "edges": [
            {"source": "src", "target": "parse"},
            {"source": "parse", "target": "filter"},
            {"source": "filter", "target": "sink"},
        ],
    }


def test_router_prefix_is_not_duplicated():
    response = client.post("/api/intelligence/analyze", json=_filter_pipeline())
    assert response.status_code == 200


def test_valid_filter_pipeline_has_no_errors():
    response = client.post("/api/intelligence/analyze", json=_filter_pipeline())
    body = response.json()
    assert response.status_code == 200
    assert body["valid"] is True
    assert body["summary"]["errors"] == 0


def test_unknown_node_type_is_rejected_by_pydantic():
    payload = _filter_pipeline()
    payload["nodes"][1]["type"] = "MagicAIJoin"
    response = client.post("/api/intelligence/analyze", json=payload)
    assert response.status_code == 422


def test_cycle_is_rejected_before_semantic_analysis():
    payload = _filter_pipeline()
    payload["edges"].append({"source": "filter", "target": "parse"})
    response = client.post("/api/intelligence/analyze", json=payload)
    assert response.status_code == 422


def test_streaming_aggregation_without_window_warns():
    payload = {
        "nodes": [
            {"id": "src", "type": "kafkaTopic", "data": {"label": "orders", "params": {}}},
            {"id": "gb", "type": "sparkTransform", "data": {"transformType": "GroupBy", "params": {"columns": "user_id"}}},
            {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {"aggregations": [{"func": "sum", "col": "amount", "alias": "total"}]}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "totals", "params": {}}},
        ],
        "edges": [
            {"source": "src", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    response = client.post("/api/intelligence/analyze", json=payload)
    codes = {issue["code"] for issue in response.json()["issues"]}
    assert "UNBOUNDED_STREAMING_AGGREGATION" in codes


def test_incomplete_groupby_window_is_error():
    payload = {
        "nodes": [
            {"id": "src", "type": "kafkaTopic", "data": {"label": "orders", "params": {}}},
            {"id": "gb", "type": "sparkTransform", "data": {"transformType": "GroupBy", "params": {"columns": "user_id", "windowEnabled": True, "windowDuration": "10 minutes"}}},
            {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "totals", "params": {}}},
        ],
        "edges": [
            {"source": "src", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is False
    assert "INCOMPLETE_WINDOW_CONFIG" in {i["code"] for i in body["issues"]}


def test_stream_stream_join_reports_time_bound_risk():
    payload = {
        "nodes": [
            {"id": "left", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "right", "type": "kafkaTopic", "data": {"label": "events", "params": {"readMode": "stream"}}},
            {"id": "join", "type": "sparkTransform", "data": {"transformType": "Join", "params": {"joinKey": "user_id", "joinType": "inner"}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "joined", "params": {}}},
        ],
        "edges": [
            {"source": "left", "target": "join"},
            {"source": "right", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    codes = {i["code"] for i in body["issues"]}
    assert "STREAM_STREAM_JOIN_NO_WATERMARK" in codes
    assert "STREAM_STREAM_JOIN_NO_TIME_BOUND" in codes


def test_reference_topic_gets_static_candidate_info():
    payload = {
        "nodes": [
            {"id": "orders", "type": "kafkaTopic", "data": {"label": "raw_orders", "params": {"readMode": "stream"}}},
            {"id": "users", "type": "kafkaTopic", "data": {"label": "users", "params": {"readMode": "stream"}}},
            {"id": "join", "type": "sparkTransform", "data": {"transformType": "Join", "params": {"joinKey": "user_id"}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "enriched", "params": {}}},
        ],
        "edges": [
            {"source": "orders", "target": "join"},
            {"source": "users", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    info = [i for i in body["issues"] if i["code"] == "JOIN_STATIC_REFERENCE_CANDIDATE"]
    assert info
    assert info[0]["repair_action"]["action"] == "set_source_read_mode"


def test_batch_only_pipeline_to_current_sink_is_error():
    payload = {
        "nodes": [
            {"id": "ref", "type": "kafkaTopic", "data": {"label": "users", "params": {"readMode": "batch"}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "users_copy", "params": {}}},
        ],
        "edges": [{"source": "ref", "target": "sink"}],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is False
    assert "BATCH_TO_STREAMING_SINK" in {i["code"] for i in body["issues"]}


def test_repair_source_mode_is_explicit_and_revalidated():
    payload = {
        "pipeline": {
            "nodes": [
                {"id": "orders", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
                {"id": "users", "type": "kafkaTopic", "data": {"label": "users", "params": {"readMode": "stream"}}},
                {"id": "join", "type": "sparkTransform", "data": {"transformType": "Join", "params": {"joinKey": "user_id"}}},
                {"id": "sink", "type": "kafkaTopic", "data": {"label": "enriched", "params": {}}},
            ],
            "edges": [
                {"source": "orders", "target": "join"},
                {"source": "users", "target": "join"},
                {"source": "join", "target": "sink"},
            ],
        },
        "actions": [
            {"action": "set_source_read_mode", "node_id": "users", "params": {"readMode": "batch"}}
        ],
    }
    response = client.post("/api/intelligence/repair", json=payload)
    assert response.status_code == 200
    nodes = {n["id"]: n for n in response.json()["pipeline"]["nodes"]}
    assert nodes["users"]["data"]["params"]["readMode"] == "batch"


def test_repair_window_requires_explicit_event_time_fields():
    payload = {
        "pipeline": {
            "nodes": [
                {"id": "src", "type": "kafkaTopic", "data": {"label": "orders", "params": {}}},
                {"id": "gb", "type": "sparkTransform", "data": {"transformType": "GroupBy", "params": {"columns": "user_id"}}},
                {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {}}},
                {"id": "sink", "type": "kafkaTopic", "data": {"label": "totals", "params": {}}},
            ],
            "edges": [
                {"source": "src", "target": "gb"},
                {"source": "gb", "target": "agg"},
                {"source": "agg", "target": "sink"},
            ],
        },
        "actions": [{"action": "enable_groupby_window", "node_id": "gb", "params": {}}],
    }
    response = client.post("/api/intelligence/repair", json=payload)
    assert response.status_code == 422


def test_diagnose_unresolved_column_is_rule_based():
    response = client.post(
        "/api/intelligence/diagnose",
        json={"error_message": "AnalysisException: cannot resolve 'customer_id' given input columns: [id, amount]"},
    )
    diagnosis = response.json()["diagnosis"]
    assert response.status_code == 200
    assert diagnosis["category"] == "SCHEMA_COLUMN"
    assert diagnosis["source"] == "rule"


def test_null_pointer_diagnosis_does_not_overclaim_data_null():
    response = client.post(
        "/api/intelligence/diagnose",
        json={"error_message": "java.lang.NullPointerException at org.apache.spark.SomeClass"},
    )
    diagnosis = response.json()["diagnosis"]
    assert "insufficient" in diagnosis["explanation"].lower()


def test_unknown_diagnosis_works_without_ai():
    response = client.post(
        "/api/intelligence/diagnose",
        json={"error_message": "Some completely unknown Spark failure XYZ", "use_ai_if_unknown": True},
    )
    diagnosis = response.json()["diagnosis"]
    assert response.status_code == 200
    assert diagnosis["source"] == "fallback"


def test_generate_returns_503_when_ai_is_disabled():
    response = client.post(
        "/api/intelligence/generate",
        json={"prompt": "Read raw_orders and filter amount above 100"},
    )
    assert response.status_code == 503


def test_broadcast_on_streaming_join_side_is_error():
    payload = {
        "nodes": [
            {"id": "left", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "right", "type": "kafkaTopic", "data": {"label": "events", "params": {"readMode": "stream"}}},
            {"id": "join", "type": "sparkTransform", "data": {"transformType": "Join", "params": {"joinKey": "id", "broadcast": True}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "joined", "params": {}}},
        ],
        "edges": [
            {"source": "left", "target": "join"},
            {"source": "right", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is False
    assert "BROADCAST_STREAMING_SIDE" in {i["code"] for i in body["issues"]}


def test_parquet_after_aggregation_flags_current_generator_limitation():
    payload = {
        "nodes": [
            {"id": "src", "type": "kafkaTopic", "data": {"label": "orders", "params": {}}},
            {"id": "gb", "type": "sparkTransform", "data": {"transformType": "GroupBy", "params": {"columns": "user_id"}}},
            {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {"aggregations": [{"func": "count", "col": "*"}]}}},
            {"id": "sink", "type": "parquetSink", "data": {"path": "/tmp/out", "params": {}}},
        ],
        "edges": [
            {"source": "src", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is False
    assert "PARQUET_AGGREGATION_UNSUPPORTED_BY_CURRENT_GENERATOR" in {i["code"] for i in body["issues"]}


def test_stream_stream_join_with_watermarks_and_time_bound_is_valid():
    payload = {
        "nodes": [
            {"id": "left", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "right", "type": "kafkaTopic", "data": {"label": "events", "params": {"readMode": "stream"}}},
            {"id": "left_parse", "type": "sparkTransform", "data": {"transformType": "ParseJSON", "params": {"schema": "user_id INT, event_time TIMESTAMP"}}},
            {"id": "right_parse", "type": "sparkTransform", "data": {"transformType": "ParseJSON", "params": {"schema": "user_id INT, event_time TIMESTAMP"}}},
            {
                "id": "join",
                "type": "sparkTransform",
                "data": {
                    "transformType": "Join",
                    "params": {
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
                    },
                },
            },
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "joined", "params": {}}},
        ],
        "edges": [
            {"source": "left", "target": "left_parse"},
            {"source": "left_parse", "target": "join"},
            {"source": "right", "target": "right_parse"},
            {"source": "right_parse", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    codes = {i["code"] for i in body["issues"]}
    assert body["valid"] is True
    assert "STREAM_STREAM_JOIN_NO_WATERMARK" not in codes
    assert "STREAM_STREAM_JOIN_NO_TIME_BOUND" not in codes
    assert "STREAM_STREAM_JOIN_TIME_BOUND_INVALID" not in codes


def test_join_without_parsejson_is_rejected_by_schema_validation():
    payload = {
        "nodes": [
            {"id": "left", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "right", "type": "kafkaTopic", "data": {"label": "events", "params": {"readMode": "stream"}}},
            {"id": "join", "type": "sparkTransform", "data": {"transformType": "Join", "params": {
                "joinKey": "user_id", "joinType": "inner", "enableWatermarks": True,
                "leftWatermarkCol": "event_time", "leftWatermarkDelay": "10 minutes",
                "rightWatermarkCol": "event_time", "rightWatermarkDelay": "10 minutes",
                "timeBoundEnabled": True, "timeRangeBefore": "10 minutes", "timeRangeAfter": "5 minutes"}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "joined", "params": {}}},
        ],
        "edges": [
            {"source": "left", "target": "join"},
            {"source": "right", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is False
    issues = [i for i in body["issues"] if i["code"] == "SCHEMA_COLUMN_UNAVAILABLE"]
    assert any("event_time" in issue["message"] for issue in issues)
    assert any("user_id" in issue["message"] for issue in issues)



def test_invalid_stream_stream_time_bound_is_error():
    payload = {
        "nodes": [
            {"id": "left", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "right", "type": "kafkaTopic", "data": {"label": "events", "params": {"readMode": "stream"}}},
            {
                "id": "join",
                "type": "sparkTransform",
                "data": {
                    "transformType": "Join",
                    "params": {
                        "joinKey": "user_id",
                        "enableWatermarks": True,
                        "leftWatermarkCol": "event_time",
                        "leftWatermarkDelay": "10 minutes",
                        "rightWatermarkCol": "event_time",
                        "rightWatermarkDelay": "10 minutes",
                        "timeBoundEnabled": True,
                        "timeRangeBefore": "soon",
                        "timeRangeAfter": "5 minutes",
                    },
                },
            },
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "joined", "params": {}}},
        ],
        "edges": [
            {"source": "left", "target": "join"},
            {"source": "right", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is False
    assert "STREAM_STREAM_JOIN_TIME_BOUND_INVALID" in {i["code"] for i in body["issues"]}


def test_repair_join_time_bound_is_explicit():
    payload = {
        "pipeline": {
            "nodes": [
                {"id": "left", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
                {"id": "right", "type": "kafkaTopic", "data": {"label": "events", "params": {"readMode": "stream"}}},
                {
                    "id": "join",
                    "type": "sparkTransform",
                    "data": {
                        "transformType": "Join",
                        "params": {
                            "joinKey": "user_id",
                            "enableWatermarks": True,
                            "leftWatermarkCol": "event_time",
                            "leftWatermarkDelay": "10 minutes",
                            "rightWatermarkCol": "event_time",
                            "rightWatermarkDelay": "10 minutes",
                        },
                    },
                },
                {"id": "sink", "type": "kafkaTopic", "data": {"label": "joined", "params": {}}},
            ],
            "edges": [
                {"source": "left", "target": "join"},
                {"source": "right", "target": "join"},
                {"source": "join", "target": "sink"},
            ],
        },
        "actions": [
            {
                "action": "set_join_time_bound",
                "node_id": "join",
                "params": {"timeRangeBefore": "10 minutes", "timeRangeAfter": "5 minutes"},
            }
        ],
    }
    response = client.post("/api/intelligence/repair", json=payload)
    assert response.status_code == 200
    join = next(n for n in response.json()["pipeline"]["nodes"] if n["id"] == "join")
    assert join["data"]["params"]["timeBoundEnabled"] is True


def test_windowed_watermarked_aggregation_can_target_parquet():
    payload = {
        "nodes": [
            {"id": "src", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "parse", "type": "sparkTransform", "data": {"transformType": "ParseJSON", "params": {"schema": "user_id INT, event_time TIMESTAMP, amount DOUBLE"}}},
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
            {"source": "src", "target": "parse"},
            {"source": "parse", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is True
    assert "PARQUET_AGGREGATION_UNSUPPORTED_BY_CURRENT_GENERATOR" not in {i["code"] for i in body["issues"]}


def test_windowed_aggregation_without_parsejson_is_rejected():
    payload = {
        "nodes": [
            {"id": "src", "type": "kafkaTopic", "data": {"label": "orders", "params": {"readMode": "stream"}}},
            {"id": "gb", "type": "sparkTransform", "data": {"transformType": "GroupBy", "params": {
                "columns": "user_id", "windowEnabled": True, "windowDuration": "10 minutes",
                "timestampCol": "event_time", "watermarkDelay": "10 minutes"}}},
            {"id": "agg", "type": "sparkTransform", "data": {"transformType": "Aggregate", "params": {
                "aggregations": [{"func": "sum", "col": "amount", "alias": "total"}]}}},
            {"id": "sink", "type": "parquetSink", "data": {"path": "/tmp/out", "params": {}}},
        ],
        "edges": [
            {"source": "src", "target": "gb"},
            {"source": "gb", "target": "agg"},
            {"source": "agg", "target": "sink"},
        ],
    }
    body = client.post("/api/intelligence/analyze", json=payload).json()
    assert body["valid"] is False
    issues = [i for i in body["issues"] if i["code"] == "SCHEMA_COLUMN_UNAVAILABLE"]
    assert any("user_id" in issue["message"] for issue in issues)
    assert any("event_time" in issue["message"] for issue in issues)
    assert any("amount" in issue["message"] for issue in issues)
