"""Prepare, but DO NOT RUN, isolated L4-F from the real frozen B/F01_run1 DAG.

Usage from StreamETL project root (Windows CMD):
    set "PYTHONPATH=%CD%\backend"
    .venv\Scripts\python.exe evaluation\l4\prepare_l4_f.py

Writes only to evaluation/l4 and temporarily to backend/pipeline_job.py,
restored byte-for-byte in finally. Refuses to overwrite L4-F files.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
from fastapi.testclient import TestClient
from main import app

src = ROOT / "artifacts" / "ai_benchmark_abc_v2" / "B" / "F01_run1.json"
l4 = ROOT / "evaluation" / "l4"
output = l4 / "F01"
generated_shared = ROOT / "backend" / "pipeline_job.py"

if not src.is_file():
    raise SystemExit(f"Missing frozen DAG: {src}")
if output.exists():
    raise SystemExit(f"Refusing overwrite: {output} (select a new run directory manually)")
if not (l4 / "outputs").is_dir() or not (l4 / "checkpoints").is_dir():
    raise SystemExit("Create evaluation/l4/{outputs,checkpoints} first")

raw = src.read_bytes()
candidate = json.loads(raw.decode("utf-8"))
dag = copy.deepcopy(candidate)
nodes = dag["nodes"]
edges = dag["edges"]
if len(nodes) != 4 or len(edges) != 3:
    raise SystemExit("Unexpected frozen F01 DAG topology; aborting")
by_id = {n["id"]: n for n in nodes}
sources = [n for n in nodes if n["type"] == "kafkaTopic" and not any(e["target"] == n["id"] for e in edges)]
sinks = [n for n in nodes if n["type"] == "kafkaTopic" and any(e["target"] == n["id"] for e in edges)]
if len(sources) != 1 or len(sinks) != 1 or sources[0]["data"]["label"] != "raw_orders" or sinks[0]["data"]["label"] != "filtered_orders":
    raise SystemExit("Unexpected topic names or source/sink roles; aborting")
filters = [n for n in nodes if n["type"] == "sparkTransform" and n["data"].get("transformType") == "Filter"]
if len(filters) != 1 or filters[0]["data"].get("params", {}).get("condition") != "status = 'COMPLETED' AND amount > 100":
    raise SystemExit("Unexpected filter expression; aborting")
source_topic = "l4_f01_orders_in"
sink_topic = "l4_f01_orders_out"
sources[0]["data"]["label"] = source_topic
sources[0]["data"].setdefault("params", {})["startingOffset"] = "earliest"
sources[0]["data"]["params"]["consumerGroupId"] = "l4-f01-20261114"
sinks[0]["data"]["label"] = sink_topic

prior_bytes = generated_shared.read_bytes() if generated_shared.is_file() else None
try:
    with TestClient(app) as client:
        response = client.post("/api/pipeline/generate", json=dag)
    if response.status_code != 200:
        raise RuntimeError(f"Generation failed: {response.status_code}: {response.text[:1500]}")
    code = response.json()["code"]
finally:
    if prior_bytes is None:
        if generated_shared.exists():
            generated_shared.unlink()
    else:
        generated_shared.write_bytes(prior_bytes)

old_checkpoint = "/tmp/checkpoint_l4_f01_orders_out"
new_checkpoint = "/app/evaluation/l4/checkpoints/F01"
if old_checkpoint not in code:
    raise SystemExit("Unexpected generated checkpoint path; aborting")
code = code.replace(old_checkpoint, new_checkpoint)
if ".option('subscribe', 'l4_f01_orders_in')" not in code or ".option('topic', 'l4_f01_orders_out')" not in code:
    raise SystemExit("Unexpected generated source/sink topics; aborting")
if "status = 'COMPLETED' AND amount > 100" not in code:
    raise SystemExit("Generated code lost filter condition; aborting")
compile(code, "L4_F01_pipeline.py", "exec")

# Deterministic Kafka values. 3 expected, 3 rejected.
events = [
    {"order_id":"L4F-01","user_id":11,"amount":150.0,"status":"COMPLETED","event_time":"2026-11-14T00:00:00"},
    {"order_id":"L4F-02","user_id":12,"amount":100.0,"status":"COMPLETED","event_time":"2026-11-14T00:00:01"},
    {"order_id":"L4F-03","user_id":13,"amount":300.0,"status":"CANCELLED","event_time":"2026-11-14T00:00:02"},
    {"order_id":"L4F-04","user_id":14,"amount":100.01,"status":"COMPLETED","event_time":"2026-11-14T00:00:03"},
    {"order_id":"L4F-05","user_id":15,"amount":20.0,"status":"PENDING","event_time":"2026-11-14T00:00:04"},
    {"order_id":"L4F-06","user_id":16,"amount":999.0,"status":"COMPLETED","event_time":"2026-11-14T00:00:05"},
]
expected = [e["order_id"] for e in events if e["status"] == "COMPLETED" and e["amount"] > 100]

# Create output directory only after full static preparation.
output.mkdir(parents=False)
(output / "source_candidate.json").write_bytes(raw)
(output / "isolated_dag.json").write_text(json.dumps(dag, indent=2, ensure_ascii=False)+"\n", encoding="utf-8")
(output / "L4_F01_pipeline.py").write_text(code+"\n", encoding="utf-8")
(output / "input.jsonl").write_text("".join(json.dumps(e, separators=(",", ":"))+"\n" for e in events), encoding="utf-8")
(output / "expected_order_ids.json").write_text(json.dumps(expected, indent=2)+"\n", encoding="utf-8")
manifest = {
    "scenario":"L4-F01", "condition":"B", "candidate":"F01_run1", "run_executed":False,
    "candidate_sha256":hashlib.sha256(raw).hexdigest(),
    "isolated_code_sha256":hashlib.sha256((code+"\n").encode()).hexdigest(),
    "source_topic":source_topic,"sink_topic":sink_topic,
    "checkpoint":new_checkpoint,"input_count":len(events),"expected_count":len(expected),
    "expected_ids":expected,
    "changes":["input/output topic isolation","source consumer group isolation","checkpoint path isolation"],
    "note":"Generated via unmodified StreamETL endpoint; shared pipeline_job.py restored; no Docker/Spark/Kafka run performed."
}
(output / "manifest.json").write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")
print("L4-F PREPARATION OK (no Spark job started)")
print("Directory:", output)
print("Original SHA256:", manifest["candidate_sha256"])
print("Expected matches:", expected)
print("Shared backend/pipeline_job.py was restored.")
