"""Prepare isolated L4-W01 from the frozen B/W01_run1 candidate.

Run from the project root in Windows CMD:
    set "PYTHONPATH=%CD%\\backend"
    .venv\\Scripts\\python.exe evaluation\\l4\\prepare_l4_w.py

This only prepares files. It does NOT start Docker, Spark or Kafka.
The StreamETL generator temporarily writes backend/pipeline_job.py;
its previous bytes are restored in all cases.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
from fastapi.testclient import TestClient
from main import app

src = ROOT / "artifacts" / "ai_benchmark_abc_v2" / "B" / "W01_run1.json"
l4 = ROOT / "evaluation" / "l4"
out = l4 / "W01"
shared = ROOT / "backend" / "pipeline_job.py"
if not src.is_file():
    raise SystemExit(f"Frozen W01 candidate missing: {src}")
if out.exists():
    raise SystemExit(f"REFUSE OVERWRITE: {out}")
for d in (l4 / "inputs", l4 / "outputs", l4 / "checkpoints", l4 / "logs"):
    if not d.is_dir():
        raise SystemExit(f"Create directory first: {d}")

raw = src.read_bytes()
original = json.loads(raw.decode("utf-8"))
dag = copy.deepcopy(original)
nodes, edges = dag["nodes"], dag["edges"]
if len(nodes) != 5 or len(edges) != 4:
    raise SystemExit("Unexpected W01 topology")
by_id = {n["id"]: n for n in nodes}
sources = [n for n in nodes if n["type"] == "kafkaTopic" and not any(e["target"] == n["id"] for e in edges)]
sinks = [n for n in nodes if n["type"] == "parquetSink"]
group = [n for n in nodes if n["type"] == "sparkTransform" and n["data"].get("transformType") == "GroupBy"]
aggs = [n for n in nodes if n["type"] == "sparkTransform" and n["data"].get("transformType") == "Aggregate"]
if len(sources) != 1 or len(sinks) != 1 or len(group) != 1 or len(aggs) != 1:
    raise SystemExit("Unexpected W01 node roles")
if sources[0]["data"].get("label") != "raw_orders":
    raise SystemExit("Unexpected W01 input")
p = group[0]["data"].get("params", {})
expected_p = {"windowEnabled": True, "windowDuration": "10 minutes", "windowSlide": "5 minutes", "timestampCol": "event_time", "watermarkDelay": "10 minutes", "columns": "user_id"}
if any(p.get(k) != v for k,v in expected_p.items()):
    raise SystemExit("Unexpected W01 window parameters")
if aggs[0]["data"].get("params", {}).get("aggregations") != [{"func":"sum","col":"amount","alias":"total_amount"}]:
    raise SystemExit("Unexpected W01 aggregate expression")

source_topic = "l4_w01_orders_in"
output_path = "/app/evaluation/l4/outputs/W01_parquet"
checkpoint = "/app/evaluation/l4/checkpoints/W01"
sources[0]["data"]["label"] = source_topic
sources[0]["data"].setdefault("params", {})["startingOffset"] = "earliest"
sources[0]["data"]["params"]["consumerGroupId"] = "l4-w01-20261114"
sinks[0]["data"]["path"] = output_path

prior = shared.read_bytes() if shared.is_file() else None
try:
    with TestClient(app) as c:
        response = c.post("/api/pipeline/generate", json=dag)
    if response.status_code != 200:
        raise RuntimeError(f"W01 generation HTTP {response.status_code}: {response.text[:1200]}")
    code = response.json()["code"]
finally:
    if prior is None:
        if shared.exists():
            shared.unlink()
    else:
        shared.write_bytes(prior)

old_cp = "/tmp/checkpoint_parquet_" + sinks[0]["id"]
if old_cp not in code:
    raise SystemExit(f"Unexpected generated checkpoint; expected {old_cp}")
code = code.replace(old_cp, checkpoint)
for must in [source_topic, output_path, checkpoint, "window('event_time', '10 minutes', '5 minutes')", "withWatermark('event_time', '10 minutes')", "sum('amount').alias('total_amount')"]:
    if must not in code:
        raise SystemExit(f"Expected generated expression absent: {must}")
compile(code, "L4_W01_pipeline.py", "exec")

# Three-stage event-time experiment. Stage 2 advances watermark; stage 3
# ensures an additional micro-batch occurs after watermark advancement.
# Stage 2/3 use another user ID so expected results for user 11 are isolated.
stage1 = [
    {"order_id":"L4W-01","user_id":11,"amount":100.0,"status":"COMPLETED","event_time":"2026-11-14T00:01:00"},
    {"order_id":"L4W-02","user_id":11,"amount":20.0,"status":"COMPLETED","event_time":"2026-11-14T00:06:00"},
    {"order_id":"L4W-03","user_id":11,"amount":40.0,"status":"COMPLETED","event_time":"2026-11-14T00:11:00"},
]
stage2 = [{"order_id":"L4W-ADVANCE-1","user_id":99,"amount":1.0,"status":"COMPLETED","event_time":"2026-11-14T00:41:00"}]
stage3 = [{"order_id":"L4W-ADVANCE-2","user_id":99,"amount":2.0,"status":"COMPLETED","event_time":"2026-11-14T01:05:00"}]
expected_user11 = [
    {"user_id":11,"window_start":"2026-11-14T00:00:00Z","window_end":"2026-11-14T00:10:00Z","total_amount":120.0},
    {"user_id":11,"window_start":"2026-11-14T00:05:00Z","window_end":"2026-11-14T00:15:00Z","total_amount":60.0},
    {"user_id":11,"window_start":"2026-11-14T00:10:00Z","window_end":"2026-11-14T00:20:00Z","total_amount":40.0},
]

def jsonl(items):
    return "".join(json.dumps(v, separators=(",", ":")) + "\n" for v in items)

# Only write after all checks have passed.
out.mkdir()
(out / "source_candidate.json").write_bytes(raw)
(out / "isolated_dag.json").write_text(json.dumps(dag, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
(out / "L4_W01_pipeline.py").write_text(code + "\n", encoding="utf-8")
for stage, data in enumerate([stage1,stage2,stage3],1):
    (out / f"stage{stage}.jsonl").write_text(jsonl(data), encoding="utf-8")
(out / "expected_user11.json").write_text(json.dumps(expected_user11, indent=2) + "\n", encoding="utf-8")
manifest = {
 "scenario":"L4-W01", "condition":"B", "candidate":"W01_run1", "run_executed":False,
 "candidate_sha256":hashlib.sha256(raw).hexdigest(),
 "generated_code_sha256": hashlib.sha256((code+"\n").encode("utf-8")).hexdigest(),
 "source_topic":source_topic, "output_path":output_path, "checkpoint_path":checkpoint,
 "stages":[len(stage1),len(stage2),len(stage3)], "expected_user11":expected_user11,
 "changes":["isolate input topic", "startingOffsets changed latest -> earliest for controlled reproducibility", "isolated consumer group", "isolate parquet output", "isolate checkpoint"],
 "caution":"Event-time batches must be sent separately, after preceding batch is committed. Do not claim baseline exact agreement from this isolated runtime trial.",
 "note":"No Kafka/Spark runtime executed; backend/pipeline_job.py restored."
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
print("L4-W01 PREPARATION OK — no Docker, Kafka or Spark commands executed")
print("Directory:", out)
print("Original candidate SHA256:", manifest["candidate_sha256"])
print("Reference aggregates for user_id=11:", expected_user11)
print("Shared backend/pipeline_job.py was restored")
