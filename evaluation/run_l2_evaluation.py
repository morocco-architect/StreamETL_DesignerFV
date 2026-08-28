from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import time
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
EXAMPLES = ROOT / "examples"
RESULTS = ROOT / "evaluation" / "results"
RESULTS.mkdir(parents=True, exist_ok=True)

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("STREAMETL_AI_PROVIDER", "disabled")
sys.path.insert(0, str(BACKEND))

from fastapi.testclient import TestClient  # noqa: E402
from main import app  # noqa: E402

client = TestClient(app)


def load_example(name: str) -> dict:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


def timed_post(path: str, payload: dict) -> tuple[float, int, dict]:
    t0 = time.perf_counter()
    response = client.post(path, json=payload)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    try:
        body = response.json()
    except Exception:
        body = {"raw": response.text}
    return elapsed_ms, response.status_code, body


def generated_loc(code: str) -> int:
    return sum(1 for line in code.splitlines() if line.strip())


def benchmark_examples(iterations: int) -> list[dict]:
    rows: list[dict] = []
    mapping = {
        "filter_orders": "filter_orders.json",
        "windowed_parquet": "windowed_sales_to_parquet.json",
        "bounded_stream_join": "bounded_stream_join.json",
    }
    for label, filename in mapping.items():
        payload = load_example(filename)
        for i in range(iterations):
            analyze_ms, analyze_status, analyze_body = timed_post("/api/intelligence/analyze", payload)
            generate_ms, generate_status, generate_body = timed_post("/api/pipeline/generate", payload)
            rows.append(
                {
                    "pipeline": label,
                    "iteration": i + 1,
                    "analyze_ms": round(analyze_ms, 6),
                    "generate_ms": round(generate_ms, 6),
                    "analyze_status": analyze_status,
                    "generate_status": generate_status,
                    "semantic_valid": bool(analyze_body.get("valid")),
                    "generated_loc": generated_loc(generate_body.get("code", "")) if generate_status == 200 else 0,
                }
            )
    return rows


def make_unbounded_join() -> dict:
    payload = load_example("bounded_stream_join.json")
    for node in payload["nodes"]:
        if node.get("data", {}).get("transformType") == "Join":
            params = node["data"].setdefault("params", {})
            for key in [
                "enableWatermarks", "leftWatermarkCol", "leftWatermarkDelay",
                "rightWatermarkCol", "rightWatermarkDelay", "timeBoundEnabled",
                "timeRangeBefore", "timeRangeAfter",
            ]:
                params.pop(key, None)
    return payload


def make_nonwindowed_parquet() -> dict:
    payload = load_example("windowed_sales_to_parquet.json")
    for node in payload["nodes"]:
        if node.get("data", {}).get("transformType") == "GroupBy":
            params = node["data"].setdefault("params", {})
            for key in ["windowEnabled", "windowDuration", "slideDuration", "timestampCol", "watermarkDelay"]:
                params.pop(key, None)
    return payload


def guardrail_matrix() -> list[dict]:
    scenarios = [
        ("valid_filter", load_example("filter_orders.json"), True),
        ("valid_windowed_parquet", load_example("windowed_sales_to_parquet.json"), True),
        ("valid_bounded_join", load_example("bounded_stream_join.json"), True),
        ("invalid_unbounded_join", make_unbounded_join(), False),
        ("invalid_nonwindowed_parquet", make_nonwindowed_parquet(), False),
    ]
    rows: list[dict] = []
    for name, payload, expected_accept in scenarios:
        analyze_ms, analyze_status, analyze_body = timed_post("/api/intelligence/analyze", payload)
        generate_ms, generate_status, generate_body = timed_post("/api/pipeline/generate", payload)
        accepted = generate_status == 200
        rows.append(
            {
                "scenario": name,
                "expected_accept": expected_accept,
                "accepted": accepted,
                "correct_decision": accepted == expected_accept,
                "analyze_status": analyze_status,
                "generate_status": generate_status,
                "semantic_valid": bool(analyze_body.get("valid")),
                "analyze_ms": round(analyze_ms, 6),
                "generate_ms": round(generate_ms, 6),
                "detail": json.dumps(generate_body if generate_status != 200 else generate_body.get("validation", {}), ensure_ascii=False),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def summarize(timing_rows: list[dict], guardrail_rows: list[dict]) -> dict:
    summary = {
        "iterations_per_pipeline": len(timing_rows) // 3 if timing_rows else 0,
        "pipelines": {},
        "guardrails": {
            "correct": sum(bool(r["correct_decision"]) for r in guardrail_rows),
            "total": len(guardrail_rows),
        },
    }
    for pipeline in sorted({r["pipeline"] for r in timing_rows}):
        subset = [r for r in timing_rows if r["pipeline"] == pipeline]
        summary["pipelines"][pipeline] = {
            "median_analyze_ms": statistics.median(r["analyze_ms"] for r in subset),
            "median_generate_ms": statistics.median(r["generate_ms"] for r in subset),
            "generated_loc": int(statistics.median(r["generated_loc"] for r in subset)),
            "success_rate": sum(r["generate_status"] == 200 for r in subset) / len(subset),
        }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamETL 1.2 L2 evaluation harness")
    parser.add_argument("--iterations", type=int, default=50, help="iterations per valid example")
    args = parser.parse_args()
    if args.iterations < 1:
        raise SystemExit("--iterations must be >= 1")

    timing_rows = benchmark_examples(args.iterations)
    guardrail_rows = guardrail_matrix()
    write_csv(RESULTS / "timing.csv", timing_rows)
    write_csv(RESULTS / "guardrails.csv", guardrail_rows)
    summary = summarize(timing_rows, guardrail_rows)
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Results written to: {RESULTS}")


if __name__ == "__main__":
    main()
