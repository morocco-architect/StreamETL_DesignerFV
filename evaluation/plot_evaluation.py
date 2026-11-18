from __future__ import annotations

import csv
import statistics
import xml.etree.ElementTree as ET
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "evaluation" / "results"
FIGURES = ROOT / "evaluation" / "figures"
FIGURES.mkdir(parents=True, exist_ok=True)


def read_csv(name: str) -> list[dict]:
    with (RESULTS / name).open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def save_latency(rows: list[dict]) -> None:
    pipelines = sorted({r["pipeline"] for r in rows})
    analyze = [statistics.median(float(r["analyze_ms"]) for r in rows if r["pipeline"] == p) for p in pipelines]
    generate = [statistics.median(float(r["generate_ms"]) for r in rows if r["pipeline"] == p) for p in pipelines]
    x = range(len(pipelines))
    width = 0.35
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar([i - width / 2 for i in x], analyze, width, label="Semantic analysis")
    ax.bar([i + width / 2 for i in x], generate, width, label="PySpark generation")
    ax.set_ylabel("Median latency (ms)")
    ax.set_xticks(list(x), pipelines, rotation=15, ha="right")
    ax.set_title("StreamETL 1.2 deterministic processing latency")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIGURES / "fig_evaluation_latency.png", dpi=300)
    plt.close(fig)


def save_loc(rows: list[dict]) -> None:
    pipelines = sorted({r["pipeline"] for r in rows})
    values = [int(statistics.median(int(r["generated_loc"]) for r in rows if r["pipeline"] == p)) for p in pipelines]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(pipelines, values)
    ax.set_ylabel("Generated non-empty PySpark lines")
    ax.set_title("Generated code size for documented pipelines")
    ax.tick_params(axis="x", rotation=15)
    fig.tight_layout()
    fig.savefig(FIGURES / "fig_generated_loc.png", dpi=300)
    plt.close(fig)


def save_guardrails(rows: list[dict]) -> None:
    labels = [r["scenario"] for r in rows]
    values = [1 if r["correct_decision"].lower() == "true" else 0 for r in rows]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(labels, values)
    ax.set_ylim(0, 1.15)
    ax.set_ylabel("Correct deterministic decision (1=yes)")
    ax.set_title("Semantic guardrail decisions")
    ax.tick_params(axis="x", rotation=20)
    fig.tight_layout()
    fig.savefig(FIGURES / "fig_guardrail_decisions.png", dpi=300)
    plt.close(fig)


def save_coverage() -> None:
    coverage_path = RESULTS / "coverage.xml"
    if not coverage_path.exists():
        return
    root = ET.parse(coverage_path).getroot()
    entries: list[tuple[str, float]] = []
    for cls in root.findall(".//class"):
        filename = cls.attrib.get("filename", "")
        if not filename or "/tests/" in filename.replace("\\", "/") or filename.endswith("pipeline_job.py"):
            continue
        rate = float(cls.attrib.get("line-rate", "0")) * 100.0
        name = Path(filename).name
        entries.append((name, rate))
    # De-duplicate same basename by taking the largest observed rate.
    merged = {}
    for name, rate in entries:
        merged[name] = max(rate, merged.get(name, 0.0))
    items = sorted(merged.items(), key=lambda x: x[1])
    if not items:
        return
    fig, ax = plt.subplots(figsize=(9, max(5, len(items) * 0.35)))
    ax.barh([x[0] for x in items], [x[1] for x in items])
    ax.set_xlim(0, 100)
    ax.set_xlabel("Line coverage (%)")
    ax.set_title("Application-code coverage by module")
    fig.tight_layout()
    fig.savefig(FIGURES / "fig_coverage_by_module.png", dpi=300)
    plt.close(fig)


def main() -> None:
    timing = read_csv("timing.csv")
    guardrails = read_csv("guardrails.csv")
    save_latency(timing)
    save_loc(timing)
    save_guardrails(guardrails)
    save_coverage()
    print(f"Figures written to: {FIGURES}")


if __name__ == "__main__":
    main()
