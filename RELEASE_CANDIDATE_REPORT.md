# StreamETL 1.2.0-rc1 — Release Candidate Verification Report

This report distinguishes **verified software facts** from **runtime evidence boundaries** and from claims that still require a broader experiment.

## Evidence levels

- **L1 — code inspection:** implementation is present in source.
- **L2 — automated test:** a deterministic automated test exercises the stated behavior.
- **L3 — integration/runtime:** behavior is demonstrated on the real external stack (Kafka/Spark/Docker/provider).
- **L4 — experimental evaluation:** comparative/research claims require a frozen experimental protocol.

## Verified in this GitHub-ready package

| Item | Level | Status |
|---|---:|---|
| native Intelligence schema (`kafkaTopic` / `sparkTransform` / `parquetSink`) | L1/L2 | PASS |
| `/generate`, `/analyze`, `/repair`, `/diagnose` routes | L1/L2 | PASS |
| graph-cycle rejection | L1/L2 | PASS |
| semantic preflight before deterministic generation | L1/L2 | PASS |
| non-windowed Aggregate → Parquet rejection | L1/L2 | PASS |
| windowed+watermarked Aggregate → Parquet compilation | L1/L2 | PASS |
| unbounded stream-stream join rejection | L1/L2 | PASS |
| bounded stream-stream temporal condition generation | L1/L2 | PASS |
| unsupported aggregation function rejection | L1/L2 | PASS |
| invalid Kafka topic label rejection | L1/L2 | PASS |
| rule-first Spark diagnosis for covered patterns | L1/L2 | PASS |
| provider-disabled `/generate` returns explicit 503 | L1/L2 | PASS |
| Ollama/OpenAI configured request timeout regression | L2 | PASS |
| frontend production build | L2/build | PASS on validated Windows environment |
| Ollama natural-language → native DAG | L3 | PASS for validated filter scenario |
| deterministic Analyze → generated PySpark | L3 | PASS for validated filter scenario |
| Kafka/Spark execution → Parquet output | L3 | PASS for validated filter scenario |

## Current automated test execution

The packaging environment had no network access to install `kafka-python-ng`, so an **import-only Kafka stub outside the repository** was used solely to satisfy imports for compiler/API tests. No Kafka behavior was simulated.

Current packaged-code result:

```text
34 passed
application-code coverage: 73%
```

The two added provider regression tests cover the configured HTTP timeout path for Ollama and OpenAI and prevent reintroduction of the previous `timeout`/`self.timeout` failure.

The L2 benchmark re-executed 50 iterations for each documented DAG. All three had a 1.0 deterministic analyze/generate success rate and all 5/5 guardrail decisions were correct. Raw values are in `evaluation/results/summary.json` and `evaluation/results/timing.csv`.

## Validated real-stack scenario

The release candidate was exercised on Windows with Docker Desktop, Kafka 7.4.0, ZooKeeper, PostgreSQL 15, Spark 3.5.1 / Scala 2.12 / Java 11, the local `streametl-spark-python:3.5.1` image, and Ollama `llama3.1:8b`.

For the filter pipeline, controlled Kafka input was:

```text
(31, 50)
(32, 150)
(33, 220)
(34, 80)
```

The deterministic filter was `amount > 100`. The Parquet verification returned exactly:

```text
32  150.0
33  220.0
```

This establishes one complete L3 path: natural-language AI Build → native DAG → deterministic analysis → deterministic PySpark → Spark/Kafka execution → expected Parquet output.

## Remaining L3 work before claiming every example is runtime-validated

The following examples are already L2-tested and compile successfully, but should still be rerun on the exact public release commit for full L3 coverage:

- windowed + watermarked aggregation → Parquet;
- bounded stream-stream join;
- representative runtime failure → rule-first diagnosis.

The OpenAI provider is unit-tested with mocked HTTP transport but was not validated here against a real external API key.

## Release metadata blocker

No project-level code license has been selected by the authors. A public GitHub repository can technically be uploaded without one, but reuse rights remain restricted. Add the intended `LICENSE` before a public SoftwareX software release.

## Evidence boundary

Do not claim universal runtime correctness, productivity gains, or controlled-user improvements from these tests. Those are L4 claims and require the experimental protocol described in `docs/REPRODUCIBILITY.md`.
