# StreamETL 1.2.0-rc1 — Validation status

This GitHub-ready package separates automated software validation from external runtime validation.

## Automated validation (current packaged code)

- Python syntax compilation: PASS.
- Backend test suite: **34/34 passed** in the packaging environment using an import-only Kafka stub because the environment had no network access to install `kafka-python-ng`.
- Application-code coverage: **73%**.
- The stub is used only to satisfy imports; no Kafka behavior is simulated by these tests.
- New provider regression tests verify that Ollama and OpenAI HTTP calls use the configured instance timeout (`self.timeout`), preventing the earlier `NameError` regression.
- L2 evaluation artifacts for the three documented DAGs remain under `evaluation/results/` and `evaluation/figures/`.

## Frontend validation

The frontend production build was validated on Windows with Node 24 / npm 11 / Vite 8:

- 1884 modules transformed
- production build completed successfully
- generated `dist/` assets were intentionally excluded from GitHub because they are reproducible via `npm ci && npm run build`

## Runtime validation performed on the real stack

Environment exercised during the release-candidate validation:

- Docker Desktop / Windows
- Kafka 7.4.0 + ZooKeeper
- Spark 3.5.1 / Scala 2.12 / Java 11
- local Spark Python image `streametl-spark-python:3.5.1`
- PostgreSQL 15
- Ollama with `llama3.1:8b`

Confirmed end-to-end scenario:

1. Natural-language AI Build produced a native StreamETL DAG.
2. Deterministic Analyze reported 0 errors / 0 warnings for the validated filter pipeline.
3. Deterministic compiler generated PySpark.
4. The generated job executed on the Spark/Kafka Docker stack.
5. Controlled Kafka input was injected into `raw_orders`:
   - `(31, 50)`
   - `(32, 150)`
   - `(33, 220)`
   - `(34, 80)`
6. The Parquet output contained exactly the expected filtered rows:
   - `(32, 150.0)`
   - `(33, 220.0)`

The windowed-Parquet and bounded stream-stream join examples are covered by deterministic L2 tests and compilation tests. They should still be rerun as L3 runtime scenarios on the exact release commit before claiming complete external-stack coverage for all examples.

## Evidence boundary

Do not describe this release as scientifically proving developer-productivity gains or universal runtime correctness. The release validates the implemented software contracts and one documented end-to-end runtime scenario; broader comparative claims require the protocol described in `docs/REPRODUCIBILITY.md`.
