# StreamETL Designer — Intelligence-Assisted RC

**Release candidate:** `1.2.0-rc1`  
**GitHub-ready package:** cleaned of virtual environments, `node_modules`, generated jobs, caches, and nested Git repositories.

StreamETL is a self-hosted low-code environment for designing native Kafka–Spark Structured Streaming DAGs, validating them, generating inspectable PySpark, and executing the generated job on a Docker-based Spark/Kafka stack.

This release candidate merges the corrected StreamETL Core with the **Pipeline Intelligence** layer. The architectural invariant is:

```text
optional AI → native StreamETL DAG → deterministic validation
            → deterministic PySpark generation → Spark/Kafka
```

The LLM is never allowed to emit executable PySpark that bypasses the compiler.

## What changed in 1.2.0-rc1

### Pipeline Intelligence is now part of the real backend

`backend/intelligence/` uses the same native node vocabulary as the React Flow application:

- `kafkaTopic`
- `sparkTransform`
- `parquetSink`
- transforms: `ParseJSON`, `Filter`, `GroupBy`, `Aggregate`, `Join`

It exposes:

```text
POST /api/intelligence/generate   natural language → native DAG (optional AI)
POST /api/intelligence/analyze    deterministic semantic analysis
POST /api/intelligence/repair     explicit deterministic repairs
POST /api/intelligence/diagnose   rule-first Spark error diagnosis
```

The frontend now includes a **Pipeline Intelligence** panel for AI-assisted design, deterministic analysis, visible semantic issues, explicit repair actions when available, and automatic rule-first diagnosis when a Spark job crashes.

### Core compiler fixes

- DAG/Pydantic preflight validation runs before PySpark generation.
- Cycles, invalid arity, unsupported node contracts and semantic errors are rejected before code generation.
- The previous invalid `Parquet + Aggregate → outputMode('complete')` path has been removed.
  - Parquet/file output is always `append`.
  - non-windowed streaming aggregations are rejected for Parquet;
  - windowed + watermarked aggregations can reach a Parquet sink in finalized Append semantics.
- Unbounded stream–stream joins are rejected by the safe compiler profile.
  - both streams require watermarks;
  - an explicit event-time range (`timeRangeBefore`, `timeRangeAfter`) is required;
  - the generated join contains the temporal condition.
- Aggregation function names are constrained to the functions exposed by the UI.
- Kafka topic names are validated against Kafka-safe characters/length.
- User-controlled strings are rendered as Python literals rather than interpolated as executable code.
- Generated Python identifiers are normalized independently from user labels.

### Reproducibility / evidence

- Backend CI workflow: `.github/workflows/ci.yml`
- local validation helper: `scripts/validate_release.sh`
- JUnit + coverage artifacts are produced by CI/local validation
- backend tests now cover the Intelligence API **and** deterministic compiler behavior

See [`RELEASE_CANDIDATE_REPORT.md`](./RELEASE_CANDIDATE_REPORT.md) for the evidence boundaries and [`docs/REPRODUCIBILITY.md`](./docs/REPRODUCIBILITY.md) for the L1–L4 validation protocol.

## Repository layout

```text
StreamETL/
├── .github/workflows/ci.yml
├── .env.example
├── README.md
├── RELEASE_CANDIDATE_REPORT.md
├── CHANGELOG.md
├── examples/                          # native DAGs for validation/integration
├── docs/
│   └── REPRODUCIBILITY.md
├── deploy/
│   └── docker-compose.yml
├── backend/
│   ├── main.py                        # FastAPI + deterministic compiler/runtime API
│   ├── config.py
│   ├── database.py
│   ├── models.py
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   ├── intelligence/
│   │   ├── schemas.py
│   │   ├── semantic_advisor.py
│   │   ├── repair.py
│   │   ├── pipeline_assistant.py
│   │   ├── error_diagnosis.py
│   │   ├── providers.py
│   │   ├── prompts.py
│   │   ├── router.py
│   │   └── tests/
│   └── tests/
│       └── test_generator.py
├── frontend/
│   └── src/App.jsx
├── simulation/
└── scripts/
    └── validate_release.sh
```

## Run

### 1. Infrastructure

The repository contains the minimal Apache Spark 3.5.1 Docker build files needed by the tested stack. On Windows, the helper script builds the local Spark Python image when needed and starts all services:

```bat
scripts\windows\02_start_docker_stack.bat
```

Cross-platform manual equivalent:

```bash
docker build -t spark:3.5.1-scala2.12-java11-ubuntu deploy/spark-image/base
docker build -t streametl-spark-python:3.5.1 deploy/spark-image/python
cd deploy
docker compose up -d
```

### 2. Backend

Create the environment from the repository root so the Windows helper scripts can reuse it:

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r backend/requirements.txt
cd backend
uvicorn main:app --host 127.0.0.1 --port 8000
```

Windows shortcuts:

```bat
scripts\windows\03_run_backend.bat          REM AI disabled
scripts\windows\03_run_backend_ollama.bat   REM Ollama enabled
```

### 3. Frontend

```bash
cd frontend
npm ci
npm run dev
```

Production build on Windows:

```bat
scripts\windows\05_build_frontend.bat
```

### 4. Optional simulated Kafka data

```bash
cd ../simulation
python mock_producer.py
python mock_producer_users.py
```

## Pipeline Intelligence configuration

Core StreamETL remains usable without a generative model. With the default setting:

```bash
STREAMETL_AI_PROVIDER=disabled
```

`/analyze`, `/repair`, and rule-first `/diagnose` remain available. `/generate` returns an explicit 503 rather than pretending a demo graph was generated by AI.

### Ollama

```bash
export STREAMETL_AI_PROVIDER=ollama
export OLLAMA_HOST=http://localhost:11434
export STREAMETL_AI_MODEL=llama3.1:8b
```

### OpenAI

```bash
export STREAMETL_AI_PROVIDER=openai
export OPENAI_API_KEY=...
export STREAMETL_AI_MODEL=gpt-5.6-luna
```

The provider returns JSON; Pydantic + the deterministic semantic advisor remain authoritative after the model response.

## Validation

```bash
cd backend
pip install -r requirements-dev.txt
pytest -v
```

or from repository root:

```bash
./scripts/validate_release.sh
```

The script records environment metadata, JUnit output and coverage under `artifacts/local-validation/`. Native DAGs for integration tests are provided under `examples/`.

## Scope

### Implemented in this release candidate

- **Pipeline Intelligence**: design assistance, semantic analysis, explicit repairs, error diagnosis.

### Deliberately not implemented yet

- **Data Intelligence nodes** (`MLInferenceNode`, `AnomalyDetectionNode`, `LLMEnrichmentNode`).
- Spark-metrics-driven Optimization Advisor.
- claims that AI improves developer productivity or reliability in real users.

Those require a separate experimental protocol rather than feature prose alone.

## Scientific validation status

Automated tests establish selected software properties, not comparative scientific effectiveness. Before using productivity/reliability claims in a SoftwareX manuscript, evaluate the frozen release with a benchmark set and report at least:

- Exact DAG accuracy;
- Node/Edge precision, recall and F1;
- semantic-validity rate;
- generated pipeline execution-success rate;
- repair-success rate;
- invalid/unsafe pipeline acceptance rate;
- task-time or user outcomes only under a documented controlled protocol.

## Windows evaluation and paper figures

For a reproducible Windows evaluation, use a supported Python version (CI covers 3.10 and 3.12; the recorded local validation also exercised 3.13) and run from the repository root:

```bat
scripts\windows\01_setup_and_l2_evaluate.bat
```

This executes the full 34-test backend suite, writes JUnit/coverage evidence, benchmarks deterministic semantic analysis/code generation on the three documented DAGs, checks guardrail decisions, and creates publication-ready PNG figures under `evaluation/figures/`.

For runtime screenshots and L3 evidence, see `docs/SCREENSHOT_AND_L3_CHECKLIST.md`. Start the real stack with `scripts\windows\02_start_docker_stack.bat`, then backend and frontend in separate terminals with scripts `03` and `04`.


## Validation snapshot

See [`VALIDATION_REPORT.md`](./VALIDATION_REPORT.md) for the exact evidence boundary of this GitHub-ready package.
