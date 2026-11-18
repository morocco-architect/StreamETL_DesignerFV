# Changelog

## 1.2.0 - scientific revision (2026-11-18)

### Added
- Schema-aware deterministic validation before PySpark generation.
- Schema analysis regression tests and semantic decision matrix.
- Archived A/B/C benchmark evidence: 54 DAGs retrospectively re-evaluated.
- L4 experimental evidence for F01 filtering, W01 windowed aggregation, and J01 streaming join.
- Project-level MIT LICENSE.
- Updated GitHub Actions workflow for Python 3.10/3.12 and frontend build.

### Validation snapshot
- Local Windows Python 3.12.10: 94/94 backend tests passed.
- Statement coverage: 82%; combined statement/branch coverage: 79%.
- Schema-aware acceptance: A 12/18, B 15/18, C 15/18.
- A/B/C results are retrospective validation of archived DAGs, not independent new model generations.
- GitHub Actions CI passed on 2026-11-18: backend Python 3.10/3.12 on Ubuntu and frontend Node.js 20 (commit 5c20696; run 44336358013).

### Limitations
- Automated tests do not establish real-user productivity improvements.
- Historical release-candidate validation and November experimental evidence remain distinct.



## 1.2.0-rc1 — release candidate

### Added
- Native Pipeline Intelligence integrated into the FastAPI backend.
- Deterministic `/generate`, `/analyze`, `/repair`, and `/diagnose` workflow.
- Provider abstraction for disabled, Ollama, and OpenAI modes.
- Path-aware semantic checks for streaming aggregation and joins.
- Explicit stream-stream event-time bounds and watermark validation.
- Rule-first Spark error diagnosis.
- Compiler/API tests plus CI JUnit and coverage evidence.
- Native DAG examples under `examples/`.
- Release reproducibility and evidence documentation.

### Changed
- Pipeline generation now performs Pydantic + semantic preflight before deterministic PySpark generation.
- Parquet sinks use Append mode only; non-finalizable streaming aggregations are rejected.
- Stream-stream joins require watermarks and explicit temporal bounds in the safe compiler profile.
- Generated identifiers and user-controlled string literals are sanitized/quoted before source generation.
- PostgreSQL startup failure no longer blocks unrelated Kafka/Spark API functionality.
- Docker service names/configuration are centralized.

### Removed
- Dummy fallback DAG generation when generative AI is disabled.
- Generated `backend/pipeline_job.py` from the source package; it is produced at runtime by the generation endpoint.

### Known validation gaps
- Real Docker/Kafka/Spark end-to-end execution is still required for L3 evidence.
- Real OpenAI/Ollama requests are not part of the local L2 validation.
- Frontend production build was not reproducible in the offline local workspace and is delegated to CI.
- No productivity/usability claim is supported until a controlled L4 experiment is completed.


## GitHub-ready packaging update — 2026-08-28

- Fixed AI provider request timeout use to consistently pass `self.timeout`.
- Added provider regression tests for Ollama/OpenAI timeout handling.
- Backend suite now contains 34 tests; current packaged-code run: 34/34 PASS, 73% coverage.
- Added clean Windows startup scripts for Docker, backend, Ollama backend, frontend, and production frontend build.
- Corrected Docker/PostgreSQL defaults to `admin:admin@localhost:5433/streametl`.
- Made the Vite backend proxy configurable with `VITE_BACKEND_TARGET` (default `127.0.0.1:8000`).
- Removed local `.venv`, `node_modules`, generated jobs, caches, and nested third-party Git metadata from the GitHub archive.
- Added minimal Apache Spark 3.5.1 Docker build files required to reconstruct the locally validated Spark Python image.
- Added `VALIDATION_REPORT.md` with explicit L2/L3 evidence boundaries.
