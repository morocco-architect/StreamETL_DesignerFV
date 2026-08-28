#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/artifacts/local-validation"
mkdir -p "$OUT"

{
  echo "StreamETL local validation"
  echo "UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Python: $(python --version 2>&1)"
  echo "Platform: $(python - <<'PY'
import platform
print(platform.platform())
PY
)"
  echo "Key Python packages:"
  python - <<'PY'
from importlib.metadata import PackageNotFoundError, version
for name in [
    "fastapi", "pydantic", "SQLAlchemy", "kafka-python-ng",
    "requests", "pytest", "httpx", "pytest-cov"
]:
    try:
        print(f"  {name}=={version(name)}")
    except PackageNotFoundError:
        print(f"  {name}=NOT_INSTALLED")
PY
  if command -v git >/dev/null 2>&1 && git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Commit: $(git -C "$ROOT" rev-parse HEAD)"
  else
    echo "Commit: unavailable (not a Git checkout)"
  fi
} | tee "$OUT/environment.txt"

python -m compileall -q "$ROOT/backend"

(
  cd "$ROOT/backend"
  DATABASE_URL="${DATABASE_URL:-sqlite:///:memory:}" \
  STREAMETL_AI_PROVIDER="${STREAMETL_AI_PROVIDER:-disabled}" \
  pytest -v \
    --junitxml="$OUT/pytest.xml" \
    --cov=. \
    --cov-report=xml:"$OUT/coverage.xml" \
    --cov-report=term-missing
) | tee "$OUT/pytest-output.txt"

echo "Validation artifacts written to $OUT"
