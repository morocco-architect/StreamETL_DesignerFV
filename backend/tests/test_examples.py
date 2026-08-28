import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("STREAMETL_AI_PROVIDER", "disabled")

from main import app  # noqa: E402


client = TestClient(app)
EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


@pytest.mark.parametrize(
    "filename",
    [
        "filter_orders.json",
        "windowed_sales_to_parquet.json",
        "bounded_stream_join.json",
    ],
)
def test_documented_example_is_semantically_valid_and_compiles(filename):
    payload = json.loads((EXAMPLES / filename).read_text(encoding="utf-8"))

    analysis = client.post("/api/intelligence/analyze", json=payload)
    assert analysis.status_code == 200, analysis.text
    assert analysis.json()["valid"] is True, analysis.text

    generation = client.post("/api/pipeline/generate", json=payload)
    assert generation.status_code == 200, generation.text
    code = generation.json()["code"]
    compile(code, f"generated_{filename}.py", "exec")
    assert generation.json()["validation"]["valid"] is True
