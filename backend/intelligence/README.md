# StreamETL Pipeline Intelligence

This package assists the **design and reliability of StreamETL pipelines**. It does not run ML models inside user data streams.

## Safety boundary

```text
free-text prompt
  → optional provider JSON
  → native Pydantic DAG
  → deterministic SemanticAdvisor
  → explicit user-visible repair (optional)
  → deterministic StreamETL compiler
  → PySpark
```

No provider can add an unknown node/transform type or bypass the deterministic compiler.

## Endpoints

- `POST /api/intelligence/generate`
- `POST /api/intelligence/analyze`
- `POST /api/intelligence/repair`
- `POST /api/intelligence/diagnose`

## Key deterministic rules

The advisor checks graph integrity/arity, required ParseJSON/Filter/GroupBy settings, streaming deduplication, branch-aware streaming lineage, broadcast misuse, stream-static candidates, bounded stream-stream joins, batch-to-streaming-sink mismatches, and Parquet aggregation compatibility.

## Generative providers

- `disabled` (default)
- `ollama`
- `openai`

Generative AI is optional. Semantic analysis and rule-first diagnosis do not require it.
