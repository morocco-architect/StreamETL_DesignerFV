# Reproducible native DAG examples

These JSON files use the same native StreamETL node contract accepted by
`POST /api/pipeline/generate` and the deterministic Intelligence validator.

- `filter_orders.json` — ParseJSON → Filter → Kafka sink.
- `windowed_sales_to_parquet.json` — event-time window + watermark → Aggregate → Parquet Append sink.
- `bounded_stream_join.json` — two parsed Kafka streams with watermarks and an explicit event-time join bound.

Static validation can be performed by POSTing each document to:

```text
POST /api/intelligence/analyze
```

Code generation can then be tested with:

```text
POST /api/pipeline/generate
```

These examples are intended for reproducibility and integration testing. Passing static/API
validation is not a substitute for real Kafka/Spark execution evidence.
