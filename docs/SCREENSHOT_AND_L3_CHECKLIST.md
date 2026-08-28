# Screenshot + L3 runtime checklist

Use the exact `1.2.0-rc1` tree and keep raw evidence for every screenshot.

## Recommended screenshots for the SoftwareX paper

1. **Main visual canvas + Pipeline Intelligence panel**
   - load `examples/filter_orders.json` or recreate it in the UI;
   - show the DAG and the analysis panel together;
   - avoid browser extensions/personal information in the capture.

2. **Windowed aggregation to Parquet**
   - use `examples/windowed_sales_to_parquet.json`;
   - show window duration, event-time column, watermark, Aggregate, and Parquet sink;
   - capture the deterministic analysis as valid.

3. **Bounded stream-stream join**
   - use `examples/bounded_stream_join.json`;
   - show both watermarks and before/after time bounds in the Join properties;
   - capture the generated PySpark section containing `withWatermark(...)` and the `INTERVAL` temporal predicate.

4. **Guardrail rejection**
   - remove the time bound from the join or remove the window from Aggregate→Parquet;
   - capture the semantic error and the blocked generation response.

5. **Runtime evidence**
   - Docker Desktop / `docker compose ps` with Kafka, Spark master/worker, PostgreSQL, Kafka UI and Zookeeper healthy/running;
   - Spark UI while a representative job is running;
   - Kafka UI showing input and output topics/messages;
   - Parquet output listing/preview for the windowed example.

## L3 commands/evidence to retain

Record in `artifacts/l3-validation/<date>/`:

- `docker compose ps`
- `docker compose logs --no-color`
- backend console log
- generated `pipeline_job.py`
- native DAG JSON
- producer input sample
- consumer/output sample
- Spark UI screenshot
- Kafka UI screenshot
- Parquet output preview

Do not report human-productivity gains from screenshots. Those are L4 claims and require a controlled protocol.
