# Evaluation pack for StreamETL 1.2.0-rc1

This folder produces **software-level (L2) evidence** and publication-ready PNG figures without inventing user-productivity claims.

## Outputs

After running the Windows helper, the following are created:

- `results/pytest.xml` — raw pytest/JUnit evidence
- `results/coverage.xml` — application-code coverage
- `results/timing.csv` — semantic-analysis and deterministic code-generation timings
- `results/guardrails.csv` — accept/reject decisions for valid and invalid pipelines
- `results/summary.json` — compact numeric summary
- `figures/fig_evaluation_latency.png`
- `figures/fig_generated_loc.png`
- `figures/fig_guardrail_decisions.png`
- `figures/fig_coverage_by_module.png`

## Scientific interpretation

The timing figure measures local API/compiler overhead only. It does **not** establish faster Spark execution or human productivity. The guardrail figure checks deterministic decisions for the documented examples and two intentionally unsafe variants. L3 Kafka/Spark/Docker evidence must be collected separately.
