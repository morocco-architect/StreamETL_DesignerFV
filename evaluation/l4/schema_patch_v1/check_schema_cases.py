"""Read-only regression smoke check; run from repository root.
Set PYTHONPATH=%CD%\backend (Windows CMD). No API generation or Spark runtime.
"""
from pathlib import Path
import json
from intelligence.schemas import StreamETLDAG
from intelligence.semantic_advisor import SemanticAdvisor

cases = [
    ('J01_RAW', Path('evaluation/l4/J01/raw_isolated_dag.json'), False),
    ('J01_PARSED', Path('evaluation/l4/J01/parsed_isolated_dag.json'), True),
    ('W01', Path('evaluation/l4/W01/isolated_dag.json'), True),
]
any_fail = False
for name, path, expected_valid in cases:
    if not path.exists():
        print(f'{name}: SKIPPED (not found: {path})')
        continue
    try:
        dag = StreamETLDAG.model_validate(json.loads(path.read_text(encoding='utf-8')))
        report = SemanticAdvisor.analyze(dag)
        relevant = [(i.code, i.severity.value, i.node_id, i.message) for i in report.issues if i.code.startswith('SCHEMA_')]
        print(name, 'VALID=', report.valid, 'SCHEMA_ISSUES=', relevant)
        if report.valid != expected_valid:
            any_fail = True
    except Exception as e:
        any_fail = True
        print(f'{name}: ERROR: {e}')
if any_fail:
    raise SystemExit(1)
