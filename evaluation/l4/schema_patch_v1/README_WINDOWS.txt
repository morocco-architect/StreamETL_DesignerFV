StreamETL — first conservative schema-aware validator (v1 candidate)

This patch is NOT yet validated against the user's full local repository.
It never launches Spark or modifies Kafka. It installs a backup and refuses overwrite.

1) Extract these three .py files into evaluation\l4\schema_patch_v1\
2) From C:\StreamETL-1.2.0-rc1:
   set "PYTHONPATH=%CD%\backend"
   .venv\Scripts\python.exe -m py_compile evaluation\l4\schema_patch_v1\schema_analyzer.py
   .venv\Scripts\python.exe evaluation\l4\schema_patch_v1\install_schema_analysis.py
   .venv\Scripts\python.exe evaluation\l4\schema_patch_v1\check_schema_cases.py
   .venv\Scripts\python.exe -m pytest backend\intelligence\tests -q

LIMITS:
- Topological propagation with Kafka source technical columns and flat Spark SQL DDL ParseJSON schema.
- Rejects provably absent simple Filter, Join, GroupBy, Aggregate columns; timestamp type checks.
- Complex filters, SQL DDL with nested structures, Aggregate output and Join output are treated as unknown.
- Conservative UNKNOWN means a skipped check, not a runtime guarantee.
- Aggregate semantics and column aliases should be enhanced using the real generator code.
- The smoke checker skips missing W01 path; does not claim its test succeeded.

Rollback (only after tests, without erasing outputs):
   copy /Y backend\intelligence\semantic_advisor.py.before_schema_v1.bak backend\intelligence\semantic_advisor.py
   del backend\intelligence\schema_analyzer.py

Do NOT call /api/pipeline/generate until the original regression tests are green.
