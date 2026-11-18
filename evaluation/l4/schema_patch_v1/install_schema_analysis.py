"""One-time safe installer. Run from StreamETL repository root on Windows.
Refuses to overwrite a pre-existing schema_analyzer.py and creates a .bak of semantic_advisor.py.
"""
from pathlib import Path
import shutil

root = Path.cwd()
backend = root / 'backend' / 'intelligence'
source = Path(__file__).with_name('schema_analyzer.py')
target = backend / 'schema_analyzer.py'
advisor = backend / 'semantic_advisor.py'
backup = backend / 'semantic_advisor.py.before_schema_v1.bak'
assert source.is_file() and advisor.is_file(), 'Run at project root; package files must be together.'
if target.exists() or backup.exists():
    raise SystemExit('REFUSED: target or backup already exists; no overwrite performed.')
s = advisor.read_text(encoding='utf-8')
anchor = '        cls._analyze_runtime_lineage(pipeline, graph, issues)\n'
if s.count(anchor) != 1:
    raise SystemExit('REFUSED: expected exact single integration anchor not found.')
if 'from .schema_analyzer import SchemaAnalyzer' in s:
    raise SystemExit('REFUSED: schema analyzer is already imported.')
replacement = anchor + '        from .schema_analyzer import SchemaAnalyzer\n        issues.extend(SchemaAnalyzer.analyze(pipeline))\n'
new = s.replace(anchor, replacement, 1)
# Safely compile syntax before writing.
compile(new, str(advisor), 'exec')
compile(source.read_text(encoding='utf-8'), str(target), 'exec')
shutil.copy2(advisor, backup)
try:
    shutil.copy2(source, target)
    advisor.write_text(new, encoding='utf-8')
except Exception:
    shutil.copy2(backup, advisor)
    if target.exists(): target.unlink()
    raise
print('SCHEMA_ANALYSIS_INSTALLED')
print('BACKUP =', backup)
print('MODULE =', target)
print('ADVISOR =', advisor)
