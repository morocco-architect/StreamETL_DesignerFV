"""Paired StreamETL follow-up: B=constraint-aware prompt; C=explicit repair of frozen A.

Requires the v1 script alongside this file and the v1 saved baseline folder.
Does not overwrite the saved baseline. C is an oracle-informed, explicit repair experiment,
not an autonomous repair nor an independent LLM-generation score.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'backend'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
os.environ.setdefault('DATABASE_URL', 'sqlite:///:memory:')

from fastapi.testclient import TestClient
from intelligence.pipeline_assistant import _assign_deterministic_ui_layout
from intelligence.providers import OllamaProvider, OpenAIProvider
from intelligence.prompts import PIPELINE_BUILDER_SYSTEM_PROMPT
from intelligence.repair import PipelineRepairer
from intelligence.schemas import PipelineSuggestion, RepairAction, RepairRequest, StreamETLDAG
from intelligence.semantic_advisor import SemanticAdvisor
from main import app
from run_ai_benchmark import make_tasks, structural_exact

# Deliberately no task reference DAGs in the enhanced prompt.
EXTRA_SYSTEM = """
STREAMETL SAFE-GENERATION CHECKLIST (follow these rules in the JSON, not just in 'explanation'):
1. Create the exact source, transformation and sink TYPES requested by the user.
2. For Kafka stream-stream Join, both inputs need valid event-time columns. The Join node
   data.params MUST include: joinKey, joinType, enableWatermarks=true,
   leftWatermarkCol, leftWatermarkDelay, rightWatermarkCol, rightWatermarkDelay,
   timeBoundEnabled=true, timeRangeBefore, timeRangeAfter.
3. Populate all watermark and time-bound values EXACTLY from the user request;
   never guess omitted values. Do not merely describe them in 'explanation'.
4. If the task says parse both Kafka JSON payloads, include ParseJSON in EACH input branch.
5. If task says Kafka sink, use a kafkaTopic sink with the specified label;
   if task says Parquet, use parquetSink with its specified path.
6. For windowed aggregation, put windowEnabled, windowDuration, windowSlide,
   timestampCol, watermarkDelay into GroupBy.params; put aggregations in Aggregate.params.
7. Check that all edge endpoints exist, transforms have correct arity,
   and every required sink is connected before emitting a JSON object.
8. Return only native DAG JSON (nodes, edges, optional explanation); no Markdown.
""".strip()

# Task constraints were taken from the frozen user prompts, not inferred from reference DAGs.
EXPLICIT_JOIN_CONSTRAINTS = {
    'J01': {'leftWatermarkCol': 'event_time', 'leftWatermarkDelay': '10 minutes',
            'rightWatermarkCol': 'event_time', 'rightWatermarkDelay': '10 minutes',
            'timeRangeBefore': '10 minutes', 'timeRangeAfter': '5 minutes'},
    'J02': {'leftWatermarkCol': 'event_time', 'leftWatermarkDelay': '10 minutes',
            'rightWatermarkCol': 'event_time', 'rightWatermarkDelay': '10 minutes',
            'timeRangeBefore': '5 minutes', 'timeRangeAfter': '5 minutes'},
}

FIELDS = ['condition','task_id','repeat','provider','model','raw_response_parsed',
          'pydantic_valid','semantic_valid','codegen_success','python_compiles',
          'exact_dag_agreement','elapsed_seconds','error','issue_codes',
          'actions_applied','baseline_json_sha256']

def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def fresh_folder(path: Path):
    if path.exists() and any(path.iterdir()):
        raise RuntimeError(f'Refusing to overwrite a nonempty output folder: {path}')
    path.mkdir(parents=True, exist_ok=True)

def base_record(condition, task, repeat, provider, model):
    r = {key: '' for key in FIELDS}
    r.update(condition=condition, task_id=task['id'], repeat=repeat,
             provider=provider, model=model, raw_response_parsed=0,
             pydantic_valid=0, semantic_valid=0, codegen_success=0,
             python_compiles=0, exact_dag_agreement=0)
    return r

def evaluate(candidate, task, row, client, output, tag):
    suggestion = PipelineSuggestion.model_validate(candidate)
    row['pydantic_valid'] = 1
    suggestion = _assign_deterministic_ui_layout(suggestion)
    report = SemanticAdvisor.analyze(suggestion)
    row['semantic_valid'] = int(report.valid)
    row['issue_codes'] = ';'.join(sorted({i.code for i in report.issues}))
    dag = suggestion.model_dump(mode='json', exclude_none=True)
    row['exact_dag_agreement'] = int(structural_exact(task['reference'], dag))
    (output / f'{tag}.json').write_text(json.dumps(dag, ensure_ascii=False, indent=2), encoding='utf-8')
    # The existing generation API writes backend/pipeline_job.py: restore it afterwards.
    job = ROOT / 'backend' / 'pipeline_job.py'
    existed = job.exists()
    original = job.read_bytes() if existed else None
    try:
        if report.valid:
            response = client.post('/api/pipeline/generate', json=dag)
            try:
                payload = response.json()
            except Exception:
                payload = {}
            if response.status_code == 200 and isinstance(payload.get('code'), str):
                row['codegen_success'] = 1
                compile(payload['code'], tag + '.py', 'exec')
                row['python_compiles'] = 1
            else:
                row['error'] = f'codegen HTTP {response.status_code}: {response.text[:250]}'
    finally:
        if existed:
            job.write_bytes(original)
        elif job.exists():
            job.unlink()
    return row

def repair_baseline(task, original, row):
    dag = StreamETLDAG.model_validate(original)
    report = SemanticAdvisor.analyze(dag)
    codes = {issue.code for issue in report.issues}
    actions = []
    joins = [n for n in dag.nodes if n.type.value == 'sparkTransform' and
             n.data.transformType and n.data.transformType.value == 'Join']
    if task['id'] in EXPLICIT_JOIN_CONSTRAINTS and len(joins) == 1:
        p = EXPLICIT_JOIN_CONSTRAINTS[task['id']]
        join_id = joins[0].id
        if 'STREAM_STREAM_JOIN_NO_WATERMARK' in codes:
            actions.append(RepairAction(action='set_join_watermarks', node_id=join_id,
                params={key:p[key] for key in ('leftWatermarkCol','leftWatermarkDelay','rightWatermarkCol','rightWatermarkDelay')}))
        if 'STREAM_STREAM_JOIN_NO_TIME_BOUND' in codes or 'STREAM_STREAM_JOIN_TIME_BOUND_INVALID' in codes:
            actions.append(RepairAction(action='set_join_time_bound', node_id=join_id,
                params={key:p[key] for key in ('timeRangeBefore','timeRangeAfter')}))
    if not actions:
        row['actions_applied'] = 'none'
        return original
    response = PipelineRepairer.repair(RepairRequest(pipeline=dag, actions=actions))
    row['actions_applied'] = ';'.join(a.action.value for a in actions)
    return response.pipeline.model_dump(mode='json', exclude_none=True)

def write_summary(path, rows, label):
    with (path / 'results.csv').open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    keys = ['raw_response_parsed','pydantic_valid','semantic_valid','codegen_success',
            'python_compiles','exact_dag_agreement']
    summary = {'condition': label, 'runs': len(rows),
               'metrics': {k:{'count':sum(int(r[k]) for r in rows),
                              'rate':round(sum(int(r[k]) for r in rows)/len(rows),4)}
                           for k in keys},
               'caveat':'Deterministic tests: codegen and Python compile are not Kafka/Spark execution. C uses explicit task constraints (oracle-informed), not autonomous correction.'}
    (path/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary,indent=2))

def main(args):
    baseline = Path(args.baseline).resolve()
    if not (baseline / 'task_manifest.json').is_file():
        raise FileNotFoundError('Missing baseline task_manifest.json')
    manifest_file = json.loads((baseline / 'task_manifest.json').read_text(encoding='utf-8'))
    tasks = make_tasks()
    for frozen, current in zip(manifest_file, tasks):
        if frozen['id'] != current['id'] or frozen['prompt'] != current['prompt'] or frozen['reference'] != current['reference']:
            raise RuntimeError('Baseline manifest differs from current v1 tasks; do not compare these runs')
    if len(manifest_file) != len(tasks):
        raise RuntimeError('Baseline task count mismatch')
    with (baseline / 'results.csv').open(encoding='utf-8', newline='') as f:
        baseline_rows = list(csv.DictReader(f))
    if len(baseline_rows) != len(tasks)*args.repeats:
        raise RuntimeError(f'Baseline rows count {len(baseline_rows)} does not match {len(tasks)} tasks × {args.repeats} repeats')
    known = {(r['task_id'], int(r['repeat'])) for r in baseline_rows}
    expected = {(t['id'], i) for t in tasks for i in range(1,args.repeats+1)}
    if known != expected:
        raise RuntimeError('Baseline (task,repeat) pairs do not match current experiment')
    out = Path(args.output).resolve()
    if out == baseline or baseline in out.parents or out in baseline.parents:
        raise RuntimeError('Output must be separate from baseline, not its parent/child')
    fresh_folder(out)
    (out/'baseline_manifest_sha256.txt').write_text(sha256(baseline/'task_manifest.json')+'\n', encoding='utf-8')
    (out/'experiment_config.json').write_text(json.dumps({
        'provider':args.provider,'model':args.model,'repeats':args.repeats,
        'baseline':str(baseline),'baseline_manifest_sha256':sha256(baseline/'task_manifest.json'),
        'baseline_results_sha256':sha256(baseline/'results.csv'),
        'system_prompt_sha256':hashlib.sha256(PIPELINE_BUILDER_SYSTEM_PROMPT.encode()).hexdigest(),
        'enhanced_prompt_sha256':hashlib.sha256((PIPELINE_BUILDER_SYSTEM_PROMPT+'\n\n'+EXTRA_SYSTEM).encode()).hexdigest(),
        'C_repair':'Only predefined explicit join constraints via PipelineRepairer; requires frozen A DAG; no LLM call for C.'
    }, indent=2),encoding='utf-8')
    client = TestClient(app)
    os.environ['STREAMETL_AI_MODEL'] = args.model
    if args.provider == 'ollama':
        os.environ['OLLAMA_HOST'] = args.ollama_host
        provider = OllamaProvider()
    else:
        provider = OpenAIProvider()
    for condition in ('B','C'):
        target = out / condition
        fresh_folder(target)
        rows=[]
        for task in tasks:
            for repeat in range(1,args.repeats+1):
                row=base_record(condition,task,repeat,provider.name,provider.model)
                started=time.perf_counter()
                tag=f"{task['id']}_run{repeat}"
                try:
                    if condition == 'B':
                        raw=provider.complete_json(PIPELINE_BUILDER_SYSTEM_PROMPT+'\n\n'+EXTRA_SYSTEM,task['prompt'])
                        row['raw_response_parsed']=1
                        evaluate(raw, task,row,client,target,tag)
                    else:
                        prior=baseline / (tag+'.json')
                        row['baseline_json_sha256']=sha256(prior)
                        original=json.loads(prior.read_text(encoding='utf-8'))
                        row['raw_response_parsed']=1  # baseline file loaded, not a fresh provider call
                        dag=repair_baseline(task,original,row)
                        evaluate(dag,task,row,client,target,tag)
                except Exception as e:
                    row['error']=f'{type(e).__name__}: {str(e)[:350]}'
                row['elapsed_seconds']=round(time.perf_counter()-started,3)
                rows.append(row)
                print(f"{condition} {tag}: sem={row['semantic_valid']} exact={row['exact_dag_agreement']} compile={row['python_compiles']} actions={row['actions_applied']} error={row['error']}",flush=True)
        write_summary(target,rows,condition)
    print('NOTE: A baseline was not modified. C is oracle-informed explicit repair, not fully autonomous.')

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--provider',choices=['ollama','openai'],default='ollama')
    p.add_argument('--model',default='llama3.1:8b')
    p.add_argument('--ollama-host',default='http://localhost:11434')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--baseline',default=str(ROOT/'artifacts'/'ai_benchmark'))
    p.add_argument('--output',default=str(ROOT/'artifacts'/'ai_benchmark_abc_v2'))
    args=p.parse_args()
    if args.repeats<1:p.error('repeats must be positive')
    main(args)
