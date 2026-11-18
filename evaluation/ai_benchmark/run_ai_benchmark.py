"""StreamETL Build AI benchmark v1: frozen tasks, repeatable provider runs.
Run from repository root: .venv\\Scripts\\python.exe evaluation\\ai_benchmark\\run_ai_benchmark.py --provider ollama --model llama3.1:8b --repeats 3
This script never submits Spark jobs or writes to Kafka.
"""
from __future__ import annotations
import argparse
import copy
import csv
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'backend'))
os.environ.setdefault('DATABASE_URL', 'sqlite:///:memory:')

from fastapi.testclient import TestClient
from intelligence.pipeline_assistant import _assign_deterministic_ui_layout
from intelligence.prompts import PIPELINE_BUILDER_SYSTEM_PROMPT
from intelligence.semantic_advisor import SemanticAdvisor
from intelligence.providers import OllamaProvider, OpenAIProvider
from intelligence.schemas import PipelineSuggestion
from main import app

SPECS = [
    ('F01', 'filter_orders.json', 'Create a streaming Kafka-to-Kafka DAG. Read raw_orders; ParseJSON with schema "order_id STRING, user_id INT, amount DOUBLE, status STRING, event_time TIMESTAMP"; filter for status = \'COMPLETED\' AND amount > 100; write to filtered_orders. Use exactly ParseJSON and Filter transformations.'),
    ('F02', 'filter_orders.json', 'Make the same streaming order-filter pipeline: input Kafka raw_orders; parse schema "order_id STRING, user_id INT, amount DOUBLE, status STRING, event_time TIMESTAMP"; filter status = \'COMPLETED\' AND amount > 200; output Kafka filtered_orders.'),
    ('W01', 'windowed_sales_to_parquet.json', 'Design a streaming pipeline reading raw_orders, parsing "order_id STRING, user_id INT, amount DOUBLE, status STRING, event_time TIMESTAMP", GroupBy user_id with a 10 minute event-time window, 5 minute slide, 10 minute watermark on event_time, then Aggregate sum(amount) alias total_amount. Write append-finalized results to Parquet /tmp/parquet_windowed_sales.'),
    ('W02', 'windowed_sales_to_parquet.json', 'Create Kafka raw_orders -> ParseJSON schema "order_id STRING, user_id INT, amount DOUBLE, status STRING, event_time TIMESTAMP" -> GroupBy user_id with windowDuration="15 minutes", windowSlide="5 minutes", timestampCol="event_time", watermarkDelay="10 minutes" -> Aggregate sum amount as total_amount -> Parquet /tmp/parquet_windowed_sales.'),
    ('J01', 'bounded_stream_join.json', 'Join two Kafka streaming sources, orders and events. Parse orders with "order_id STRING, user_id INT, amount DOUBLE, event_time TIMESTAMP", events with "event_id STRING, user_id INT, event_type STRING, event_time TIMESTAMP". Inner join by user_id, enable watermarks on event_time with 10 minutes on both sides, bound event-time before=10 minutes after=5 minutes. Output Kafka joined_events.'),
    ('J02', 'bounded_stream_join.json', 'Construct two-stream Kafka join: orders and events parsed with schemas "order_id STRING, user_id INT, amount DOUBLE, event_time TIMESTAMP" and "event_id STRING, user_id INT, event_type STRING, event_time TIMESTAMP". Inner join on user_id, both event_time watermarks 10 minutes, explicit time range before="5 minutes", after="5 minutes"; Kafka sink joined_events.'),
]

def make_tasks():
    tasks = []
    for name, filename, prompt in SPECS:
        dag = json.loads((ROOT / 'examples' / filename).read_text(encoding='utf-8'))
        if name == 'F02':
            next(n for n in dag['nodes'] if n['data'].get('transformType') == 'Filter')['data']['params']['condition'] = "status = 'COMPLETED' AND amount > 200"
        if name == 'W02':
            next(n for n in dag['nodes'] if n['data'].get('transformType') == 'GroupBy')['data']['params']['windowDuration'] = '15 minutes'
        if name == 'J02':
            next(n for n in dag['nodes'] if n['data'].get('transformType') == 'Join')['data']['params']['timeRangeBefore'] = '5 minutes'
        tasks.append(dict(id=name, prompt=prompt, reference=dag, original=filename))
    return tasks

# Comparison ignores node ids, edges ids, UI layout and operational-only parameters.
# It retains all *specified* node parameters affecting the pipeline's requested semantics.
OPERATIONAL = {'consumerGroupId', 'startingOffset', 'acksPolicy'}

def canonical_node(node):
    d = node['data']
    p = {k: v for k,v in d.get('params', {}).items() if k not in OPERATIONAL}
    return json.dumps({'type': node['type'], 'label': d.get('label'),
        'transformType': d.get('transformType'), 'path': d.get('path'),
        'params': p}, sort_keys=True, ensure_ascii=False)

def structural_exact(reference, candidate):
    """Strict attributed graph isomorphism; stable under renaming node/edge IDs.
    Complexity manageable for these 4-6-node task fixtures.
    """
    rn, cn = reference['nodes'], candidate['nodes']
    if len(rn) != len(cn) or len(reference['edges']) != len(candidate['edges']):
        return False
    rids = [n['id'] for n in rn]
    cids = [n['id'] for n in cn]
    rcolors = [canonical_node(n) for n in rn]
    ccolors = [canonical_node(n) for n in cn]
    if sorted(rcolors) != sorted(ccolors):
        return False
    rset = {(e['source'],e['target']) for e in reference['edges']}
    cset = {(e['source'],e['target']) for e in candidate['edges']}
    buckets = [[j for j,c in enumerate(ccolors) if c == color] for color in rcolors]
    def check(i, used, mapping):
        if i == len(rn):
            return {(mapping[a], mapping[b]) for a,b in rset} == cset
        for j in buckets[i]:
            if j not in used:
                mapping[rids[i]] = cids[j]
                if check(i+1, used | {j}, mapping):
                    return True
                del mapping[rids[i]]
        return False
    return check(0, set(), {})

def run(args):
    os.environ['STREAMETL_AI_MODEL'] = args.model
    if args.provider == 'ollama':
        os.environ['OLLAMA_HOST'] = args.ollama_host
        provider = OllamaProvider()
    elif args.provider == 'openai':
        provider = OpenAIProvider()
    else:
        raise ValueError('provider must be ollama or openai')
    client = TestClient(app)
    tasks = make_tasks()
    dest = Path(args.output)
    dest.mkdir(parents=True, exist_ok=True)
    (dest/'task_manifest.json').write_text(json.dumps(tasks, indent=2, ensure_ascii=False), encoding='utf-8')
    rows = []
    fields = ['task_id','repeat','provider','model','raw_response_parsed','pydantic_valid','semantic_valid','codegen_success','python_compiles','exact_dag_agreement','elapsed_seconds','error','issue_codes']
    for task in tasks:
        for repeat in range(1,args.repeats+1):
            record = dict.fromkeys(fields,'')
            record.update(task_id=task['id'], repeat=repeat, provider=provider.name, model=provider.model,
                          raw_response_parsed=0,pydantic_valid=0,semantic_valid=0,codegen_success=0,python_compiles=0,exact_dag_agreement=0)
            started = time.perf_counter()
            try:
                # Provider call is real. The deterministic semantic gate stays local.
                raw = provider.complete_json(PIPELINE_BUILDER_SYSTEM_PROMPT, task['prompt'])
                record['raw_response_parsed'] = 1
                suggestion = PipelineSuggestion.model_validate(raw)
                record['pydantic_valid'] = 1
                suggestion = _assign_deterministic_ui_layout(suggestion)
                validation = SemanticAdvisor.analyze(suggestion)
                candidate = suggestion.model_dump(mode='json', exclude_none=True)
                record['semantic_valid'] = int(validation.valid)
                record['issue_codes'] = ';'.join(sorted({i.code for i in validation.issues}))
                record['exact_dag_agreement'] = int(structural_exact(task['reference'],candidate))
                (dest / f"{task['id']}_run{repeat}.json").write_text(json.dumps(candidate,indent=2,ensure_ascii=False),encoding='utf-8')
                if validation.valid:
                    resp = client.post('/api/pipeline/generate',json=candidate)
                    if resp.status_code == 200 and isinstance(resp.json().get('code'),str):
                        record['codegen_success'] = 1
                        try:
                            compile(resp.json()['code'],f"{task['id']}_run{repeat}.py",'exec')
                            record['python_compiles'] = 1
                        except (SyntaxError,ValueError) as exc:
                            record['error'] = f'generated Python compile failure: {exc}'
                    else:
                        record['error'] = f'generator HTTP {resp.status_code}: {resp.text[:350]}'
            except Exception as exc:
                # Separate provider failures and malformed output in raw log; do not fabricate validation.
                record['error'] = f'{type(exc).__name__}: {str(exc)[:350]}'
            record['elapsed_seconds'] = round(time.perf_counter()-started,3)
            rows.append(record)
            print(f"{task['id']} run {repeat}/{args.repeats}: semantics={record['semantic_valid']} exact={record['exact_dag_agreement']} codegen={record['codegen_success']} {record['error']}",flush=True)
    with (dest/'results.csv').open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    summary = {'provider':provider.name, 'model':provider.model,'tasks':len(tasks),'repeats':args.repeats,'runs':len(rows),
       'interpretation':'Code generation and compile() do not establish Spark runtime execution. Exact graph agreement is strict on declared semantic params, ignoring node IDs/UI layout/operational-only options.',
       'metrics':{k: {'count':sum(int(r[k]) for r in rows),'rate':round(sum(int(r[k]) for r in rows)/len(rows),4)} for k in ['raw_response_parsed','pydantic_valid','semantic_valid','codegen_success','python_compiles','exact_dag_agreement']}}
    (dest/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary,indent=2))

if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--provider',choices=['ollama','openai'],default='ollama')
    p.add_argument('--model',default='llama3.1:8b')
    p.add_argument('--ollama-host',default='http://localhost:11434')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default=str(ROOT/'artifacts'/'ai_benchmark'))
    a=p.parse_args()
    if a.repeats < 1: p.error('repeats must be >=1')
    run(a)
