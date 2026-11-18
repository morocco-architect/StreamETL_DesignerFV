"""Prepare two isolated L4-J01 variants; never start Kafka or Spark.

Run from StreamETL project root, Windows CMD:
 set "PYTHONPATH=%CD%\\backend"
 .venv\\Scripts\\python.exe evaluation\\l4\\prepare_l4_j.py

Variant A: frozen oracle-repaired candidate with isolated Kafka topics and sink.
Variant B: same candidate plus two explicit JSON parsing steps.
Both generated via the unchanged StreamETL API; backend/pipeline_job.py restored.
"""
from __future__ import annotations
import copy, hashlib, json, sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'backend'))
from fastapi.testclient import TestClient
from main import app

source=ROOT/'artifacts/ai_benchmark_abc_v2/C/J01_run1.json'
l4=ROOT/'evaluation/l4'
out=l4/'J01'
shared=ROOT/'backend/pipeline_job.py'
if not source.is_file(): raise SystemExit(f'Missing candidate {source}')
if out.exists(): raise SystemExit(f'REFUSE OVERWRITE {out}')
for sub in ['inputs','outputs','checkpoints','logs']:
 if not (l4/sub).is_dir(): raise SystemExit(f'Missing {l4/sub}')
raw=source.read_bytes()
original=json.loads(raw)
nodes=original['nodes']; edges=original['edges']
if len(nodes)!=4 or len(edges)!=3: raise SystemExit('Unexpected J01 topology; no changes made')
lookup={n['id']:n for n in nodes}
join=[n for n in nodes if n['type']=='sparkTransform' and n['data'].get('transformType')=='Join']
sinks=[n for n in nodes if n['type']=='parquetSink']
sources=[n for n in nodes if n['type']=='kafkaTopic' and not any(e['target']==n['id'] for e in edges)]
if len(join)!=1 or len(sinks)!=1 or len(sources)!=2: raise SystemExit('Unexpected J01 roles')
if sorted(n['data']['label'] for n in sources)!=['events','orders']: raise SystemExit('Unexpected original topics')
params=join[0]['data']['params']
assert params.get('enableWatermarks') and params.get('timeBoundEnabled')
assert params['joinKey']=='user_id' and params['leftWatermarkCol']=='event_time' and params['rightWatermarkCol']=='event_time'
assert params['timeRangeBefore']=='10 minutes' and params['timeRangeAfter']=='5 minutes'

TOPICS={'orders':'l4_j01_orders_in','events':'l4_j01_events_in'}

def isolate(candidate, label):
 dag=copy.deepcopy(candidate)
 for n in dag['nodes']:
  if n['type']=='kafkaTopic' and n['data']['label'] in TOPICS:
   old=n['data']['label']; n['data']['label']=TOPICS[old]
   n['data'].setdefault('params',{})['startingOffset']='earliest'
   n['data']['params']['consumerGroupId']=f'l4-j01-{old}-20261114'
  if n['type']=='parquetSink':
   n['data']['path']=f'/app/evaluation/l4/outputs/J01_{label}_parquet'
 return dag

variant_a=isolate(original,'raw')
variant_b=copy.deepcopy(variant_a)
# Give the parsed branch its own sink (not the copied raw path).
for _node in variant_b['nodes']:
 if _node['type']=='parquetSink':
  _node['data']['path']='/app/evaluation/l4/outputs/J01_parsed_parquet'
nodeids={n['id'] for n in variant_b['nodes']}
edgeids={e.get('id') for e in variant_b['edges']}
join_id=join[0]['id']
for original_topic, schema in [
 ('orders','order_id STRING, user_id INT, amount DOUBLE, event_time TIMESTAMP'),
 ('events','event_id STRING, user_id INT, event_type STRING, event_time TIMESTAMP')]:
 src_id=next(n['id'] for n in variant_b['nodes'] if n['type']=='kafkaTopic' and n['data']['label']==TOPICS[original_topic])
 parse_id=f'l4_parse_{original_topic}'
 if parse_id in nodeids: raise SystemExit('Parse node ID collision')
 nodeids.add(parse_id)
 variant_b['nodes'].append({'id':parse_id,'type':'sparkTransform','data':{'transformType':'ParseJSON','params':{'schema':schema}}})
 original_edge=next((e for e in variant_b['edges'] if e['source']==src_id and e['target']==join_id),None)
 if original_edge is None: raise SystemExit(f'Missing {original_topic}->Join edge')
 original_edge['target']=parse_id
 new_id=f'l4_edge_{original_topic}_to_join'
 if new_id in edgeids: raise SystemExit('Edge ID collision')
 edgeids.add(new_id)
 variant_b['edges'].append({'id':new_id,'source':parse_id,'target':join_id})

prior=shared.read_bytes() if shared.is_file() else None
codes={}
try:
 with TestClient(app) as client:
  for name,dag in [('raw',variant_a),('parsed',variant_b)]:
   resp=client.post('/api/pipeline/generate',json=dag)
   if resp.status_code!=200: raise RuntimeError(f'{name} HTTP {resp.status_code}: {resp.text[:1500]}')
   code=resp.json()['code']
   default_cp='/tmp/checkpoint_parquet_'+sinks[0]['id']
   if default_cp not in code: raise RuntimeError(f'{name}: expected checkpoint {default_cp} not found')
   code=code.replace(default_cp,f'/app/evaluation/l4/checkpoints/J01_{name}')
   for topic in TOPICS.values():
    if topic not in code: raise RuntimeError(f'{name}: missing topic {topic}')
   if f'/app/evaluation/l4/outputs/J01_{name}_parquet' not in code: raise RuntimeError(f'{name}: output path missing')
   if '.withWatermark(' not in code: raise RuntimeError(f'{name}: generated watermarks missing')
   if name=='parsed' and code.count('from_json(')!=2: raise RuntimeError('Expected exactly two ParseJSON transforms')
   if name=='raw' and 'from_json(' in code: raise RuntimeError('Raw variant unexpectedly parses JSON')
   compile(code,f'J01_{name}_pipeline.py','exec')
   codes[name]=code
finally:
 if prior is None:
  if shared.exists(): shared.unlink()
 else: shared.write_bytes(prior)

out.mkdir()
(out/'source_candidate.json').write_bytes(raw)
for name,dag in [('raw',variant_a),('parsed',variant_b)]:
 (out/f'{name}_isolated_dag.json').write_text(json.dumps(dag,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
 (out/f'J01_{name}_pipeline.py').write_text(codes[name],encoding='utf-8')
manifest={
 'candidate':'C/J01_run1.json',
 'candidate_sha256':hashlib.sha256(raw).hexdigest(),
 'raw_variant':'only operational topic/path/checkpoint isolation; no ParseJSON',
 'parsed_variant':'same candidate plus explicit two ParseJSON nodes; NOT unmodified original',
 'source_topics':TOPICS,
 'parquet_paths':{k:f'/app/evaluation/l4/outputs/J01_{k}_parquet' for k in codes},
 'checkpoints':{k:f'/app/evaluation/l4/checkpoints/J01_{k}' for k in codes},
 'code_sha256':{k:hashlib.sha256(v.encode()).hexdigest() for k,v in codes.items()},
 'no_runtime_executed':True,
 'warnings':['No real Spark run, no claim of runtime acceptance','Raw variant likely fails unresolved user_id/event_time','Parsed variant must be tested separately with finite known input datasets']
}
(out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print('L4-J01 PREPARATION OK — no Kafka or Spark job started')
print('Directory:',out)
print('Original SHA256:',manifest['candidate_sha256'])
print('Generated variants:',', '.join(codes))
print('Shared backend/pipeline_job.py was restored')
