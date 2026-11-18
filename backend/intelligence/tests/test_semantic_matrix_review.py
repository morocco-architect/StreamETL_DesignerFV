"""Independent, deterministic semantic-regression matrix for StreamETL 1.2 RC1.

Run from project root (CMD):
    set "PYTHONPATH=%CD%\backend"
    python -m pytest backend/intelligence/tests/test_semantic_matrix_review.py -v

Only the current native schemas and SemanticAdvisor are tested: no Spark, Kafka or LLM.
Each row declares an expected acceptance decision and expected issue code BEFORE evaluation.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from intelligence.schemas import StreamETLDAG
from intelligence.semantic_advisor import SemanticAdvisor

EXAMPLES = Path(__file__).resolve().parents[3] / 'examples'


def load(name):
    return json.loads((EXAMPLES / name).read_text(encoding='utf-8'))


def node(d, node_id):
    return next(n for n in d['nodes'] if n['id'] == node_id)


def params(d, node_id):
    return node(d, node_id)['data'].setdefault('params', {})


def remove_edge(d, source, target):
    d['edges'] = [e for e in d['edges'] if not (e['source'] == source and e['target'] == target)]


def apply(d, operation):
    op, *args = operation
    if op == 'set':
        params(d, args[0])[args[1]] = args[2]
    elif op == 'del':
        params(d, args[0]).pop(args[1], None)
    elif op == 'remove_edge':
        remove_edge(d, *args)
    elif op == 'add_edge':
        d['edges'].append({'source': args[0], 'target': args[1]})
    elif op == 'node_type':
        node(d, args[0])['type'] = args[1]
    elif op == 'label':
        node(d, args[0])['data']['label'] = args[1]
    elif op == 'append_source':
        d['nodes'].append({'id': args[0], 'type': 'kafkaTopic', 'data': {'label': args[0], 'params': {}}})
    elif op == 'append_sink':
        d['nodes'].append({'id': args[0], 'type': 'parquetSink', 'data': {'path': '/tmp/sink', 'params': {}}})
    else:
        raise AssertionError(f'unknown mutation: {operation}')

# Row = identifier, example file, mutations, expected valid, expected diagnostic (or None).
# Warnings may be present even for valid=True; valid means no ERROR issues.
F = 'filter_orders.json'
W = 'windowed_sales_to_parquet.json'
J = 'bounded_stream_join.json'
CASES = [
    ('T01_filter_reference', F, [], True, None),
    ('T02_window_reference', W, [], True, None),
    ('T03_join_reference', J, [], True, None),
    ('T04_missing_parse_schema', F, [('set','parse-orders','schema','')], False, 'PARSEJSON_SCHEMA_MISSING'),
    ('T05_missing_filter_predicate', F, [('set','filter-completed','condition','')], False, 'FILTER_CONDITION_MISSING'),
    ('T06_disconnected_source', F, [('remove_edge','orders-source','parse-orders')], False, 'DISCONNECTED_SOURCE'),
    ('T07_transform_arity', F, [('remove_edge','parse-orders','filter-completed')], False, 'TRANSFORM_ARITY'),
    ('T08_kafka_sink_outgoing', F, [('append_sink','extra-sink'),('add_edge','filtered-sink','extra-sink')], False, 'KAFKA_SINK_HAS_OUTPUT'),
    ('T09_kafka_sink_multi_parent', F, [('add_edge','parse-orders','filtered-sink')], False, 'KAFKA_SINK_ARITY'),
    ('T10_parquet_sink_not_terminal', W, [('append_sink','extra-sink'),('add_edge','parquet-sink','extra-sink')], False, 'PARQUET_SINK_HAS_OUTPUT'),
    ('T11_parquet_sink_no_parent', W, [('remove_edge','sum-amount','parquet-sink')], False, 'PARQUET_SINK_ARITY'),
    ('T12_group_columns_missing', W, [('set','group-window','columns','')], False, 'GROUPBY_COLUMNS_MISSING'),
    ('T13_incomplete_window', W, [('del','group-window','timestampCol')], False, 'INCOMPLETE_WINDOW_CONFIG'),
    ('T14_missing_watermark_for_parquet', W, [('del','group-window','watermarkDelay')], False, 'PARQUET_AGGREGATION_UNSUPPORTED_BY_CURRENT_GENERATOR'),
    ('T15_nonwindowed_parquet_aggregation', W, [('set','group-window','windowEnabled',False)], False, 'PARQUET_AGGREGATION_UNSUPPORTED_BY_CURRENT_GENERATOR'),
    ('T16_group_without_aggregate', W, [('remove_edge','group-window','sum-amount')], False, 'GROUPBY_WITHOUT_AGGREGATE'),
    ('T17_streaming_dedup_without_watermark', W, [('set','group-window','windowEnabled',False),('set','sum-amount','dedup_before',True)], False, 'STREAMING_DEDUP_WITHOUT_WATERMARK'),
    ('T18_join_key_missing', J, [('set','bounded-join','joinKey','')], False, 'JOIN_KEY_MISSING'),
    ('T19_join_watermark_disabled', J, [('set','bounded-join','enableWatermarks',False)], False, 'STREAM_STREAM_JOIN_NO_WATERMARK'),
    ('T20_join_time_bound_disabled', J, [('set','bounded-join','timeBoundEnabled',False)], False, 'STREAM_STREAM_JOIN_NO_TIME_BOUND'),
    ('T21_join_bound_invalid', J, [('set','bounded-join','timeRangeBefore','not-a-duration')], False, 'STREAM_STREAM_JOIN_TIME_BOUND_INVALID'),
    ('T22_join_broadcast_stream', J, [('set','bounded-join','broadcast',True)], False, 'BROADCAST_STREAMING_SIDE'),
    ('T23_stream_cross_join', J, [('set','bounded-join','joinType','cross')], False, 'STREAM_STREAM_CROSS_JOIN_UNSUPPORTED'),
    ('T24_batch_to_stream_sink', F, [('set','orders-source','readMode','batch')], False, 'BATCH_TO_STREAMING_SINK'),
    ('T25_join_nonstandard_type_warning', J, [('set','bounded-join','joinType','nonstandard')], True, 'UNUSUAL_JOIN_TYPE'),
    ('T26_missing_aggregation_list_warning', W, [('set','sum-amount','aggregations',[])], True, 'AGGREGATIONS_MISSING'),
    ('T27_unconsumed_transform_warning', F, [('remove_edge','filter-completed','filtered-sink')], False, 'UNCONSUMED_TRANSFORM'),
    ('T28_invalid_source_read_mode', F, [('set','orders-source','readMode','nonsense')], False, 'INVALID_READ_MODE'),
]


@pytest.mark.parametrize('case_id,example,changes,expected_valid,expected_code', CASES,
                         ids=[x[0] for x in CASES])
def test_semantic_decision_and_diagnostic(case_id, example, changes, expected_valid, expected_code):
    dag_json = load(example)
    for change in changes:
        apply(dag_json, change)
    dag = StreamETLDAG.model_validate(dag_json)
    report = SemanticAdvisor.analyze(dag)
    codes = {issue.code for issue in report.issues}
    assert report.valid is expected_valid, f'{case_id}: expected {expected_valid}, got codes {codes}'
    if expected_code:
        assert expected_code in codes, f'{case_id}: expected {expected_code}, got {codes}'
    assert report.summary.errors == sum(i.severity.value == 'error' for i in report.issues)
    assert report.summary.warnings == sum(i.severity.value == 'warning' for i in report.issues)


@pytest.mark.parametrize('mutation', [
    ('duplicate_id',),('cycle',),('self_loop',),('unknown_target',),('unknown_type',)
], ids=['duplicate_id','cycle','self_loop','unknown_target','unknown_type'])
def test_pydantic_graph_contract_rejects_invalid_topology(mutation):
    dag_json = load(F)
    kind = mutation[0]
    if kind == 'duplicate_id': node(dag_json,'parse-orders')['id'] = 'orders-source'
    elif kind == 'cycle': dag_json['edges'].append({'source':'filter-completed','target':'parse-orders'})
    elif kind == 'self_loop': dag_json['edges'].append({'source':'parse-orders','target':'parse-orders'})
    elif kind == 'unknown_target': dag_json['edges'].append({'source':'parse-orders','target':'ghost'})
    elif kind == 'unknown_type': node(dag_json,'parse-orders')['type'] = 'MagicTransformation'
    with pytest.raises(ValidationError):
        StreamETLDAG.model_validate(dag_json)
