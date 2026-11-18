"""Conservative, static schema propagation for StreamETL native DAGs.

No Spark runtime, Kafka connection, or LLM call. An absent column is an ERROR
only when the upstream schema is closed/known. UNKNOWN schemas are not guessed.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import re
from typing import Dict, List, Mapping, Optional, Set, Tuple

from .schemas import NodeType, Severity, StreamETLDAG, TransformType, ValidationIssue


_KAFKA_FIELDS: Dict[str, str] = {
    "key": "BINARY", "value": "BINARY", "topic": "STRING",
    "partition": "INT", "offset": "LONG", "timestamp": "TIMESTAMP",
    "timestampType": "INT",
}
_SQL_TYPES = {"STRING", "INT", "INTEGER", "BIGINT", "LONG", "DOUBLE", "FLOAT", "BOOLEAN",
              "TIMESTAMP", "DATE", "DECIMAL", "SMALLINT", "TINYINT", "BINARY", "SHORT"}
_SQL_KEYWORDS = {"AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "BETWEEN", "LIKE",
                 "RLIKE", "REGEXP", "AS", "CAST", "COALESCE", "CASE", "WHEN", "THEN", "ELSE",
                 "END", "IF", "EXISTS", "DISTINCT", "INTERVAL", "SECOND", "MINUTE", "HOUR",
                 "DAY", "CURRENT_TIMESTAMP", "CURRENT_DATE"} | _SQL_TYPES
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*$")


@dataclass(frozen=True)
class SchemaState:
    columns: Mapping[str, str]
    complete: bool

    @classmethod
    def unknown(cls) -> "SchemaState":
        return cls({}, False)


def _issue(code: str, node_id: str, message: str, recommendation: str, severity=Severity.ERROR):
    return ValidationIssue(code=code, severity=severity, node_id=node_id,
                           message=message, recommendation=recommendation)


def _split_top_level(text: str) -> List[str]:
    """Split a Spark SQL DDL field list without splitting decimal/struct nesting."""
    out, buff, nesting, quote = [], [], 0, None
    for ch in text:
        if quote:
            buff.append(ch)
            if ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
            buff.append(ch)
        elif ch in "(<[":
            nesting += 1
            buff.append(ch)
        elif ch in ")>]":
            nesting -= 1
            buff.append(ch)
        elif ch == "," and nesting == 0:
            out.append("".join(buff).strip())
            buff = []
        else:
            buff.append(ch)
    if buff:
        out.append("".join(buff).strip())
    return out


def _parse_simple_ddl(ddl: str) -> SchemaState:
    """Recognize safe flat DDL; otherwise return UNKNOWN (never invent fields)."""
    if not ddl.strip():
        return SchemaState.unknown()
    columns = {}
    for part in _split_top_level(ddl):
        match = re.fullmatch(r"\s*`?([A-Za-z_][\w]*)`?\s+([A-Za-z][\w]*)(?:\s*\((\d+(?:\s*,\s*\d+)?)\))?\s*", part)
        if not match or match.group(2).upper() not in _SQL_TYPES:
            return SchemaState.unknown()
        name, typ = match.group(1), match.group(2).upper()
        if name in columns:
            return SchemaState.unknown()
        columns[name] = typ
    return SchemaState(columns, True)


def _column_check(node_id: str, state: SchemaState, name: object, issues: List[ValidationIssue], *, usage: str, expected_type: Optional[str] = None) -> None:
    col = str(name or "").strip().strip("`")
    if not col or not _IDENTIFIER.fullmatch(col):
        return
    if col not in state.columns:
        if state.complete:
            issues.append(_issue("SCHEMA_COLUMN_UNAVAILABLE", node_id,
                f"{usage} references column {col!r}, absent from the incoming schema: {sorted(state.columns)}.",
                "Insert ParseJSON or a transformation producing this column, or change the column reference."))
        return
    if expected_type == "TIMESTAMP" and state.columns[col] != "TIMESTAMP":
        issues.append(_issue("SCHEMA_EVENT_TIME_TYPE", node_id,
            f"{usage} requires TIMESTAMP column {col!r}, found {state.columns[col]!r}.",
            "Define event_time as TIMESTAMP in ParseJSON or apply an explicit timestamp conversion."))


def _filter_identifiers(expression: str) -> Optional[Set[str]]:
    """Only inspect a simple SQL predicate. Complex SQL is UNKNOWN, not rejected."""
    # Reject constructs that a lexical scan cannot safely resolve.
    if not expression or any(c in expression for c in ('`', ';', '[', ']', '{', '}', '$')):
        return None
    # Remove string literals and numeric literals, then known keywords/functions.
    without_strings = re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", " ", expression)
    if "'" in without_strings or '"' in without_strings:
        return None
    if re.search(r"\b(?:SELECT|FROM|OVER|EXTRACT|STRUCT|TRANSFORM|LAMBDA)\b", without_strings, re.I):
        return None
    without_strings = re.sub(r"\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b", " ", without_strings)
    result = set()
    for match in re.finditer(r"[A-Za-z_][A-Za-z_0-9]*", without_strings):
        value = match.group(0)
        if value.upper() in _SQL_KEYWORDS:
            continue
        following = without_strings[match.end():].lstrip()
        if following.startswith("("):
            # Function name; expressions like functions of columns remain checked.
            continue
        # Skip dotted/qualified expressions unless parsed safely.
        prior = without_strings[:match.start()].rstrip()
        if prior.endswith(".") or following.startswith("."):
            return None
        result.add(value)
    return result


class SchemaAnalyzer:
    """Conservative schema-aware diagnostics; usable by SemanticAdvisor.analyze."""

    @classmethod
    def analyze(cls, pipeline: StreamETLDAG) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        nodes = {n.id: n for n in pipeline.nodes}
        parents: Dict[str, List[str]] = defaultdict(list)
        children: Dict[str, List[str]] = defaultdict(list)
        indegree = {n.id: 0 for n in pipeline.nodes}
        for edge in pipeline.edges:
            parents[edge.target].append(edge.source)
            children[edge.source].append(edge.target)
            indegree[edge.target] += 1
        ready = deque(n.id for n in pipeline.nodes if indegree[n.id] == 0)
        states: Dict[str, SchemaState] = {}
        while ready:
            node_id = ready.popleft()
            node = nodes[node_id]
            upstream = [states.get(p, SchemaState.unknown()) for p in parents[node_id]]
            base = upstream[0] if len(upstream) == 1 else SchemaState.unknown()
            current = base
            if node.type == NodeType.KAFKA_TOPIC:
                # Incoming edges mean the Kafka node is a sink, not a source.
                current = base if upstream else SchemaState(dict(_KAFKA_FIELDS), True)
            elif node.type == NodeType.SPARK_TRANSFORM:
                kind = node.data.transformType
                params = node.data.params
                if kind == TransformType.PARSE_JSON:
                    # Generator selects 'data.*', so the output is the parsed DDL only.
                    current = _parse_simple_ddl(str(params.get("schema", "")))
                elif kind == TransformType.FILTER:
                    current = base
                    refs = _filter_identifiers(str(params.get("condition", "")))
                    if refs is not None:
                        for ref in sorted(refs):
                            _column_check(node_id, base, ref, issues, usage="Filter")
                elif kind == TransformType.GROUP_BY:
                    # Generator stores group configuration; Aggregate performs reduction.
                    for ref in _split_top_level(str(params.get("columns", ""))):
                        _column_check(node_id, base, ref, issues, usage="GroupBy")
                    if params.get("windowEnabled"):
                        _column_check(node_id, base, params.get("timestampCol"), issues,
                                      usage="GroupBy watermark", expected_type="TIMESTAMP")
                    current = base
                elif kind == TransformType.AGGREGATE:
                    # We cannot guarantee generator's output columns without inspecting
                    # the current Aggregate code; do not pass through a false schema.
                    for agg in params.get("aggregations", []) if isinstance(params.get("aggregations"), list) else []:
                        if isinstance(agg, dict):
                            col = next((agg[k] for k in ("column", "col", "field") if isinstance(agg.get(k), str) and agg[k].strip()), None)
                            if col and col.strip() != "*":
                                # Aggregate's direct parent is usually GroupBy, which passes input through.
                                _column_check(node_id, base, col, issues, usage="Aggregate")
                    current = SchemaState.unknown()
                elif kind == TransformType.JOIN:
                    if len(upstream) == 2:
                        left, right = upstream
                        join_type = str(params.get("joinType", "inner")).strip().lower()
                        if join_type != "cross":
                            lk = params.get("joinKey")
                            rk = params.get("rightKey") or lk
                            _column_check(node_id, left, lk, issues, usage="Join left key")
                            _column_check(node_id, right, rk, issues, usage="Join right key")
                        if params.get("enableWatermarks"):
                            _column_check(node_id, left, params.get("leftWatermarkCol"), issues,
                                          usage="Join left watermark", expected_type="TIMESTAMP")
                            _column_check(node_id, right, params.get("rightWatermarkCol"), issues,
                                          usage="Join right watermark", expected_type="TIMESTAMP")
                        # Generated join projections rename shared non-key fields;
                        # do not assert a complete output schema prematurely.
                        current = SchemaState.unknown()
                    else:
                        current = SchemaState.unknown()
            states[node_id] = current
            for child in children[node_id]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
        return issues
