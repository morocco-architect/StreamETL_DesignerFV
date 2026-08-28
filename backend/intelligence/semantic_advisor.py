from __future__ import annotations

from collections import defaultdict, deque
import re
from typing import Dict, Iterable, List, Optional, Set

from .schemas import (
    NodeType,
    PipelineNode,
    RepairAction,
    RepairActionType,
    Severity,
    StreamETLDAG,
    TransformType,
    ValidationIssue,
    ValidationReport,
    ValidationSummary,
)


_REFERENCE_HINTS = (
    "user",
    "users",
    "reference",
    "lookup",
    "dimension",
    "dim_",
    "catalog",
    "master",
    "profile",
)

_DURATION_RE = re.compile(
    r"^\s*\d+\s*(second|seconds|minute|minutes|hour|hours|day|days)\s*$",
    re.I,
)


def _valid_duration(value: object) -> bool:
    return bool(_DURATION_RE.fullmatch(str(value or "")))


class SemanticAdvisor:
    """Deterministic StreamETL DAG analysis.

    No LLM is used here. Rules are aligned with the CURRENT StreamETL native
    node model and generator parameters rather than with a parallel AI-only
    schema.
    """

    @classmethod
    def analyze(cls, pipeline: StreamETLDAG) -> ValidationReport:
        graph = _GraphContext(pipeline)
        issues: List[ValidationIssue] = []

        for node in pipeline.nodes:
            indegree = len(graph.parents[node.id])
            outdegree = len(graph.children[node.id])

            if node.type == NodeType.KAFKA_TOPIC:
                if indegree == 0:
                    if outdegree == 0:
                        issues.append(_issue(
                            "DISCONNECTED_SOURCE",
                            Severity.ERROR,
                            node,
                            "Kafka source is not connected to any downstream node.",
                            "Connect the source to a transformation or sink.",
                        ))
                    read_mode = _read_mode(node)
                    if read_mode not in {"stream", "batch"}:
                        issues.append(_issue(
                            "INVALID_READ_MODE",
                            Severity.ERROR,
                            node,
                            f"Unsupported Kafka readMode={read_mode!r}.",
                            "Use 'stream' or 'batch'.",
                        ))
                else:
                    if outdegree > 0:
                        issues.append(_issue(
                            "KAFKA_SINK_HAS_OUTPUT",
                            Severity.ERROR,
                            node,
                            "A Kafka node with incoming edges is treated by the current generator as a sink and cannot feed another node.",
                            "Use a dedicated Kafka source node for downstream processing.",
                        ))
                    if indegree != 1:
                        issues.append(_issue(
                            "KAFKA_SINK_ARITY",
                            Severity.ERROR,
                            node,
                            f"Kafka sink has {indegree} incoming edges; the current generator consumes exactly one.",
                            "Connect exactly one upstream branch to the sink.",
                        ))

            elif node.type == NodeType.PARQUET_SINK:
                if indegree != 1:
                    issues.append(_issue(
                        "PARQUET_SINK_ARITY",
                        Severity.ERROR,
                        node,
                        f"Parquet sink has {indegree} incoming edges; exactly one is required.",
                        "Connect exactly one upstream branch.",
                    ))
                if outdegree:
                    issues.append(_issue(
                        "PARQUET_SINK_HAS_OUTPUT",
                        Severity.ERROR,
                        node,
                        "Parquet sink must be terminal.",
                        "Remove outgoing edges from the sink.",
                    ))

            elif node.type == NodeType.SPARK_TRANSFORM:
                cls._analyze_transform(node, graph, issues)

        cls._analyze_runtime_lineage(pipeline, graph, issues)
        summary = ValidationSummary(
            errors=sum(i.severity == Severity.ERROR for i in issues),
            warnings=sum(i.severity == Severity.WARNING for i in issues),
            info=sum(i.severity == Severity.INFO for i in issues),
        )
        return ValidationReport(valid=summary.errors == 0, issues=issues, summary=summary)

    @classmethod
    def _analyze_transform(
        cls,
        node: PipelineNode,
        graph: "_GraphContext",
        issues: List[ValidationIssue],
    ) -> None:
        transform = node.data.transformType
        params = node.data.params
        indegree = len(graph.parents[node.id])
        outdegree = len(graph.children[node.id])

        expected_inputs = 2 if transform == TransformType.JOIN else 1
        if indegree != expected_inputs:
            issues.append(_issue(
                "TRANSFORM_ARITY",
                Severity.ERROR,
                node,
                f"{transform.value if transform else 'Transform'} expects {expected_inputs} incoming edge(s), found {indegree}.",
                "Correct the graph connectivity before code generation.",
            ))
        if outdegree == 0:
            issues.append(_issue(
                "UNCONSUMED_TRANSFORM",
                Severity.WARNING,
                node,
                "Transformation output is not connected to a sink or downstream transformation.",
                "Connect the node if its result is intended to be executed.",
            ))

        if transform == TransformType.PARSE_JSON:
            schema = str(params.get("schema", "")).strip()
            if not schema:
                issues.append(_issue(
                    "PARSEJSON_SCHEMA_MISSING",
                    Severity.ERROR,
                    node,
                    "ParseJSON has no schema, while the current generator requires one to call from_json().",
                    "Provide an explicit Spark SQL schema, e.g. 'id STRING, amount DOUBLE, timestamp TIMESTAMP'.",
                ))

        elif transform == TransformType.FILTER:
            if not str(params.get("condition", "")).strip():
                issues.append(_issue(
                    "FILTER_CONDITION_MISSING",
                    Severity.ERROR,
                    node,
                    "Filter has an empty condition.",
                    "Provide a Spark SQL filter expression.",
                ))

        elif transform == TransformType.GROUP_BY:
            columns = str(params.get("columns", "")).strip()
            if not columns:
                issues.append(_issue(
                    "GROUPBY_COLUMNS_MISSING",
                    Severity.ERROR,
                    node,
                    "GroupBy has no grouping columns.",
                    "Select at least one grouping column.",
                ))

            window_enabled = bool(params.get("windowEnabled", False))
            if window_enabled:
                missing = [
                    key
                    for key in ("windowDuration", "timestampCol", "watermarkDelay")
                    if not str(params.get(key, "")).strip()
                ]
                if missing:
                    issues.append(_issue(
                        "INCOMPLETE_WINDOW_CONFIG",
                        Severity.ERROR,
                        node,
                        f"Windowing is enabled but required field(s) are missing: {', '.join(missing)}.",
                        "Set window duration, event-time column and watermark delay.",
                    ))

            children = graph.children[node.id]
            if not any(
                graph.nodes[c].type == NodeType.SPARK_TRANSFORM
                and graph.nodes[c].data.transformType == TransformType.AGGREGATE
                for c in children
            ):
                issues.append(_issue(
                    "GROUPBY_WITHOUT_AGGREGATE",
                    Severity.ERROR,
                    node,
                    "The current generator stores GroupBy configuration and expects the next connected node to be Aggregate.",
                    "Connect GroupBy directly to an Aggregate node.",
                ))

        elif transform == TransformType.AGGREGATE:
            aggregations = params.get("aggregations")
            if not isinstance(aggregations, list) or not aggregations:
                issues.append(_issue(
                    "AGGREGATIONS_MISSING",
                    Severity.WARNING,
                    node,
                    "No explicit aggregation list was supplied; the generator will fall back to count(*).",
                    "Define the intended aggregate function(s) explicitly for reproducibility.",
                ))

            if bool(params.get("dedup_before", False)) and graph.node_is_streaming(node.id):
                groupby = graph.direct_groupby_parent(node.id)
                has_watermark = bool(
                    groupby
                    and groupby.data.params.get("windowEnabled")
                    and str(groupby.data.params.get("timestampCol", "")).strip()
                    and str(groupby.data.params.get("watermarkDelay", "")).strip()
                )
                if not has_watermark:
                    issues.append(ValidationIssue(
                        code="STREAMING_DEDUP_WITHOUT_WATERMARK",
                        severity=Severity.ERROR,
                        node_id=node.id,
                        message="Streaming deduplication is enabled before Aggregate without a valid upstream watermark.",
                        recommendation="Configure a window+watermark on the directly preceding GroupBy, or disable dedup_before.",
                        repair_action=RepairAction(
                            action=RepairActionType.DISABLE_AGGREGATE_DEDUP,
                            node_id=node.id,
                        ),
                    ))

            if graph.node_is_streaming(node.id):
                groupby = graph.direct_groupby_parent(node.id)
                has_window = bool(groupby and groupby.data.params.get("windowEnabled"))
                if not has_window:
                    issues.append(_issue(
                        "UNBOUNDED_STREAMING_AGGREGATION",
                        Severity.WARNING,
                        node,
                        "Streaming aggregation is not windowed. State can grow as new grouping keys arrive.",
                        "If the use case is time-bounded, enable event-time windowing and a watermark on the preceding GroupBy.",
                    ))

        elif transform == TransformType.JOIN:
            join_type = str(params.get("joinType", "inner")).strip().lower()
            if join_type not in {"inner", "left", "right", "full", "outer", "left_outer", "right_outer", "full_outer", "cross"}:
                issues.append(_issue(
                    "UNUSUAL_JOIN_TYPE",
                    Severity.WARNING,
                    node,
                    f"Join type {join_type!r} is not among the commonly expected Spark join types.",
                    "Verify the join type against the Spark version used by StreamETL.",
                ))
            if join_type != "cross" and not str(params.get("joinKey", "")).strip():
                issues.append(_issue(
                    "JOIN_KEY_MISSING",
                    Severity.ERROR,
                    node,
                    "Join has no joinKey.",
                    "Provide the left join key and optional rightKey.",
                ))

            if indegree == 2:
                left_id, right_id = graph.parents[node.id]
                left_stream = graph.node_is_streaming(left_id)
                right_stream = graph.node_is_streaming(right_id)

                if bool(params.get("broadcast", False)) and right_stream:
                    issues.append(_issue(
                        "BROADCAST_STREAMING_SIDE",
                        Severity.ERROR,
                        node,
                        "Broadcast is enabled but the generator would apply broadcast() to a streaming right-hand DataFrame.",
                        "Use broadcast only for a measured-small static branch (readMode='batch'), or disable broadcast.",
                    ))

                if left_stream and right_stream:
                    if join_type == "cross":
                        issues.append(_issue(
                            "STREAM_STREAM_CROSS_JOIN_UNSUPPORTED",
                            Severity.ERROR,
                            node,
                            "A cross join between two unbounded streaming inputs is not accepted by StreamETL's safe compiler profile.",
                            "Use an explicit bounded keyed join, or make one side static/batch if it is reference data.",
                        ))
                        return

                    wm_complete = all(
                        str(params.get(k, "")).strip()
                        for k in (
                            "leftWatermarkCol",
                            "leftWatermarkDelay",
                            "rightWatermarkCol",
                            "rightWatermarkDelay",
                        )
                    ) and bool(params.get("enableWatermarks", False))
                    if not wm_complete:
                        issues.append(_issue(
                            "STREAM_STREAM_JOIN_NO_WATERMARK",
                            Severity.ERROR,
                            node,
                            "Both Join inputs are streaming but complete event-time watermarks are not configured.",
                            "Enable watermarks and provide event-time columns and delays for both streams.",
                        ))

                    time_bound_enabled = bool(params.get("timeBoundEnabled", False))
                    if not time_bound_enabled:
                        issues.append(_issue(
                            "STREAM_STREAM_JOIN_NO_TIME_BOUND",
                            Severity.ERROR,
                            node,
                            "Both Join inputs are streaming but no event-time range bound is configured.",
                            "Enable a bounded event-time range (timeRangeBefore/timeRangeAfter), or use a stream-static join for reference data.",
                        ))
                    else:
                        before = params.get("timeRangeBefore", "")
                        after = params.get("timeRangeAfter", "")
                        if not (_valid_duration(before) and _valid_duration(after)):
                            issues.append(_issue(
                                "STREAM_STREAM_JOIN_TIME_BOUND_INVALID",
                                Severity.ERROR,
                                node,
                                "The stream-stream time bound is incomplete or uses an unsupported duration format.",
                                "Use values such as '10 minutes' and '5 minutes' for timeRangeBefore/timeRangeAfter.",
                            ))
                elif left_stream != right_stream:
                    static_id = right_id if left_stream else left_id
                    static_node = graph.nodes[static_id]
                    if static_node.type == NodeType.KAFKA_TOPIC and not bool(params.get("broadcast", False)):
                        issues.append(_issue(
                            "STATIC_JOIN_BROADCAST_CANDIDATE",
                            Severity.INFO,
                            node,
                            f"Join uses a static Kafka branch ({static_node.data.label}).",
                            "If the static reference is small enough, enable broadcast after measuring its size.",
                        ))

    @classmethod
    def _analyze_runtime_lineage(
        cls,
        pipeline: StreamETLDAG,
        graph: "_GraphContext",
        issues: List[ValidationIssue],
    ) -> None:
        # The current sink generator always calls writeStream. A pipeline that is
        # purely batch therefore cannot terminate in these sinks.
        for node in pipeline.nodes:
            is_sink = (
                node.type == NodeType.PARQUET_SINK
                or (node.type == NodeType.KAFKA_TOPIC and bool(graph.parents[node.id]))
            )
            if is_sink and graph.parents[node.id]:
                upstream = graph.parents[node.id][0]

                if (
                    node.type == NodeType.PARQUET_SINK
                    and graph.path_has_aggregation(upstream)
                    and graph.path_has_non_append_safe_aggregation(upstream)
                ):
                    issues.append(_issue(
                        "PARQUET_AGGREGATION_UNSUPPORTED_BY_CURRENT_GENERATOR",
                        Severity.ERROR,
                        node,
                        "Parquet/file sinks are Append-only, but this branch contains a streaming aggregation that is not windowed and watermarked for finalized append output.",
                        "Use a windowed+watermarked GroupBy→Aggregate before Parquet, or route update/complete aggregation results to Kafka.",
                    ))

                if not graph.node_is_streaming(upstream):
                    issues.append(_issue(
                        "BATCH_TO_STREAMING_SINK",
                        Severity.ERROR,
                        node,
                        "The upstream branch is batch/static, but the current StreamETL sink generator calls writeStream.",
                        "Use a streaming branch at the sink or extend the deterministic generator with batch write support.",
                    ))

        # Heuristic advisory only: never auto-modify source mode based on its name.
        for node in pipeline.nodes:
            if node.type != NodeType.KAFKA_TOPIC or graph.parents[node.id]:
                continue
            label = (node.data.label or "").lower()
            if _read_mode(node) == "stream" and any(hint in label for hint in _REFERENCE_HINTS):
                # Only useful if the source actually feeds a Join.
                if any(
                    graph.nodes[d].type == NodeType.SPARK_TRANSFORM
                    and graph.nodes[d].data.transformType == TransformType.JOIN
                    for d in graph.descendants(node.id)
                ):
                    issues.append(ValidationIssue(
                        code="JOIN_STATIC_REFERENCE_CANDIDATE",
                        severity=Severity.INFO,
                        node_id=node.id,
                        message=f"Topic {node.data.label!r} looks like reference data but is configured as a stream.",
                        recommendation="If measurements confirm it is a small, bounded reference dataset, consider readMode='batch' and optionally broadcast it in the Join.",
                        repair_action=RepairAction(
                            action=RepairActionType.SET_SOURCE_READ_MODE,
                            node_id=node.id,
                            params={"readMode": "batch"},
                        ),
                    ))


class _GraphContext:
    def __init__(self, pipeline: StreamETLDAG) -> None:
        self.pipeline = pipeline
        self.nodes: Dict[str, PipelineNode] = {n.id: n for n in pipeline.nodes}
        self.parents: Dict[str, List[str]] = defaultdict(list)
        self.children: Dict[str, List[str]] = defaultdict(list)
        for edge in pipeline.edges:
            self.parents[edge.target].append(edge.source)
            self.children[edge.source].append(edge.target)
        self._stream_cache: Dict[str, bool] = {}
        self._agg_cache: Dict[str, bool] = {}

    def node_is_streaming(self, node_id: str, _trail: Optional[Set[str]] = None) -> bool:
        if node_id in self._stream_cache:
            return self._stream_cache[node_id]
        node = self.nodes[node_id]
        parents = self.parents[node_id]

        if node.type == NodeType.KAFKA_TOPIC and not parents:
            result = _read_mode(node) != "batch"
        elif not parents:
            result = False
        else:
            # Join is streaming if either branch is streaming; other transforms
            # have one input under the semantic arity rules.
            result = any(self.node_is_streaming(parent) for parent in parents)

        self._stream_cache[node_id] = result
        return result

    def path_has_aggregation(self, node_id: str) -> bool:
        if node_id in self._agg_cache:
            return self._agg_cache[node_id]
        node = self.nodes[node_id]
        here = (
            node.type == NodeType.SPARK_TRANSFORM
            and node.data.transformType == TransformType.AGGREGATE
        )
        result = here or any(self.path_has_aggregation(parent) for parent in self.parents[node_id])
        self._agg_cache[node_id] = result
        return result

    def path_has_non_append_safe_aggregation(self, node_id: str) -> bool:
        node = self.nodes[node_id]
        if node.type == NodeType.SPARK_TRANSFORM and node.data.transformType == TransformType.AGGREGATE:
            groupby = self.direct_groupby_parent(node_id)
            append_safe_here = bool(
                groupby
                and groupby.data.params.get("windowEnabled")
                and str(groupby.data.params.get("windowDuration", "")).strip()
                and str(groupby.data.params.get("timestampCol", "")).strip()
                and str(groupby.data.params.get("watermarkDelay", "")).strip()
            )
            if not append_safe_here:
                return True
        return any(self.path_has_non_append_safe_aggregation(parent) for parent in self.parents[node_id])

    def descendants(self, node_id: str) -> Iterable[str]:
        seen: Set[str] = set()
        queue = deque(self.children[node_id])
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            yield current
            queue.extend(self.children[current])

    def direct_groupby_parent(self, node_id: str) -> Optional[PipelineNode]:
        parents = self.parents[node_id]
        if len(parents) != 1:
            return None
        parent = self.nodes[parents[0]]
        if parent.type == NodeType.SPARK_TRANSFORM and parent.data.transformType == TransformType.GROUP_BY:
            return parent
        return None


def _read_mode(node: PipelineNode) -> str:
    return str(node.data.params.get("readMode", "stream")).strip().lower()


def _issue(
    code: str,
    severity: Severity,
    node: PipelineNode,
    message: str,
    recommendation: str,
) -> ValidationIssue:
    return ValidationIssue(
        code=code,
        severity=severity,
        node_id=node.id,
        message=message,
        recommendation=recommendation,
    )
