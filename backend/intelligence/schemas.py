from __future__ import annotations

from enum import Enum
import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from typing_extensions import Annotated


NodeId = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120)]
_KAFKA_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


class NodeType(str, Enum):
    """Native node types used by the current StreamETL React Flow canvas."""

    KAFKA_TOPIC = "kafkaTopic"
    SPARK_TRANSFORM = "sparkTransform"
    PARQUET_SINK = "parquetSink"


class TransformType(str, Enum):
    """Transform vocabulary implemented by the existing PySpark generator."""

    FILTER = "Filter"
    PARSE_JSON = "ParseJSON"
    GROUP_BY = "GroupBy"
    AGGREGATE = "Aggregate"
    JOIN = "Join"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class Position(BaseModel):
    x: float = 0.0
    y: float = 0.0


class NodeData(BaseModel):
    """Subset of React Flow node data needed by the backend.

    Extra serialisable UI fields are ignored intentionally so that a DAG copied
    from the live React application can be analysed without schema drift.
    """

    model_config = ConfigDict(extra="ignore")

    label: Optional[str] = None
    transformType: Optional[TransformType] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    path: Optional[str] = None


class PipelineNode(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: NodeId
    type: NodeType
    data: NodeData
    position: Optional[Position] = None

    @model_validator(mode="after")
    def validate_node_contract(self) -> "PipelineNode":
        if self.type == NodeType.KAFKA_TOPIC:
            label = (self.data.label or "").strip()
            if not label:
                raise ValueError("kafkaTopic nodes require data.label (Kafka topic name)")
            if not _KAFKA_TOPIC_RE.fullmatch(label):
                raise ValueError(
                    "Kafka topic names must be 1-249 characters using only letters, "
                    "digits, '.', '_' or '-'"
                )
        if self.type == NodeType.SPARK_TRANSFORM and self.data.transformType is None:
            raise ValueError("sparkTransform nodes require data.transformType")
        return self


class PipelineEdge(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Optional[str] = None
    source: NodeId
    target: NodeId


class StreamETLDAG(BaseModel):
    """Native StreamETL DAG accepted by the current /api/pipeline/generate route."""

    nodes: List[PipelineNode] = Field(min_length=1)
    edges: List[PipelineEdge] = Field(default_factory=list)

    @model_validator(mode="after")
    def graph_integrity(self) -> "StreamETLDAG":
        node_ids = [node.id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("Node ids must be unique")

        known = set(node_ids)
        edge_pairs: set[tuple[str, str]] = set()
        adjacency: Dict[str, List[str]] = {node_id: [] for node_id in node_ids}
        indegree: Dict[str, int] = {node_id: 0 for node_id in node_ids}

        for edge in self.edges:
            if edge.source not in known or edge.target not in known:
                raise ValueError(
                    f"Edge {edge.source!r}->{edge.target!r} references an unknown node"
                )
            if edge.source == edge.target:
                raise ValueError(f"Self-loop is not allowed on node {edge.source!r}")
            pair = (edge.source, edge.target)
            if pair in edge_pairs:
                raise ValueError(f"Duplicate edge {edge.source!r}->{edge.target!r}")
            edge_pairs.add(pair)
            adjacency[edge.source].append(edge.target)
            indegree[edge.target] += 1

        # Kahn cycle check. The generator expects a DAG, so fail early here.
        queue = [node_id for node_id, degree in indegree.items() if degree == 0]
        visited = 0
        while queue:
            current = queue.pop(0)
            visited += 1
            for target in adjacency[current]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)
        if visited != len(node_ids):
            raise ValueError("Pipeline graph contains a cycle")

        return self


class RepairActionType(str, Enum):
    SET_SOURCE_READ_MODE = "set_source_read_mode"
    ENABLE_GROUPBY_WINDOW = "enable_groupby_window"
    SET_JOIN_WATERMARKS = "set_join_watermarks"
    SET_JOIN_TIME_BOUND = "set_join_time_bound"
    DISABLE_AGGREGATE_DEDUP = "disable_aggregate_dedup"


class RepairAction(BaseModel):
    action: RepairActionType
    node_id: NodeId
    params: Dict[str, Any] = Field(default_factory=dict)


class ValidationIssue(BaseModel):
    code: str
    severity: Severity
    node_id: Optional[str] = None
    message: str
    recommendation: Optional[str] = None
    repair_action: Optional[RepairAction] = None


class ValidationSummary(BaseModel):
    errors: int = 0
    warnings: int = 0
    info: int = 0


class ValidationReport(BaseModel):
    valid: bool
    issues: List[ValidationIssue] = Field(default_factory=list)
    summary: ValidationSummary


class PipelineBuildRequest(BaseModel):
    prompt: Annotated[str, StringConstraints(strip_whitespace=True, min_length=3, max_length=6000)]


class PipelineSuggestion(StreamETLDAG):
    explanation: Optional[str] = None


class PipelineBuildResponse(BaseModel):
    pipeline: PipelineSuggestion
    validation: ValidationReport
    provider: str
    model: Optional[str] = None


class RepairRequest(BaseModel):
    pipeline: StreamETLDAG
    actions: List[RepairAction] = Field(min_length=1, max_length=20)


class RepairResponse(BaseModel):
    pipeline: StreamETLDAG
    applied_actions: List[RepairAction]
    validation: ValidationReport


class ErrorDiagnosisRequest(BaseModel):
    error_message: Annotated[str, StringConstraints(strip_whitespace=True, min_length=3, max_length=30000)]
    pipeline: Optional[StreamETLDAG] = None
    node_id: Optional[str] = None
    use_ai_if_unknown: bool = True


class DiagnosisSource(str, Enum):
    RULE = "rule"
    AI = "ai"
    FALLBACK = "fallback"


class ErrorDiagnosis(BaseModel):
    category: str
    severity: Severity
    probable_node: Optional[str] = None
    explanation: str
    recommended_actions: List[str]
    source: DiagnosisSource
    rule_id: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)


class ErrorDiagnosisResponse(BaseModel):
    diagnosis: ErrorDiagnosis
