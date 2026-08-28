from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Optional

from .prompts import ERROR_DIAGNOSIS_SYSTEM_PROMPT
from .providers import ProviderError, get_ai_provider
from .schemas import (
    DiagnosisSource,
    ErrorDiagnosis,
    ErrorDiagnosisRequest,
    Severity,
    TransformType,
)


@dataclass(frozen=True)
class _Rule:
    rule_id: str
    category: str
    severity: Severity
    pattern: re.Pattern[str]
    explanation: str
    actions: tuple[str, ...]


_RULES = (
    _Rule(
        "SPARK_OUTPUT_MODE_APPEND_AGG",
        "OUTPUT_MODE",
        Severity.ERROR,
        re.compile(r"append output mode.*(not supported|streaming aggregation)|streaming aggregations?.*append", re.I | re.S),
        "Spark rejected append mode for a streaming aggregation whose results may still change.",
        (
            "Use the StreamETL-generated update mode for an unwindowed aggregation.",
            "If append semantics are required, use an event-time window and watermark configuration supported by Spark.",
        ),
    ),
    _Rule(
        "SPARK_UNRESOLVED_COLUMN",
        "SCHEMA_COLUMN",
        Severity.ERROR,
        re.compile(r"(cannot resolve|unresolved[_ ]column|cannot be resolved).*(column|given input columns|attributes?)", re.I | re.S),
        "Spark could not resolve a referenced column against the schema available at that point in the pipeline.",
        (
            "Inspect the output schema of the preceding ParseJSON or transformation node.",
            "Verify column spelling, aliases and join keys before regenerating the pipeline.",
        ),
    ),
    _Rule(
        "SPARK_WATERMARK_EVENT_TIME",
        "WATERMARK",
        Severity.ERROR,
        re.compile(r"watermark.*(timestamp|event.?time|delay)|event.?time.*watermark", re.I | re.S),
        "The failure is related to event-time/watermark configuration.",
        (
            "Verify that the configured event-time column exists after ParseJSON.",
            "Ensure the event-time column is a Spark timestamp and the watermark delay is valid.",
        ),
    ),
    _Rule(
        "SPARK_CHECKPOINT_INCOMPATIBLE",
        "CHECKPOINT",
        Severity.ERROR,
        re.compile(r"checkpoint.*(incompat|recover|offset|state schema|different query)|state schema.*checkpoint", re.I | re.S),
        "Spark could not safely reuse the existing checkpoint/state for the current streaming query.",
        (
            "Use a fresh checkpoint location when the query topology or state schema changes.",
            "Preserve durable checkpoints only for compatible restarts of the same logical query.",
        ),
    ),
    _Rule(
        "SPARK_STATE_STORE",
        "STATE_STORE",
        Severity.ERROR,
        re.compile(r"state(store| store)|rocksdbstate|hdfsbackedstate", re.I),
        "The error originates from Spark Structured Streaming state management.",
        (
            "Inspect preceding aggregations, deduplication and stream-stream joins for unbounded state.",
            "Check checkpoint storage integrity and state-store resource pressure.",
        ),
    ),
    _Rule(
        "KAFKA_BROKER_UNAVAILABLE",
        "KAFKA_CONNECTIVITY",
        Severity.ERROR,
        re.compile(r"nobrokersavailable|bootstrap broker.*disconnected|failed to update metadata|connection to node .* could not be established", re.I),
        "The Spark/Kafka client could not establish a usable broker connection.",
        (
            "Verify Kafka is running and that the bootstrap address is reachable from the Spark container.",
            "Check the internal Docker listener (for this project, typically kafka:29092) and topic existence.",
        ),
    ),
    _Rule(
        "SPARK_ANALYSIS_EXCEPTION",
        "ANALYSIS_EXCEPTION",
        Severity.ERROR,
        re.compile(r"analysisexception", re.I),
        "Spark raised an AnalysisException while analysing the logical query plan.",
        (
            "Read the first AnalysisException message and inspect the node whose schema or operation it references.",
            "Validate the DAG with /analyze before regenerating the PySpark job.",
        ),
    ),
    _Rule(
        "JAVA_NULL_POINTER",
        "NULL_POINTER",
        Severity.ERROR,
        re.compile(r"nullpointerexception", re.I),
        "A NullPointerException occurred, but the exception text alone is insufficient to infer that a specific data field was null.",
        (
            "Inspect the closest Java/PySpark stack frames and the surrounding Spark error message.",
            "Check recent schema, UDF, join and connector operations rather than assuming a single root cause.",
        ),
    ),
)


class ErrorDiagnoser:
    @classmethod
    def diagnose(cls, request: ErrorDiagnosisRequest) -> ErrorDiagnosis:
        message = request.error_message
        probable_node = request.node_id or cls._guess_probable_node(message, request)

        for rule in _RULES:
            if rule.pattern.search(message):
                return ErrorDiagnosis(
                    category=rule.category,
                    severity=rule.severity,
                    probable_node=probable_node,
                    explanation=rule.explanation,
                    recommended_actions=list(rule.actions),
                    source=DiagnosisSource.RULE,
                    rule_id=rule.rule_id,
                    confidence=0.9,
                )

        if request.use_ai_if_unknown:
            provider = get_ai_provider()
            if provider.enabled:
                try:
                    context = _compact_pipeline_context(request)
                    result = provider.complete_json(
                        system_prompt=ERROR_DIAGNOSIS_SYSTEM_PROMPT,
                        user_prompt=f"Error:\n{message}\n\nPipeline context:\n{context}",
                    )
                    explanation = str(result.get("explanation", "")).strip()
                    actions = result.get("recommended_actions", [])
                    if explanation and isinstance(actions, list):
                        return ErrorDiagnosis(
                            category="UNCLASSIFIED_SPARK_ERROR",
                            severity=Severity.ERROR,
                            probable_node=probable_node,
                            explanation=explanation,
                            recommended_actions=[str(a) for a in actions[:5]],
                            source=DiagnosisSource.AI,
                            confidence=0.55,
                        )
                except ProviderError:
                    # Diagnosis must remain available even if the optional LLM is down.
                    pass

        return ErrorDiagnosis(
            category="UNCLASSIFIED_SPARK_ERROR",
            severity=Severity.ERROR,
            probable_node=probable_node,
            explanation="The deterministic rule engine could not classify this Spark error from the supplied message alone.",
            recommended_actions=[
                "Inspect the earliest root-cause exception in the full Spark stderr/log rather than the final wrapper exception.",
                "Run /api/intelligence/analyze on the DAG and compare the failing operation with the generated pipeline_job.py.",
            ],
            source=DiagnosisSource.FALLBACK,
            confidence=0.25,
        )

    @staticmethod
    def _guess_probable_node(message: str, request: ErrorDiagnosisRequest) -> Optional[str]:
        if request.pipeline is None:
            return None
        lowered = message.lower()
        # Strongest signal: an actual node id present in the error/log text.
        for node in request.pipeline.nodes:
            if node.id.lower() in lowered:
                return node.id

        # Weaker but useful signal: a transform name explicitly mentioned.
        for node in request.pipeline.nodes:
            transform = node.data.transformType
            if transform and transform.value.lower() in lowered:
                return node.id
        return None


def _compact_pipeline_context(request: ErrorDiagnosisRequest) -> str:
    if request.pipeline is None:
        return "(not provided)"
    parts: list[str] = []
    for node in request.pipeline.nodes[:50]:
        detail = node.type.value
        if node.data.transformType:
            detail += f"/{node.data.transformType.value}"
        elif node.data.label:
            detail += f"/{node.data.label}"
        parts.append(f"{node.id}:{detail}")
    return ", ".join(parts)
