from __future__ import annotations

from typing import Any, Dict

from pydantic import ValidationError

from .prompts import PIPELINE_BUILDER_SYSTEM_PROMPT
from .providers import AIProvider, get_ai_provider
from .schemas import PipelineSuggestion, Position
from .semantic_advisor import SemanticAdvisor


class PipelineAssistant:
    @staticmethod
    def generate_dag_from_prompt(prompt: str, provider: AIProvider | None = None):
        provider = provider or get_ai_provider()
        raw: Dict[str, Any] = provider.complete_json(
            system_prompt=PIPELINE_BUILDER_SYSTEM_PROMPT,
            user_prompt=prompt,
        )

        try:
            suggestion = PipelineSuggestion.model_validate(raw)
        except ValidationError:
            # Let FastAPI/router expose the actual Pydantic detail as a 422.
            raise

        suggestion = _assign_deterministic_ui_layout(suggestion)
        validation = SemanticAdvisor.analyze(suggestion)
        return suggestion, validation, provider.info


def _assign_deterministic_ui_layout(pipeline: PipelineSuggestion) -> PipelineSuggestion:
    """Give LLM-created nodes stable React Flow positions and edge ids.

    The layout is intentionally simple and deterministic. The frontend can later
    replace it with Dagre/ELK without changing the API contract.
    """

    # Build topological depth using the already validated DAG.
    parents: Dict[str, list[str]] = {node.id: [] for node in pipeline.nodes}
    for edge in pipeline.edges:
        parents[edge.target].append(edge.source)

    depth: Dict[str, int] = {}

    def get_depth(node_id: str) -> int:
        if node_id in depth:
            return depth[node_id]
        if not parents[node_id]:
            depth[node_id] = 0
        else:
            depth[node_id] = 1 + max(get_depth(parent) for parent in parents[node_id])
        return depth[node_id]

    rows: Dict[int, int] = {}
    for node in pipeline.nodes:
        d = get_depth(node.id)
        row = rows.get(d, 0)
        rows[d] = row + 1
        if node.position is None:
            node.position = Position(x=80 + d * 260, y=80 + row * 150)

    for index, edge in enumerate(pipeline.edges, start=1):
        if not edge.id:
            edge.id = f"e-{edge.source}-{edge.target}-{index}"

    return pipeline
