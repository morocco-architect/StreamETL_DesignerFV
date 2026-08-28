"""Console demonstration of the deterministic StreamETL Intelligence layer."""

from pprint import pprint

from intelligence.error_diagnosis import ErrorDiagnoser
from intelligence.schemas import ErrorDiagnosisRequest, StreamETLDAG
from intelligence.semantic_advisor import SemanticAdvisor


pipeline = StreamETLDAG.model_validate(
    {
        "nodes": [
            {"id": "orders", "type": "kafkaTopic", "data": {"label": "raw_orders", "params": {"readMode": "stream"}}},
            {"id": "users", "type": "kafkaTopic", "data": {"label": "users", "params": {"readMode": "stream"}}},
            {"id": "join", "type": "sparkTransform", "data": {"transformType": "Join", "params": {"joinKey": "user_id", "joinType": "inner"}}},
            {"id": "sink", "type": "kafkaTopic", "data": {"label": "enriched_orders", "params": {}}},
        ],
        "edges": [
            {"source": "orders", "target": "join"},
            {"source": "users", "target": "join"},
            {"source": "join", "target": "sink"},
        ],
    }
)

print("=== Semantic analysis ===")
pprint(SemanticAdvisor.analyze(pipeline).model_dump())

print("\n=== Rule-first error diagnosis ===")
pprint(
    ErrorDiagnoser.diagnose(
        ErrorDiagnosisRequest(
            error_message="AnalysisException: cannot resolve customer_id given input columns [user_id, amount]",
            pipeline=pipeline,
        )
    ).model_dump()
)
