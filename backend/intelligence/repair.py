from __future__ import annotations

from copy import deepcopy

from .schemas import (
    NodeType,
    RepairAction,
    RepairActionType,
    RepairRequest,
    RepairResponse,
    StreamETLDAG,
    TransformType,
)
from .semantic_advisor import SemanticAdvisor


class RepairError(ValueError):
    pass


class PipelineRepairer:
    """Deterministic, explicit, user-confirmed repair operations.

    No repair is inferred or applied silently. The caller must send concrete
    actions, which keeps the AI layer advisory and the transformation auditable.
    """

    @classmethod
    def repair(cls, request: RepairRequest) -> RepairResponse:
        pipeline = StreamETLDAG.model_validate(request.pipeline.model_dump())
        nodes = {node.id: node for node in pipeline.nodes}
        applied: list[RepairAction] = []

        for action in request.actions:
            node = nodes.get(action.node_id)
            if node is None:
                raise RepairError(f"Unknown node id {action.node_id!r}")

            if action.action == RepairActionType.SET_SOURCE_READ_MODE:
                if node.type != NodeType.KAFKA_TOPIC:
                    raise RepairError("set_source_read_mode applies only to kafkaTopic nodes")
                mode = str(action.params.get("readMode", "")).lower()
                if mode not in {"stream", "batch"}:
                    raise RepairError("readMode must be 'stream' or 'batch'")
                node.data.params["readMode"] = mode

            elif action.action == RepairActionType.ENABLE_GROUPBY_WINDOW:
                if not (
                    node.type == NodeType.SPARK_TRANSFORM
                    and node.data.transformType == TransformType.GROUP_BY
                ):
                    raise RepairError("enable_groupby_window applies only to GroupBy nodes")
                required = ("timestampCol", "windowDuration", "watermarkDelay")
                missing = [k for k in required if not str(action.params.get(k, "")).strip()]
                if missing:
                    raise RepairError(
                        "enable_groupby_window requires: " + ", ".join(missing)
                    )
                node.data.params.update(
                    {
                        "windowEnabled": True,
                        "timestampCol": action.params["timestampCol"],
                        "windowDuration": action.params["windowDuration"],
                        "watermarkDelay": action.params["watermarkDelay"],
                        "windowSlide": action.params.get("windowSlide", ""),
                    }
                )

            elif action.action == RepairActionType.SET_JOIN_WATERMARKS:
                if not (
                    node.type == NodeType.SPARK_TRANSFORM
                    and node.data.transformType == TransformType.JOIN
                ):
                    raise RepairError("set_join_watermarks applies only to Join nodes")
                required = (
                    "leftWatermarkCol",
                    "leftWatermarkDelay",
                    "rightWatermarkCol",
                    "rightWatermarkDelay",
                )
                missing = [k for k in required if not str(action.params.get(k, "")).strip()]
                if missing:
                    raise RepairError(
                        "set_join_watermarks requires: " + ", ".join(missing)
                    )
                node.data.params["enableWatermarks"] = True
                for key in required:
                    node.data.params[key] = action.params[key]

            elif action.action == RepairActionType.SET_JOIN_TIME_BOUND:
                if not (
                    node.type == NodeType.SPARK_TRANSFORM
                    and node.data.transformType == TransformType.JOIN
                ):
                    raise RepairError("set_join_time_bound applies only to Join nodes")
                before = str(action.params.get("timeRangeBefore", "")).strip()
                after = str(action.params.get("timeRangeAfter", "")).strip()
                if not before or not after:
                    raise RepairError(
                        "set_join_time_bound requires timeRangeBefore and timeRangeAfter"
                    )
                node.data.params.update(
                    {
                        "timeBoundEnabled": True,
                        "timeRangeBefore": before,
                        "timeRangeAfter": after,
                    }
                )

            elif action.action == RepairActionType.DISABLE_AGGREGATE_DEDUP:
                if not (
                    node.type == NodeType.SPARK_TRANSFORM
                    and node.data.transformType == TransformType.AGGREGATE
                ):
                    raise RepairError("disable_aggregate_dedup applies only to Aggregate nodes")
                node.data.params["dedup_before"] = False

            else:  # pragma: no cover - guarded by enum validation
                raise RepairError(f"Unsupported repair action {action.action}")

            applied.append(action)

        # Re-validate graph contract and semantics after all modifications.
        repaired = StreamETLDAG.model_validate(pipeline.model_dump())
        report = SemanticAdvisor.analyze(repaired)
        return RepairResponse(
            pipeline=repaired,
            applied_actions=applied,
            validation=report,
        )
