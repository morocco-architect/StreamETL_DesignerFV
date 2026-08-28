"""
StreamETL Designer — Backend API
FastAPI server that connects to Kafka and exposes topic management endpoints.

Release-candidate architecture notes:
- Removed the unused `import docker` (the Docker SDK was never actually
  used — job execution goes through `docker exec` via subprocess).
- All previously hardcoded values (Kafka hosts, Spark container name,
  Spark master URL, Kafka package coordinates) now come from config.py so
  they are defined once and can be overridden with environment variables.
- Table creation at startup no longer crashes the whole API if Postgres is
  unreachable; it logs a warning instead so Kafka/Spark endpoints keep
  working even when the database is down.
- Mounted the StreamETL Intelligence Layer (semantic validation, error
  diagnosis, NL-to-DAG) under /api/intelligence.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import logging
import json
import re
from sqlalchemy.orm import Session
from fastapi import Depends

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INTERNAL_BOOTSTRAP_SERVERS,
    SPARK_MASTER_CONTAINER,
    SPARK_MASTER_URL,
    SPARK_KAFKA_PACKAGE,
    CORS_ALLOWED_ORIGINS,
)
from database import engine, Base, get_db
from models import Pipeline
from intelligence.router import router as intelligence_router
from intelligence.schemas import Severity, StreamETLDAG
from intelligence.semantic_advisor import SemanticAdvisor

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
logger = logging.getLogger("streametl")
logging.basicConfig(level=logging.INFO)

# Création des tables dans la base de données.
# Ne bloque plus le démarrage de l'API si Postgres n'est pas encore prêt
# (ex: `docker compose` pas encore lancé) — seules les routes /pipeline/*
# liées à la persistance seront indisponibles (503), le reste de l'API
# (topics Kafka, génération/exécution PySpark) continue de fonctionner.
try:
    Base.metadata.create_all(bind=engine)
except Exception as exc:  # noqa: BLE001 - startup must not crash the API
    logger.warning(
        "Impossible d'initialiser la base de données PostgreSQL au démarrage "
        "(%s). Les endpoints /api/pipeline/save|history|latest renverront "
        "une erreur 503 tant que la base n'est pas accessible.",
        exc,
    )

app = FastAPI(
    title="StreamETL Designer API",
    description="Backend API for the StreamETL low-code pipeline designer",
    version="1.2.0-rc1",
)

# CORS — allow React dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# StreamETL Intelligence Layer: semantic DAG validation, Spark error
# diagnosis, and natural-language-to-pipeline generation.
app.include_router(intelligence_router, prefix="/api/intelligence", tags=["intelligence"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_admin_client() -> KafkaAdminClient:
    """Create a short-lived admin client for each request."""
    try:
        return KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id="streametl-backend",
            api_version=(2, 8, 1),
        )
    except NoBrokersAvailable:
        raise HTTPException(
            status_code=503,
            detail="Impossible de se connecter à Kafka. Vérifiez que le broker est en cours d'exécution.",
        )


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class TopicCreate(BaseModel):
    name: str
    num_partitions: int = 1
    replication_factor: int = 1


class TopicInfo(BaseModel):
    name: str
    partitions: int


class PipelineSave(BaseModel):
    name: Optional[str] = None
    nodes: list
    edges: list


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {"message": "StreamETL Designer API is running 🚀"}


@app.get("/api/pipeline/latest")
def get_latest_pipeline(db: Session = Depends(get_db)):
    """Retourne le dernier pipeline sauvegardé dans la base de données."""
    pipeline = db.query(Pipeline).order_by(Pipeline.id.desc()).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Aucun pipeline sauvegardé.")
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "nodes": pipeline.nodes,
        "edges": pipeline.edges,
        "created_at": pipeline.created_at.isoformat() if pipeline.created_at else None,
    }


@app.post("/api/pipeline/save")
def save_pipeline(payload: PipelineSave, db: Session = Depends(get_db)):
    """Sauvegarde un pipeline dans la base de données PostgreSQL."""
    from datetime import datetime
    name = payload.name or f"Pipeline_{datetime.now().strftime('%Y-%m-%d_%H:%M')}"

    # Sérialiser les nodes en supprimant les fonctions (callbacks)
    clean_nodes = []
    for n in payload.nodes:
        node = dict(n) if isinstance(n, dict) else n
        if "data" in node and isinstance(node["data"], dict):
            node["data"] = {k: v for k, v in node["data"].items() if not callable(v)}
        clean_nodes.append(node)

    new_pipeline = Pipeline(
        name=name,
        nodes=clean_nodes,
        edges=payload.edges
    )
    db.add(new_pipeline)
    db.commit()
    db.refresh(new_pipeline)
    logger.info(f"Pipeline sauvegardé: {new_pipeline.id} - {name}")

    return {
        "id": new_pipeline.id,
        "name": new_pipeline.name,
        "created_at": new_pipeline.created_at.isoformat() if new_pipeline.created_at else None,
        "message": "Pipeline sauvegardé avec succès !"
    }


@app.get("/api/pipeline/history")
def get_pipeline_history(db: Session = Depends(get_db)):
    """Retourne la liste de tous les pipelines sauvegardés."""
    pipelines = db.query(Pipeline).order_by(Pipeline.id.desc()).all()
    return [
        {
            "id": p.id,
            "name": p.name or f"Pipeline #{p.id}",
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "node_count": len(p.nodes) if p.nodes else 0,
            "edge_count": len(p.edges) if p.edges else 0,
        }
        for p in pipelines
    ]


@app.get("/api/pipeline/{pipeline_id}")
def get_pipeline_by_id(pipeline_id: int, db: Session = Depends(get_db)):
    """Retourne un pipeline complet par son ID."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail=f"Pipeline #{pipeline_id} introuvable.")
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "nodes": pipeline.nodes,
        "edges": pipeline.edges,
        "created_at": pipeline.created_at.isoformat() if pipeline.created_at else None,
    }


@app.get("/api/topics", response_model=list[TopicInfo])
def list_topics():
    """Return every topic visible on the Kafka cluster."""
    admin = _get_admin_client()
    try:
        metadata = admin.describe_topics()
        topics: list[TopicInfo] = []
        for t in metadata:
            topic_name = t["topic"]
            # Skip Kafka internal topics (start with __)
            if topic_name.startswith("__"):
                continue
            topics.append(
                TopicInfo(
                    name=topic_name,
                    partitions=len(t["partitions"]),
                )
            )
        return topics
    finally:
        admin.close()


@app.post("/api/topics", response_model=TopicInfo, status_code=201)
def create_topic(payload: TopicCreate):
    """Create a new Kafka topic."""
    admin = _get_admin_client()
    try:
        new_topic = NewTopic(
            name=payload.name,
            num_partitions=payload.num_partitions,
            replication_factor=payload.replication_factor,
        )
        admin.create_topics(new_topics=[new_topic], validate_only=False)
        return TopicInfo(name=payload.name, partitions=payload.num_partitions)
    except TopicAlreadyExistsError:
        raise HTTPException(
            status_code=409,
            detail=f"Le topic '{payload.name}' existe déjà.",
        )
    finally:
        admin.close()


@app.delete("/api/topics/{topic_name}", status_code=204)
def delete_topic(topic_name: str):
    """Delete a Kafka topic."""
    admin = _get_admin_client()
    try:
        admin.delete_topics(topics=[topic_name])
    except Exception as e:
        logger.error(f"Error deleting topic '{topic_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la suppression du topic : {str(e)}",
        )
    finally:
        admin.close()


@app.get("/api/topics/{topic_name}/preview")
def get_topic_preview(topic_name: str, limit: int = 5):
    """Peek at the latest messages in a Kafka topic."""
    try:
        # On se connecte en tant que consommateur temporaire
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=1000, # On attend max 1s pour éviter de bloquer
            api_version=(2, 8, 1),
        )

        # On récupère les partitions
        partitions = consumer.partitions_for_topic(topic_name)
        if partitions is None:
            return []

        topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
        consumer.assign(topic_partitions)

        # On va à la fin de chaque partition pour lire les derniers messages
        end_offsets = consumer.end_offsets(topic_partitions)

        messages = []
        for tp in topic_partitions:
            end_offset = end_offsets[tp]
            if end_offset == 0: continue

            start_offset = max(0, end_offset - limit)
            consumer.seek(tp, start_offset)

            # On lit les messages
            count = 0
            for msg in consumer:
                if msg.value:
                    messages.append(msg.value)
                count += 1
                if count >= limit:
                    break

        consumer.close()
        # Retourne les messages les plus récents (triés par ordre inverse de réception pour l'UI)
        return messages[-limit:][::-1]
    except Exception as e:
        logger.error(f"Error previewing topic {topic_name}: {e}")
        return []


# ---------------------------------------------------------------------------
# Deterministic code-generation safety helpers
# ---------------------------------------------------------------------------
_IDENTIFIER_RE = re.compile(r"[^0-9A-Za-z_]+")
_ALLOWED_AGG_FUNCS = {
    "count", "sum", "avg", "min", "max", "collect_list", "collect_set",
    "first", "last", "stddev", "variance",
}
_ALLOWED_JOIN_TYPES = {
    "inner", "left", "right", "full", "outer", "left_outer",
    "right_outer", "full_outer", "semi", "anti", "cross",
}
_DURATION_RE = re.compile(r"^\s*(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days)\s*$", re.I)


def _safe_identifier(value: str, prefix: str = "v") -> str:
    """Return a valid Python identifier derived from user-controlled labels.

    User labels remain unchanged in Spark/Kafka string options; only generated
    Python variable names are normalised.
    """
    cleaned = _IDENTIFIER_RE.sub("_", str(value)).strip("_") or prefix
    cleaned = re.sub(r"_+", "_", cleaned)
    if cleaned[0].isdigit():
        cleaned = f"{prefix}_{cleaned}"
    return cleaned[:100]


def _py_literal(value: object) -> str:
    """Render data as a Python string/value literal without executable interpolation."""
    return repr(value)


def _comment_text(value: object, max_length: int = 160) -> str:
    """Make user-controlled text safe for generated one-line comments."""
    text = str(value).replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_length]


def _normalise_interval(value: str, *, field_name: str) -> str:
    """Validate a bounded event-time interval and return Spark SQL INTERVAL text."""
    match = _DURATION_RE.fullmatch(str(value or ""))
    if not match:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Validation Error: {field_name} must look like '10 minutes', "
                "'30 seconds', '2 hours', or '1 day'."
            ),
        )
    amount, unit = match.groups()
    unit = unit.upper()
    return f"INTERVAL {amount} {unit}"


def _preflight_pipeline(payload: dict):
    """Validate native DAG structure and deterministic semantic rules before compilation."""
    try:
        dag = StreamETLDAG.model_validate(payload)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid StreamETL DAG: {exc}") from exc

    report = SemanticAdvisor.analyze(dag)
    errors = [issue for issue in report.issues if issue.severity == Severity.ERROR]
    if errors:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Pipeline rejected by deterministic semantic validation.",
                "issues": [issue.model_dump(mode="json") for issue in errors],
            },
        )
    return dag, report


# ---------------------------------------------------------------------------
# Code Generator
# ---------------------------------------------------------------------------
@app.post("/api/pipeline/generate")
def generate_pipeline(payload: dict):
    """Translate a validated native StreamETL DAG into inspectable PySpark code."""
    dag, validation_report = _preflight_pipeline(payload)
    native_payload = dag.model_dump(mode="json")
    nodes = native_payload.get("nodes", [])
    edges = native_payload.get("edges", [])

    logger.info("Nouveau pipeline reçu, validation réussie; génération du code PySpark...")

    code = [
        "from pyspark.sql import SparkSession",
        "from pyspark.sql.functions import *",
        "",
        "spark = SparkSession.builder \\",
        "    .appName('StreamETL_App') \\",
        f"    .master('{SPARK_MASTER_URL}') \\",
        f"    .config('spark.jars.packages', '{SPARK_KAFKA_PACKAGE}') \\",
        "    .getOrCreate()",
        ""
    ]

    node_map = {n["id"]: n for n in nodes}

    # Build adjacency: for each node, what are its outgoing targets?
    outgoing = {}  # node_id -> [target_node_id, ...]
    incoming = {}  # node_id -> [source_node_id, ...]
    for e in edges:
        outgoing.setdefault(e["source"], []).append(e["target"])
        incoming.setdefault(e["target"], []).append(e["source"])

    # ── Phase 1: Topological sort (Kahn's algorithm) ──
    # Process nodes in dependency order so that when we reach a Join,
    # both of its input branches have already been processed.
    in_degree = {n["id"]: 0 for n in nodes}
    for e in edges:
        in_degree[e["target"]] = in_degree.get(e["target"], 0) + 1

    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    topo_order = []
    while queue:
        nid = queue.pop(0)
        topo_order.append(nid)
        for target in outgoing.get(nid, []):
            in_degree[target] -= 1
            if in_degree[target] == 0:
                queue.append(target)

    if len(topo_order) != len(nodes):
        raise HTTPException(
            status_code=400,
            detail="Validation Error: the pipeline graph contains a cycle. "
            "A directed acyclic graph (DAG) is required.",
        )

    # ── Phase 2: Generate code following topological order ──
    # Track the DataFrame variable name produced by each node.
    node_df_var = {}   # node_id -> variable name of the DataFrame it outputs
    node_has_agg = {}  # node_id -> whether the path includes an aggregation
    node_is_streaming = {} # node_id -> whether the DataFrame is streaming
    # True means the branch can legally terminate in an Append-only file sink.
    # A non-windowed streaming aggregation sets it to False; a directly
    # windowed+watermarked aggregation can emit finalized rows in Append mode.
    node_append_safe = {}
    pending_groupby = {}  # node_id -> groupBy/window config (consumed by Aggregate)
    step_counter = [0]  # mutable counter for step numbering
    sink_queries = []   # list of query variable names for awaitAnyTermination

    def next_step():
        step_counter[0] += 1
        return step_counter[0]

    for nid in topo_order:
        node = node_map[nid]

        # ── Kafka Source (no incoming edges = source) ──
        if node["type"] == "kafkaTopic" and nid not in incoming:
            topic_name = node["data"]["label"]
            topic_ident = _safe_identifier(topic_name, "topic")
            df_var = f"df_{topic_ident}"
            step = next_step()
            read_mode = node["data"].get("params", {}).get("readMode", "stream")

            if read_mode == "batch":
                # Read as BATCH (static) — used for stream-static joins
                code.append(f"# {step}. Read from Kafka Source (BATCH): {_comment_text(topic_name)}")
                code.append(f"{df_var} = spark.read \\")
                code.append("    .format('kafka') \\")
                code.append(f"    .option('kafka.bootstrap.servers', {_py_literal(KAFKA_INTERNAL_BOOTSTRAP_SERVERS)}) \\")
                code.append(f"    .option('subscribe', {_py_literal(topic_name)}) \\")
                code.append("    .load()\n")
                node_is_streaming[nid] = False
            else:
                # Read as STREAM (default)
                starting_offset = node["data"].get("params", {}).get("startingOffset", "earliest")
                group_id = node["data"].get("params", {}).get("consumerGroupId", "default-group")
                code.append(f"# {step}. Read from Kafka Source: {_comment_text(topic_name)}")
                code.append(f"{df_var} = spark.readStream \\")
                code.append("    .format('kafka') \\")
                code.append(f"    .option('kafka.bootstrap.servers', {_py_literal(KAFKA_INTERNAL_BOOTSTRAP_SERVERS)}) \\")
                code.append(f"    .option('subscribe', {_py_literal(topic_name)}) \\")
                code.append(f"    .option('startingOffsets', {_py_literal(starting_offset)}) \\")
                code.append(f"    .option('kafka.group.id', {_py_literal(group_id)}) \\")
                code.append("    .load()\n")
                node_is_streaming[nid] = True
            node_df_var[nid] = df_var
            node_has_agg[nid] = False
            node_append_safe[nid] = True

        # ── Kafka Sink (has incoming edges = sink) ──
        elif node["type"] == "kafkaTopic" and nid in incoming:
            source_nid = incoming[nid][0]
            current_df = node_df_var.get(source_nid, "UNKNOWN_DF")
            has_agg = node_has_agg.get(source_nid, False)
            sink_topic = node["data"]["label"]
            sink_ident = _safe_identifier(sink_topic, "sink")
            acks_policy = str(node["data"].get("params", {}).get("acksPolicy", "all"))
            step = next_step()
            code.append(f"# {step}. Write to Kafka Sink: {_comment_text(sink_topic)}")
            sink_df_var = f"sink_df_{sink_ident}"
            code.append(f"{sink_df_var} = {current_df}.select(to_json(struct('*')).alias('value'))")
            query_var = f"query_{sink_ident}"
            code.append(f"{query_var} = {sink_df_var}.writeStream \\")
            code.append("    .format('kafka') \\")
            if has_agg:
                code.append("    .outputMode('update') \\")
            else:
                code.append("    .outputMode('append') \\")
            code.append(f"    .option('kafka.bootstrap.servers', {_py_literal(KAFKA_INTERNAL_BOOTSTRAP_SERVERS)}) \\")
            code.append(f"    .option('topic', {_py_literal(sink_topic)}) \\")
            checkpoint = f"/tmp/checkpoint_{sink_ident}"
            code.append(f"    .option('checkpointLocation', {_py_literal(checkpoint)}) \\")
            code.append(f"    .option('kafka.acks', {_py_literal(acks_policy)}) \\")
            code.append("    .start()\n")
            sink_queries.append(query_var)
            node_df_var[nid] = sink_df_var
            node_has_agg[nid] = has_agg
            node_is_streaming[nid] = node_is_streaming.get(source_nid, True)
            node_append_safe[nid] = node_append_safe.get(source_nid, True)

        # ── Parquet Sink ──
        elif node["type"] == "parquetSink":
            source_nid = incoming.get(nid, [None])[0]
            current_df = node_df_var.get(source_nid, "UNKNOWN_DF")
            has_agg = node_has_agg.get(source_nid, False)
            append_safe = node_append_safe.get(source_nid, True)
            path = node["data"].get("path", "/tmp/parquet_output")
            if has_agg and not append_safe:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Validation Error: Spark file/Parquet sinks are Append-only. "
                        "This branch contains a streaming aggregation that is not "
                        "windowed+watermarked, so finalized append output cannot be guaranteed."
                    ),
                )
            step = next_step()
            code.append(f"# {step}. Write to Parquet Sink: {_comment_text(path)}")
            query_var = f"query_parquet_{_safe_identifier(nid, 'sink')}"
            code.append(f"{query_var} = {current_df}.writeStream \\")
            code.append("    .format('parquet') \\")
            code.append("    .outputMode('append') \\")
            code.append(f"    .option('path', {_py_literal(path)}) \\")
            checkpoint = f"/tmp/checkpoint_parquet_{_safe_identifier(nid, 'sink')}"
            code.append(f"    .option('checkpointLocation', {_py_literal(checkpoint)}) \\")
            code.append("    .start()\n")
            sink_queries.append(query_var)
            node_df_var[nid] = current_df
            node_has_agg[nid] = has_agg
            node_is_streaming[nid] = node_is_streaming.get(source_nid, True)
            node_append_safe[nid] = append_safe

        # ── Spark Transforms ──
        elif node["type"] == "sparkTransform":
            t_type = node["data"]["transformType"]
            params = node["data"].get("params", {})
            source_nid = incoming.get(nid, [None])[0]
            current_df = node_df_var.get(source_nid, "UNKNOWN_DF")
            has_agg = node_has_agg.get(source_nid, False)
            append_safe = node_append_safe.get(source_nid, True)
            step = next_step()
            new_df = f"df_step{step}"

            code.append(f"# {step}. Transformation: {t_type}")

            if t_type == "Filter":
                cond = str(params.get("condition", "True"))
                code.append(f"{new_df} = {current_df}.filter({_py_literal(cond)})")
                node_is_streaming[nid] = node_is_streaming.get(source_nid, True)

            elif t_type == "ParseJSON":
                schema = str(params.get("schema", "STRING"))
                code.append(f"{new_df} = {current_df}.selectExpr('CAST(value AS STRING)') \\")
                code.append(f"    .select(from_json(col('value'), {_py_literal(schema)}).alias('data')) \\")
                code.append("    .select('data.*')")
                node_is_streaming[nid] = node_is_streaming.get(source_nid, True)

            elif t_type == "GroupBy":
                cols = params.get("columns", "")
                col_list = [c.strip() for c in cols.split(",") if c.strip()]
                has_agg = True
                window_enabled = params.get("windowEnabled", False)

                # Store groupBy cols and window config; the next Aggregate will consume them
                pending_groupby[nid] = {
                    "cols": col_list,
                    "windowEnabled": window_enabled,
                    "windowDuration": params.get("windowDuration", ""),
                    "windowSlide": params.get("windowSlide", ""),
                    "timestampCol": params.get("timestampCol", ""),
                    "watermarkDelay": params.get("watermarkDelay", ""),
                }

                if window_enabled:
                    ts_col = params.get("timestampCol", "")
                    watermark = params.get("watermarkDelay", "")
                    duration = params.get("windowDuration", "")
                    slide = params.get("windowSlide", "")

                    if watermark and ts_col:
                        code.append(f"{new_df} = {current_df}.withWatermark({_py_literal(ts_col)}, {_py_literal(watermark)})")
                        current_df = new_df

                    if duration and ts_col:
                        code.append(f"# (GroupBy on [{_comment_text(', '.join(col_list))}] with window — combined with next Aggregate)")
                    else:
                        code.append(f"# (GroupBy on [{_comment_text(', '.join(col_list))}] — combined with next Aggregate)")
                else:
                    code.append(f"# (GroupBy on [{_comment_text(', '.join(col_list))}] — combined with next Aggregate)")

                # Pass through the same df variable (groupby alone is not valid)
                new_df = current_df
                node_is_streaming[nid] = node_is_streaming.get(source_nid, True)

            elif t_type == "Aggregate":
                has_agg = True
                dedup_before = params.get("dedup_before", False)
                aggregations = params.get("aggregations", [{"func": "count", "col": "*", "alias": ""}])

                # Optional dedup
                if dedup_before:
                    is_streaming = node_is_streaming.get(source_nid, True)
                    gb_config = pending_groupby.get(source_nid, {})
                    has_watermark = bool(gb_config and gb_config.get("windowEnabled") and gb_config.get("watermarkDelay") and gb_config.get("timestampCol"))

                    if is_streaming and not has_watermark:
                        raise HTTPException(
                            status_code=400,
                            detail="Validation Error: 'Deduplicate before' requires a Watermark in Streaming mode. Please check 'Enable window' and set a 'Watermark delay' in the preceding GroupBy node, or uncheck Deduplicate."
                        )

                    dedup_df = f"df_dedup_step{step}"
                    code.append(f"{dedup_df} = {current_df}.dropDuplicates()")
                    current_df = dedup_df

                # Build aggregation expressions from a closed function vocabulary.
                agg_exprs = []
                for a in aggregations:
                    func = str(a.get("func", "count")).strip().lower()
                    col = str(a.get("col", "*"))
                    alias = str(a.get("alias", ""))
                    if func not in _ALLOWED_AGG_FUNCS:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Validation Error: unsupported aggregation function {func!r}.",
                        )

                    col_arg = "'*'" if col == "*" or not col else _py_literal(col)
                    expr_str = f"{func}({col_arg})"
                    if alias:
                        expr_str += f".alias({_py_literal(alias)})"
                    agg_exprs.append(expr_str)

                agg_args = ", ".join(agg_exprs)

                # Check if the source node was a GroupBy
                gb_config = pending_groupby.pop(source_nid, None)
                if gb_config:
                    gb_cols = gb_config["cols"]
                    gb_args_list = []
                    for c in gb_cols:
                        if any(char in c for char in ['>', '<', '=', ' ', '(', ')']):
                            gb_args_list.append(f"expr({_py_literal(c)})")
                        else:
                            gb_args_list.append(_py_literal(c))
                    gb_args = ", ".join(gb_args_list)

                    if gb_config.get("windowEnabled") and gb_config.get("windowDuration") and gb_config.get("timestampCol"):
                        ts_col = gb_config["timestampCol"]
                        duration = gb_config["windowDuration"]
                        slide = gb_config.get("windowSlide", "")
                        if slide:
                            window_expr = f"window({_py_literal(ts_col)}, {_py_literal(duration)}, {_py_literal(slide)})"
                        else:
                            window_expr = f"window({_py_literal(ts_col)}, {_py_literal(duration)})"
                        code.append(f"{new_df} = {current_df}.groupBy({gb_args}, {window_expr}).agg({agg_args})")
                    else:
                        code.append(f"{new_df} = {current_df}.groupBy({gb_args}).agg({agg_args})")
                else:
                    code.append(f"{new_df} = {current_df}.agg({agg_args})")

                node_is_streaming[nid] = node_is_streaming.get(source_nid, True)
                if node_is_streaming[nid]:
                    append_safe = bool(
                        gb_config
                        and gb_config.get("windowEnabled")
                        and str(gb_config.get("windowDuration", "")).strip()
                        and str(gb_config.get("timestampCol", "")).strip()
                        and str(gb_config.get("watermarkDelay", "")).strip()
                    )
                else:
                    append_safe = True

            elif t_type == "Join":
                join_key = str(params.get("joinKey", "id")).strip()
                right_key = str(params.get("rightKey", "")).strip()
                join_type = str(params.get("joinType", "inner")).strip().lower()
                if join_type not in _ALLOWED_JOIN_TYPES:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Validation Error: unsupported join type {join_type!r}.",
                    )
                broadcast_hint = bool(params.get("broadcast", False))
                right_df_param = str(params.get("rightDf", "")).strip()

                # Join has TWO incoming edges under the native StreamETL contract.
                join_sources = incoming.get(nid, [])
                has_agg = any(node_has_agg.get(src, False) for src in join_sources) or has_agg
                append_safe = all(node_append_safe.get(src, True) for src in join_sources)

                if len(join_sources) >= 2:
                    left_df = node_df_var.get(join_sources[0], "UNKNOWN_DF")
                    right_df = node_df_var.get(join_sources[1], "UNKNOWN_DF")
                    left_is_stream = node_is_streaming.get(join_sources[0], True)
                    right_is_stream = node_is_streaming.get(join_sources[1], True)
                elif len(join_sources) == 1:
                    # Kept only for defensive compatibility; semantic preflight rejects this arity.
                    left_df = node_df_var.get(join_sources[0], "UNKNOWN_DF")
                    right_df = right_df_param if re.fullmatch(r"[A-Za-z_]\w*", right_df_param) else "UNKNOWN_DF"
                    left_is_stream = node_is_streaming.get(join_sources[0], True)
                    right_is_stream = False
                else:
                    left_df = current_df
                    right_df = "UNKNOWN_DF"
                    left_is_stream = node_is_streaming.get(source_nid, True)
                    right_is_stream = False

                # Spark stream-static joins require the streaming relation on the left for
                # several join types. Swap a static-left/stream-right pair deterministically.
                if not left_is_stream and right_is_stream:
                    logger.info("Swapping Join inputs: left=%s (static), right=%s (stream)", left_df, right_df)
                    left_df, right_df = right_df, left_df
                    left_is_stream, right_is_stream = right_is_stream, left_is_stream
                    if right_key:
                        join_key, right_key = right_key, join_key

                enable_watermarks = bool(params.get("enableWatermarks", False))
                l_col = str(params.get("leftWatermarkCol", "")).strip()
                l_delay = str(params.get("leftWatermarkDelay", "")).strip()
                r_col = str(params.get("rightWatermarkCol", "")).strip()
                r_delay = str(params.get("rightWatermarkDelay", "")).strip()

                if enable_watermarks:
                    if l_col and l_delay:
                        left_df_wm = f"{left_df}_wm"
                        code.append(
                            f"{left_df_wm} = {left_df}.withWatermark({_py_literal(l_col)}, {_py_literal(l_delay)})"
                        )
                        left_df = left_df_wm
                    if r_col and r_delay:
                        right_df_wm = f"{right_df}_wm"
                        code.append(
                            f"{right_df_wm} = {right_df}.withWatermark({_py_literal(r_col)}, {_py_literal(r_delay)})"
                        )
                        right_df = right_df_wm

                rk = right_key if right_key else join_key
                if join_type == "cross":
                    join_cond = None
                else:
                    # Column-form equality avoids string-code interpolation and can be
                    # extended safely with an event-time range condition.
                    join_cond = (
                        f"({left_df}[{_py_literal(join_key)}] == "
                        f"{right_df}[{_py_literal(rk)}])"
                    )

                # A stream-stream join is accepted only when semantic preflight has
                # complete watermarks and an explicit bounded event-time range.
                if left_is_stream and right_is_stream and join_type != "cross":
                    before = _normalise_interval(
                        params.get("timeRangeBefore", ""),
                        field_name="timeRangeBefore",
                    )
                    after = _normalise_interval(
                        params.get("timeRangeAfter", ""),
                        field_name="timeRangeAfter",
                    )
                    time_cond = (
                        f"(({left_df}[{_py_literal(l_col)}] >= "
                        f"{right_df}[{_py_literal(r_col)}] - expr({_py_literal(before)})) & "
                        f"({left_df}[{_py_literal(l_col)}] <= "
                        f"{right_df}[{_py_literal(r_col)}] + expr({_py_literal(after)})))"
                    )
                    join_cond = f"({join_cond}) & ({time_cond})"

                right_ref = f"broadcast({right_df})" if broadcast_hint else right_df
                if join_type == "cross":
                    code.append(f"{new_df} = {left_df}.crossJoin({right_ref})")
                else:
                    code.append(
                        f"{new_df} = {left_df}.join({right_ref}, {join_cond}, {_py_literal(join_type)})"
                    )

                node_is_streaming[nid] = left_is_stream or right_is_stream

            code.append("")
            node_df_var[nid] = new_df
            node_has_agg[nid] = has_agg
            node_append_safe[nid] = append_safe

    code.append("spark.streams.awaitAnyTermination()")

    final_code = "\n".join(code)

    import os
    out_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "pipeline_job.py"))
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(final_code)
        logger.info(f"Fichier sauvegardé avec succès dans : {out_path}")
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde : {e}")

    return {
        "status": "success",
        "message": "Code généré avec succès !",
        "code": final_code,
        "validation": validation_report.model_dump(mode="json"),
    }


@app.post("/api/pipeline/execute")
def execute_pipeline():
    """Execute pipeline_job.py inside the Spark Docker container."""
    import subprocess
    try:
        # Nettoyer les anciens checkpoints pour éviter les conflits d'état
        # quand le pipeline change (ex: Join → GroupBy+Aggregate)

        # D'abord arrêter tout ancien job en cours (force kill pour éviter les hooks de fermeture de Spark)
        stop_cmd = [
            "docker", "exec", "-u", "root", SPARK_MASTER_CONTAINER,
            "sh", "-c", "pkill -9 -f pipeline_job.py || true; sleep 2; rm -rf /tmp/checkpoint_*; rm -f /tmp/spark_job.err /tmp/spark_job.log"
        ]
        subprocess.run(stop_cmd, capture_output=True, text=True)
        logger.info("Ancien job Spark arrêté et checkpoints nettoyés.")

        # Exécution en tâche de fond (détachée) via subprocess pour éviter
        # le bug 'http+docker' du SDK Python sur certaines configurations Windows.
        # On redirige stderr vers un fichier pour pouvoir l'afficher dans l'UI en cas de crash
        cmd = [
            "docker", "exec", "-d", "-u", "spark", SPARK_MASTER_CONTAINER,
            "sh", "-c", f"/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/spark_ivy_cache --packages {SPARK_KAFKA_PACKAGE} /app/backend/pipeline_job.py > /tmp/spark_job.log 2> /tmp/spark_job.err"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"Erreur Docker Exec: {result.stderr}")
            raise HTTPException(
                status_code=500,
                detail=f"Erreur lors de l'exécution dans Docker: {result.stderr}"
            )

        return {
            "status": "success",
            "message": "Le job Spark a démarré avec succès.",
            "container_id": SPARK_MASTER_CONTAINER
        }

    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail="La commande 'docker' est introuvable sur le système."
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de l'exécution: {str(e)}"
        )


@app.post("/api/pipeline/stop")
def stop_pipeline():
    """Stop the running pipeline_job.py inside the Spark Docker container."""
    import subprocess
    try:
        # Tuer tous les processus python qui font tourner pipeline_job.py ou SparkSubmit
        cmd = [
            "docker", "exec", "-u", "root", SPARK_MASTER_CONTAINER,
            "sh", "-c", "pkill -f pipeline_job.py || true"
        ]

        subprocess.run(cmd, capture_output=True, text=True)

        return {
            "status": "success",
            "message": "Le job Spark a été arrêté avec succès."
        }

    except Exception as e:
        logger.error(f"Error stopping: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de l'arrêt du job: {str(e)}"
        )


@app.get("/api/pipeline/status")
def get_pipeline_status():
    """Vérifie le statut du job Spark et renvoie les erreurs éventuelles."""
    import subprocess

    # Vérifier si le process tourne
    cmd_check = [
        "docker", "exec", "-u", "root", SPARK_MASTER_CONTAINER,
        "sh", "-c", "pgrep -f pipeline_job.py"
    ]
    res = subprocess.run(cmd_check, capture_output=True, text=True)
    is_running = res.returncode == 0

    # Lire les dernières lignes de log d'erreur
    cmd_err = [
        "docker", "exec", "-u", "root", SPARK_MASTER_CONTAINER,
        "sh", "-c", "tail -n 30 /tmp/spark_job.err || echo ''"
    ]
    res_err = subprocess.run(cmd_err, capture_output=True, text=True)
    error_logs = res_err.stdout.strip()

    has_crashed = not is_running and ("Exception:" in error_logs or "Traceback" in error_logs)

    return {
        "running": is_running,
        "crashed": has_crashed,
        "logs": error_logs
    }
