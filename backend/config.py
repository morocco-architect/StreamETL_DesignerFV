"""
StreamETL Designer — Backend configuration
--------------------------------------------
Centralises every value that was previously hardcoded and duplicated across
main.py, the code generator, and docker-compose (Kafka hosts, Spark
container name, Postgres URL, CORS origins). Everything can be overridden
with environment variables so the same code works in dev, Docker, and CI.
"""

import os


def _split_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


# --- Kafka -------------------------------------------------------------
# Used by the FastAPI backend (admin client, consumer) running on the host.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Used *inside* the generated PySpark code, which runs inside the Spark
# container and therefore talks to Kafka over the internal Docker network.
KAFKA_INTERNAL_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_INTERNAL_BOOTSTRAP_SERVERS", "kafka:29092"
)

# --- Spark ---------------------------------------------------------------
# Name of the Spark master container as seen by `docker exec`. This must
# match the `container_name` set in deploy/docker-compose.yml.
SPARK_MASTER_CONTAINER = os.getenv("SPARK_MASTER_CONTAINER", "streametl-spark-master")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://streametl-spark-master:7077")
SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

# --- Database ------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://admin:admin@localhost:5433/streametl"
)

# --- API -------------------------------------------------------------
CORS_ALLOWED_ORIGINS = _split_csv(
    os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
)
