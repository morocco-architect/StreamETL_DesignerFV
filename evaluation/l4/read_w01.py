
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

SOURCE = "/app/evaluation/l4/outputs/W01_parquet"
DESTINATION = Path("/app/evaluation/l4/outputs/W01_observed.json")

spark = (
    SparkSession.builder
    .appName("L4_W01_Result_Validation")
    .master("local[2]")
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(SOURCE)

    rows = (
        df.filter(col("user_id") == 11)
        .orderBy("window.start")
        .collect()
    )

    results = [
        {
            "user_id": row["user_id"],
            "window_start": row["window"]["start"].isoformat(),
            "window_end": row["window"]["end"].isoformat(),
            "total_amount": row["total_amount"],
        }
        for row in rows
    ]

    DESTINATION.write_text(
        json.dumps(results, indent=2),
        encoding="utf-8"
    )

    print(json.dumps(results, indent=2))
    print(f"\nSaved to: {DESTINATION}")

finally:
    spark.stop()
