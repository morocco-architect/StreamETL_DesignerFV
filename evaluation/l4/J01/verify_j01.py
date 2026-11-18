
import json
from pathlib import Path
from pyspark.sql import SparkSession

SOURCE = "/app/evaluation/l4/outputs/J01_parsed_parquet"
DEST = Path("/app/evaluation/l4/outputs/J01_observed.json")

spark = SparkSession.builder.master("local[2]").appName("J01Verification").getOrCreate()

try:
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(SOURCE)
    rows = df.orderBy("order_id", "event_id").collect()

    observed = [
        {
            "user_id": r["user_id"],
            "order_id": r["order_id"],
            "event_id": r["event_id"],
            "amount": r["amount"],
            "left_event_time": r["left_event_time"].isoformat(),
            "right_event_time": r["right_event_time"].isoformat()
        }
        for r in rows
    ]

    expected = {("O1", "E1"), ("O2", "E2")}
    actual = [(r["order_id"], r["event_id"]) for r in observed]
    passed = len(actual) == 2 and set(actual) == expected

    DEST.write_text(
        json.dumps(observed, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print("J01_PARSED_VERDICT =", "PASS" if passed else "FAIL")
    print("OBSERVED_ROWS =", len(actual))
    print(json.dumps(observed, indent=2))
    print("EVIDENCE_FILE =", DEST)

    if not passed:
        raise SystemExit(1)
finally:
    spark.stop()
