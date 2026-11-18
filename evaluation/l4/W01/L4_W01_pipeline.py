from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('StreamETL_App') \
    .master('spark://streametl-spark-master:7077') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
    .getOrCreate()

# 1. Read from Kafka Source: l4_w01_orders_in
df_l4_w01_orders_in = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'l4_w01_orders_in') \
    .option('startingOffsets', 'earliest') \
    .option('kafka.group.id', 'l4-w01-20261114') \
    .load()

# 2. Transformation: ParseJSON
df_step2 = df_l4_w01_orders_in.selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), 'order_id STRING, user_id INT, amount DOUBLE, status STRING, event_time TIMESTAMP').alias('data')) \
    .select('data.*')

# 3. Transformation: GroupBy
df_step3 = df_step2.withWatermark('event_time', '10 minutes')
# (GroupBy on [user_id] with window — combined with next Aggregate)

# 4. Transformation: Aggregate
df_step4 = df_step3.groupBy('user_id', window('event_time', '10 minutes', '5 minutes')).agg(sum('amount').alias('total_amount'))

# 5. Write to Parquet Sink: /app/evaluation/l4/outputs/W01_parquet
query_parquet_n5 = df_step4.writeStream \
    .format('parquet') \
    .outputMode('append') \
    .option('path', '/app/evaluation/l4/outputs/W01_parquet') \
    .option('checkpointLocation', '/app/evaluation/l4/checkpoints/W01') \
    .start()

spark.streams.awaitAnyTermination()
