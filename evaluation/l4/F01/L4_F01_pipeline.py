from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('StreamETL_App') \
    .master('spark://streametl-spark-master:7077') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
    .getOrCreate()

# 1. Read from Kafka Source: l4_f01_orders_in
df_l4_f01_orders_in = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'l4_f01_orders_in') \
    .option('startingOffsets', 'earliest') \
    .option('kafka.group.id', 'l4-f01-20261114') \
    .load()

# 2. Transformation: ParseJSON
df_step2 = df_l4_f01_orders_in.selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), 'order_id STRING, user_id INT, amount DOUBLE, status STRING, event_time TIMESTAMP').alias('data')) \
    .select('data.*')

# 3. Transformation: Filter
df_step3 = df_step2.filter("status = 'COMPLETED' AND amount > 100")

# 4. Write to Kafka Sink: l4_f01_orders_out
sink_df_l4_f01_orders_out = df_step3.select(to_json(struct('*')).alias('value'))
query_l4_f01_orders_out = sink_df_l4_f01_orders_out.writeStream \
    .format('kafka') \
    .outputMode('append') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('topic', 'l4_f01_orders_out') \
    .option('checkpointLocation', '/app/evaluation/l4/checkpoints/F01') \
    .option('kafka.acks', 'all') \
    .start()

spark.streams.awaitAnyTermination()
