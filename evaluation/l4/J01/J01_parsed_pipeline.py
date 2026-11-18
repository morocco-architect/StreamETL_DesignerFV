from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('StreamETL_App') \
    .master('spark://streametl-spark-master:7077') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
    .getOrCreate()

# 1. Read from Kafka Source: l4_j01_orders_in
df_l4_j01_orders_in = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'l4_j01_orders_in') \
    .option('startingOffsets', 'earliest') \
    .option('kafka.group.id', 'l4-j01-orders-20261114') \
    .load()

# 2. Read from Kafka Source: l4_j01_events_in
df_l4_j01_events_in = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'l4_j01_events_in') \
    .option('startingOffsets', 'earliest') \
    .option('kafka.group.id', 'l4-j01-events-20261114') \
    .load()

# 3. Transformation: ParseJSON
df_step3 = df_l4_j01_orders_in.selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), 'order_id STRING, user_id INT, amount DOUBLE, event_time TIMESTAMP').alias('data')) \
    .select('data.*')

# 4. Transformation: ParseJSON
df_step4 = df_l4_j01_events_in.selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), 'event_id STRING, user_id INT, event_type STRING, event_time TIMESTAMP').alias('data')) \
    .select('data.*')

# 5. Transformation: Join
df_step3_wm = df_step3.withWatermark('event_time', '10 minutes')
df_step4_wm = df_step4.withWatermark('event_time', '10 minutes')
df_step5_raw = df_step3_wm.alias('l').join(df_step4_wm.alias('r'), ((col('l.user_id') == col('r.user_id'))) & (((col('l.event_time') >= col('r.event_time') - expr('INTERVAL 10 MINUTES')) & (col('l.event_time') <= col('r.event_time') + expr('INTERVAL 5 MINUTES')))), 'inner')
_join_left_cols_5 = list(df_step3_wm.columns)
_join_right_cols_5 = list(df_step4_wm.columns)
_join_shared_cols_5 = set(_join_left_cols_5) & set(_join_right_cols_5)
_join_select_cols_5 = []
_join_select_cols_5.append(col('l.user_id').alias('user_id'))
for _c in _join_left_cols_5:
    if _c == 'user_id':
        continue
    _out = f'left_{_c}' if _c in _join_shared_cols_5 else _c
    _join_select_cols_5.append(col(f'l.`{_c}`').alias(_out))
for _c in _join_right_cols_5:
    if _c == 'user_id':
        continue
    _out = f'right_{_c}' if _c in _join_shared_cols_5 else _c
    _join_select_cols_5.append(col(f'r.`{_c}`').alias(_out))
df_step5 = df_step5_raw.select(*_join_select_cols_5)

# 6. Write to Parquet Sink: /app/evaluation/l4/outputs/J01_parsed_parquet
query_parquet_n4 = df_step5.writeStream \
    .format('parquet') \
    .outputMode('append') \
    .option('path', '/app/evaluation/l4/outputs/J01_parsed_parquet') \
    .option('checkpointLocation', '/app/evaluation/l4/checkpoints/J01_parsed') \
    .start()

spark.streams.awaitAnyTermination()