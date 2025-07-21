#!/usr/bin/env python3
"""
Real-time interaction aggregator (Spark Structured Streaming).

• Consumes Kafka topic produced by generator_producer.py
• Calculates
    – max / min interactions-per-second per item
    – running interaction totals per user
    – overall average interactions per user
• Writes results to Elasticsearch indices:
      interaction-agg_item / interaction-agg_user / interaction-agg_avg
"""

import os
import logging
from logging.handlers import RotatingFileHandler

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from dotenv import load_dotenv

# ─────────────────────────── Logging ──────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        RotatingFileHandler("logs/agg.log", maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ───────────────────────── Env / Config ───────────────────────────
load_dotenv()
BROKER      = os.getenv("KAFKA_BROKER",       "localhost:9092")
TOPIC       = os.getenv("KAFKA_TOPIC",        "interactions")
ES_HOST     = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
PARTITIONS  = int(os.getenv("SPARK_PARTITIONS", "4"))

ES_ITEM_INDEX = "interaction-agg_item/_doc"
ES_USER_INDEX = "interaction-agg_user/_doc"
ES_AVG_INDEX  = "interaction-agg_avg/_doc"

# ───────────────────── Spark Session Bootstrap ────────────────────
spark = (
    SparkSession.builder.appName("InteractionAggregator")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.8.0",
    )
    .config("spark.sql.shuffle.partitions", str(PARTITIONS))
    .config("spark.streaming.backpressure.enabled", "true")
    .config("spark.sql.adaptive.enabled", "false")   # no AQE in streaming
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────── Schema Definition ───────────────────────
event_schema = StructType(
    [
        StructField("event_id",         StringType(),    True),
        StructField("user_id",          StringType(),    True),
        StructField("item_id",          StringType(),    True),
        StructField("interaction_type", StringType(),    True),
        StructField("timestamp",        TimestampType(), True),
    ]
)

# ─────────────────────── Stream Read (Kafka) ─────────────────────
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", event_schema).alias("e"))
        .select("e.*")
        .withWatermark("timestamp", "60 seconds")
)

# ─────────────────  per-second item counts  ───────────────────────
item_per_sec = (
    events.groupBy(
        F.window("timestamp", "1 second").alias("w"),
        "item_id"
    )
    .agg(F.count("*").alias("cnt"))
    .select("item_id", "w.start", "cnt")
)

# ─────────────────── Elasticsearch helper ─────────────────────────
def es_write(df: DataFrame, index: str, id_col: str | None):
    writer = (
        df.write
          .format("org.elasticsearch.spark.sql")
          .option("es.nodes", ES_HOST)
          .option("es.nodes.wan.only", "true")
          .option("es.resource", index)
          .option("es.write.operation", "upsert")
    )
    if id_col:
        writer = writer.option("es.mapping.id", id_col)
    writer.mode("append").save()

# ─────────────── foreachBatch processors  ─────────────────────────
def process_item_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    stats = (
        batch_df.groupBy("item_id")
                .agg(
                    F.max("cnt").alias("max_per_sec"),
                    F.min("cnt").alias("min_per_sec"),
                    F.sum("cnt").alias("total_interactions")
                )
    )
    es_write(stats, ES_ITEM_INDEX, "item_id")
    log.info("item batch %s → %d rows", batch_id, stats.count())

def process_user_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    # per-user delta within this micro-batch
    user_stats = (
        batch_df.groupBy("user_id")
                .agg(F.count("*").alias("delta"))
    )
    es_write(user_stats, ES_USER_INDEX, "user_id")

    # overall average interactions per user for this batch (rolling proxy)
    avg_df = (
        user_stats.agg(F.avg("delta").alias("avg_interactions_per_user"))
                  .withColumn("doc_id", F.lit("global"))
    )
    es_write(avg_df, ES_AVG_INDEX, "doc_id")
    log.info("user batch %s → %d users", batch_id, user_stats.count())

# ─────────────────────── Streaming sinks  ─────────────────────────
item_query = (
    item_per_sec.writeStream
               .foreachBatch(process_item_batch)
               .outputMode("update")
               .option("checkpointLocation", "chk/item")
               .start()
)

user_query = (
    events.writeStream
          .foreachBatch(process_user_batch)
          .outputMode("append")
          .option("checkpointLocation", "chk/user")
          .start()
)

log.info("queries started — awaiting termination")
spark.streams.awaitAnyTermination()
