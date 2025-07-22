#!/usr/bin/env python3
"""
InteractionAggregator – extended KPI edition (clean & stable).

Consumes Kafka topic of synthetic e-commerce events and writes 8 KPIs to
Elasticsearch indices.

KPIs:
  1. ItemAgg        interaction-agg_item
  2. UserAgg        interaction-agg_user
  3. AvgAgg         interaction-agg_avg
  4. Funnel         interaction-agg_funnel
  5. ActiveUsers    interaction-agg_active_users  (approx_count_distinct)
  6. RevenuePerMin  interaction-agg_revenue
  7. CategorySales  interaction-agg_category
  8. RPS            interaction-agg_rps
"""

import os
import logging
from logging.handlers import RotatingFileHandler

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, TimestampType
)
from dotenv import load_dotenv

# ────────────────── Logging ──────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        RotatingFileHandler("logs/agg.log", maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("aggregator")

# ───────────── Env & constants ───────────────
load_dotenv()
BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC   = (os.getenv("KAFKA_TOPIC") or "interactions").strip()
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
PARTS   = int(os.getenv("SPARK_PARTITIONS", "4"))

IDX = {
    "item"    : "interaction-agg_item/_doc",
    "user"    : "interaction-agg_user/_doc",
    "avg"     : "interaction-agg_avg/_doc",
    "funnel"  : "interaction-agg_funnel/_doc",
    "active"  : "interaction-agg_active_users/_doc",
    "revenue" : "interaction-agg_revenue/_doc",
    "category": "interaction-agg_category/_doc",
    "rps"     : "interaction-agg_rps/_doc",
}

if not TOPIC:
    log.critical("Kafka topic is empty; set KAFKA_TOPIC or --topic")
    raise SystemExit(1)

# ─────────── Spark session ────────────
spark = (
    SparkSession.builder.appName("InteractionAggregator")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.8.0",
    )
    .config("spark.sql.shuffle.partitions", str(PARTS))
    .config("spark.streaming.backpressure.enabled", "true")
    .config("spark.sql.adaptive.enabled", "false")            # avoid AQE in streaming
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")     # guard against broadcast
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────── Schema ───────────
schema = StructType([
    StructField("event_id",         StringType(), True),
    StructField("session_id",       StringType(), True),
    StructField("user_id",          StringType(), True),
    StructField("item_id",          StringType(), True),
    StructField("category",         StringType(), True),
    StructField("price_usd",        FloatType(),  True),
    StructField("interaction_type", StringType(), True),
    StructField("timestamp",        TimestampType(), True),
])

# ─────────── Source stream ─────
raw = (
    spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", BROKER)
         .option("subscribe", TOPIC)
         .option("startingOffsets", "latest")
         .load()
)

events = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
       .select(F.from_json("json_str", schema).alias("e"))
       .select("e.*")
       .withWatermark("timestamp", "60 seconds")
)



# ───────────────────── elastic helper ───────────────────────────
def es_write(df: DataFrame, index: str, id_col: str | None = None):
    """
    Write `df` to Elasticsearch index:
      • plain `index` mode when `id_col is None`
      • `upsert` when an id column is supplied (nulls filtered out).
    """
    if df.rdd.isEmpty():
        return
    writer = (df.write
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.nodes.wan.only", "true")
                .option("es.resource", index))

    if id_col:
        df = df.filter(F.col(id_col).isNotNull())
        writer = (writer
                  .option("es.write.operation", "upsert")
                  .option("es.mapping.id", id_col))
    else:
        writer = writer.option("es.write.operation", "index")

    writer.mode("append").save()


# ─── KPI 1 — item min / max / total ───
item_per_sec = (
    events.groupBy(F.window("timestamp", "1 second").alias("w"), "item_id")
          .agg(F.count("*").alias("cnt"))
)

def process_item(batch_df: DataFrame, _):
    stats = (
        batch_df.groupBy("item_id")
                .agg(
                    F.max("cnt").alias("max_per_sec"),
                    F.min("cnt").alias("min_per_sec"),
                    F.sum("cnt").alias("total_interactions")
                )
    )
    es_write(stats, IDX["item"], "item_id")

item_per_sec.writeStream.foreachBatch(process_item)\
    .outputMode("update").option("checkpointLocation", "chk/item").start()

# ─── KPI 2/3 — user totals & global avg ───
def process_user(batch_df: DataFrame, _):
    per_user = batch_df.groupBy("user_id")\
                       .agg(F.count("*").alias("total_interactions"))
    es_write(per_user, IDX["user"], "user_id")

    global_avg = (
        per_user.agg(F.avg("total_interactions").alias("avg_interactions_per_user"))
                .withColumn("doc_id", F.lit("global"))
    )
    es_write(global_avg, IDX["avg"], "doc_id")

events.writeStream.foreachBatch(process_user)\
      .outputMode("append").option("checkpointLocation", "chk/user").start()

# ─── KPI 4 — funnel per item ───
def process_funnel(batch_df: DataFrame, _):
    tmp = (
        batch_df.groupBy("item_id", "interaction_type")
                .agg(F.count("*").alias("cnt"))
    )
    funnel = (
        tmp.groupBy("item_id")
           .pivot("interaction_type", ["view", "click", "add_to_cart", "purchase"])
           .sum("cnt")
           .fillna(0)
           .withColumn("cr_view_click", F.col("click") / F.col("view"))
           .withColumn("cr_click_purchase", F.col("purchase") / F.col("click"))
    )
    es_write(funnel, IDX["funnel"], "item_id")

events.writeStream.foreachBatch(process_funnel)\
      .outputMode("update").option("checkpointLocation", "chk/funnel").start()

# ─── KPI 5 — active users /5 min (approx) ───
active_users = (
    events.groupBy(F.window("timestamp", "5 minutes").alias("w"))
          .agg(F.approx_count_distinct("user_id").alias("active_users"))
          .select(F.col("w.start").alias("window_start"), "active_users")
)

active_users.writeStream.foreachBatch(
    lambda df, _: es_write(df, IDX["active"], None)
).outputMode("update").option("checkpointLocation", "chk/active").start()

# ─── KPI 6 — revenue per minute ───
revenue_min = (
    events.filter("interaction_type = 'purchase'")
          .groupBy(F.window("timestamp", "1 minute").alias("w"))
          .agg(F.sum("price_usd").alias("sum_price_usd"))
          .select(F.col("w.start").alias("window_start"), "sum_price_usd")
)

revenue_min.writeStream.foreachBatch(
    lambda df, _: es_write(df, IDX["revenue"], None)
).outputMode("update").option("checkpointLocation", "chk/revenue").start()

# ─── KPI 7 — category sales ───
def process_category(batch_df: DataFrame, _):
    cat = (
        batch_df.filter("interaction_type = 'purchase'")
                .groupBy("category")
                .agg(
                    F.count("*").alias("total_units"),
                    F.sum("price_usd").alias("total_revenue")
                )
    )
    es_write(cat, IDX["category"], "category")

events.writeStream.foreachBatch(process_category)\
      .outputMode("append").option("checkpointLocation", "chk/category").start()

# ─── KPI 8 — requests per second ───
rps = (
    events.groupBy(F.window("timestamp", "1 second").alias("w"))
          .agg(F.count("*").alias("rps"))
          .select(F.col("w.start").alias("window_start"), "rps")
)

rps.writeStream.foreachBatch(
    lambda df, _: es_write(df, IDX["rps"], None)
).outputMode("update").option("checkpointLocation", "chk/rps").start()

log.info("✅ All KPI queries started – waiting for data…")
spark.streams.awaitAnyTermination()
