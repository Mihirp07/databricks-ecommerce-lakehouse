# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Bronze Ingestion Demo
# MAGIC
# MAGIC **Layer:** Bronze — Raw Ingestion  
# MAGIC **Storage path:** `s3://ecommerce-lakehouse-prod/bronze/` (dev: `dbfs:/FileStore/lakehouse/bronze/`)  
# MAGIC **Write mode:** Append-only — raw data is never modified after landing
# MAGIC
# MAGIC **Purpose:** Ingest raw order data and streaming clickstream events into
# MAGIC Delta Lake Bronze tables. Bronze preserves every raw value exactly as received
# MAGIC from the source — no type casting, no business logic, no filtering.
# MAGIC
# MAGIC **What this notebook demonstrates:**
# MAGIC - Batch CSV ingestion with schema enforcement (`mergeSchema: false`)
# MAGIC - Metadata columns for lineage (`_source_file`, `_batch_id`, `_ingested_at`)
# MAGIC - Auto Loader streaming ingestion for clickstream events
# MAGIC - Append-only Delta write pattern with date partitioning
# MAGIC - Corrupt row detection and logging
# MAGIC
# MAGIC **Prerequisites:** Run `00_setup_and_data_generation.py` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialise Environment

# COMMAND ----------

import os
import sys
from pathlib import Path

project_root = str(Path(os.getcwd()).parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.spark_utils import load_config, get_spark, resolve_path

ENV    = os.getenv("ENV_NAME", "dev")
config = load_config(ENV)
spark  = get_spark(config)

bronze_orders_path = resolve_path(config["paths"]["bronze"] + "/orders", config)
bronze_events_path = resolve_path(config["paths"]["bronze"] + "/events", config)
raw_path           = resolve_path(config["paths"]["raw"], config)

print(f"Bronze orders path : {bronze_orders_path}")
print(f"Bronze events path : {bronze_events_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — Batch Ingestion: Orders
# MAGIC
# MAGIC The `BronzeOrdersPipeline` class reads raw CSV from the landing zone,
# MAGIC adds pipeline metadata, and appends to the Bronze Delta table.
# MAGIC
# MAGIC **Key design decisions:**
# MAGIC - `inferSchema=false` — we define the schema explicitly; never trust inference in production
# MAGIC - `mode=PERMISSIVE` — malformed rows go to `_corrupt_record` rather than crashing the job
# MAGIC - `mergeSchema=false` — new columns from upstream are rejected, not silently absorbed
# MAGIC - Partition by `_ingest_date` — enables efficient watermark-based incremental reads

# COMMAND ----------

from src.bronze.bronze_orders import BronzeOrdersPipeline

pipeline = BronzeOrdersPipeline(spark, config)
summary  = pipeline.run()

print("\nIngestion summary:")
for k, v in summary.items():
    print(f"  {k:<20} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the Bronze Orders Table
# MAGIC
# MAGIC Notice that every value is still a raw string — `order_purchase_timestamp`,
# MAGIC `payment_value`, and all date fields are `STRING`. Type casting happens in Silver.
# MAGIC The three metadata columns (`_ingested_at`, `_source_file`, `_batch_id`) are
# MAGIC added by the pipeline — they do not exist in the source CSV.

# COMMAND ----------

bronze_orders = spark.read.format("delta").load(bronze_orders_path)

print(f"Total rows in bronze_orders: {bronze_orders.count():,}")
print()
print("Schema:")
bronze_orders.printSchema()

# COMMAND ----------

bronze_orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",  # raw string — not a timestamp yet
    "payment_value",             # raw string — not a decimal yet
    "payment_type",
    "_ingested_at",
    "_batch_id",
    "_source_file",
    "_ingest_date",
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partition Layout
# MAGIC
# MAGIC Bronze is partitioned by `_ingest_date`. In production with daily runs,
# MAGIC each partition holds one day of ingested records. The Silver pipeline
# MAGIC reads only new partitions since its last run — this is the incremental
# MAGIC load watermark pattern.

# COMMAND ----------

print("Bronze orders partition distribution:")
(
    bronze_orders
    .groupBy("_ingest_date")
    .count()
    .orderBy("_ingest_date", ascending=False)
    .show(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Value Inspection
# MAGIC
# MAGIC This is intentional — Bronze is a faithful mirror of the source.
# MAGIC Bad values are visible here for debugging, not hidden.

# COMMAND ----------

from pyspark.sql import functions as F

print("Sample of raw order_status values (un-standardised):")
(
    bronze_orders
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
    .show()
)

print("Rows with null payment_value (raw nulls from source):")
null_payment = bronze_orders.filter(F.col("payment_value").isNull()).count()
total        = bronze_orders.count()
print(f"  {null_payment:,} / {total:,} rows ({null_payment/total*100:.1f}%)")
print("  → These will be evaluated against DQ rules in Silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table History
# MAGIC
# MAGIC Every write to a Delta table is recorded in the transaction log.
# MAGIC This is the foundation for time travel and audit capabilities.

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY delta.`{bronze_orders_path}` LIMIT 5").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — Streaming Ingestion: Clickstream Events
# MAGIC
# MAGIC Clickstream events use Spark Structured Streaming with Auto Loader
# MAGIC (`cloudFiles` format). Auto Loader tracks processed files via a checkpoint,
# MAGIC so re-running the pipeline never re-processes the same events.
# MAGIC
# MAGIC **Trigger mode used here:** `availableNow=True` — processes all pending files
# MAGIC then stops. This is the micro-batch pattern used in scheduled Databricks Jobs.
# MAGIC For continuous streaming, set `trigger_once=False`.

# COMMAND ----------

from src.bronze.bronze_events_stream import BronzeEventsStreamPipeline

events_pipeline = BronzeEventsStreamPipeline(spark, config)
query = events_pipeline.run(trigger_once=True)

# Wait for the trigger-once query to finish processing all available files
query.awaitTermination()
print(f"\nStreaming query finished. Status: {query.lastProgress}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the Bronze Events Table

# COMMAND ----------

bronze_events = spark.read.format("delta").load(bronze_events_path)

print(f"Total rows in bronze_events: {bronze_events.count():,}")
print()
print("Schema:")
bronze_events.printSchema()

# COMMAND ----------

bronze_events.select(
    "event_id",
    "session_id",
    "customer_id",
    "event_type",
    "event_timestamp",   # raw string — typed in Silver
    "product_id",
    "device_type",
    "referrer",
    "event_date",        # derived partition column added by Bronze pipeline
    "_ingested_at",
    "_batch_id",
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Type Distribution

# COMMAND ----------

print("Event type distribution in bronze_events:")
(
    bronze_events
    .groupBy("event_type")
    .count()
    .orderBy(F.desc("count"))
    .show()
)

# COMMAND ----------

print("Device type distribution:")
(
    bronze_events
    .groupBy("device_type")
    .count()
    .orderBy(F.desc("count"))
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Summary
# MAGIC
# MAGIC | Table | Rows | Partition col | Write mode |
# MAGIC |---|---|---|---|
# MAGIC | `bronze_orders` | ~100K | `_ingest_date` | Append-only (batch) |
# MAGIC | `bronze_events` | ~500K | `event_date` | Append-only (streaming) |
# MAGIC
# MAGIC Both tables preserve raw string values. No business logic has been applied.
# MAGIC All metadata columns (`_ingested_at`, `_source_file`, `_batch_id`) are in place
# MAGIC for full lineage traceability.
# MAGIC
# MAGIC **Next:** Run `02_silver_transformation_and_dq_demo.py` to clean,
# MAGIC validate, and type-cast these Bronze records into the Silver layer.

# COMMAND ----------

print("✅ Bronze ingestion complete.")
print(f"   bronze_orders : {spark.read.format('delta').load(bronze_orders_path).count():,} rows")
print(f"   bronze_events : {spark.read.format('delta').load(bronze_events_path).count():,} rows")
print()
print("Proceed to: 02_silver_transformation_and_dq_demo.py")
