# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Silver Transformation and Data Quality Demo
# MAGIC
# MAGIC **Layer:** Silver — Cleaned, Typed, Validated  
# MAGIC **Source:** `bronze_orders`  
# MAGIC **Target:** `s3://ecommerce-lakehouse-prod/silver/orders/`  
# MAGIC **Write mode:** MERGE INTO (upsert by `order_id`)
# MAGIC
# MAGIC **Purpose:** Transform raw Bronze strings into a production-quality Silver table.
# MAGIC This is the most technically dense layer — every transformation decision is
# MAGIC deliberate and documented here with before/after comparisons.
# MAGIC
# MAGIC **What this notebook demonstrates:**
# MAGIC - Type casting with safe coercion (malformed strings → NULL, not crash)
# MAGIC - Value standardisation for `order_status` and `payment_type`
# MAGIC - Window-function deduplication (keep latest record per `order_id`)
# MAGIC - Derived business columns (`days_to_deliver`, `is_late_delivery`, `delivery_delay_days`)
# MAGIC - Data quality rules with configurable thresholds
# MAGIC - Quarantine pattern — failed rows written separately, never silently dropped
# MAGIC - MERGE INTO upsert — idempotent, safe to re-run
# MAGIC - Z-ORDER optimisation for downstream query performance
# MAGIC
# MAGIC **Prerequisites:** Run `01_bronze_ingestion_demo.py` first.

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

from pyspark.sql import functions as F
from src.utils.spark_utils import load_config, get_spark, resolve_path

ENV    = os.getenv("ENV_NAME", "dev")
config = load_config(ENV)
spark  = get_spark(config)

bronze_path     = resolve_path(config["paths"]["bronze"] + "/orders", config)
silver_path     = resolve_path(config["paths"]["silver"] + "/orders", config)
quarantine_path = resolve_path(config["paths"]["bronze"] + "/quarantine/orders", config)

print(f"Source (Bronze)    : {bronze_path}")
print(f"Target (Silver)    : {silver_path}")
print(f"Quarantine         : {quarantine_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read Bronze Orders
# MAGIC
# MAGIC We start with the raw Bronze data — all strings, with nulls and
# MAGIC dirty values exactly as received from the source.

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)

print(f"Bronze rows to process: {bronze_df.count():,}")
print()
print("Dtypes before transformation (all STRING):")
for field in bronze_df.schema.fields:
    if not field.name.startswith("_"):
        print(f"  {field.name:<40} {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Type Casting
# MAGIC
# MAGIC Cast raw strings to proper Spark types. Spark uses safe casting by default —
# MAGIC malformed values become `NULL` rather than raising an exception.
# MAGIC This is intentional: we want to see which values failed to parse (they'll
# MAGIC be caught by DQ rules), not have the pipeline crash on one bad row.

# COMMAND ----------

from src.silver.silver_orders import SilverOrdersPipeline

pipeline  = SilverOrdersPipeline(spark, config)
bronze_df = pipeline.read_bronze()
typed_df  = pipeline.cast_types(bronze_df)

# Show the before/after for key columns
print("Type casting results — key columns:")
print()
print("BEFORE (Bronze — raw strings):")
bronze_df.select(
    "order_purchase_timestamp",
    "payment_value",
    "order_approved_at",
).show(3, truncate=True)

print("AFTER (typed — proper Spark types):")
typed_df.select(
    "order_purchase_timestamp",
    "payment_value",
    "order_approved_at",
    "order_date",             # derived from order_purchase_timestamp
).show(3, truncate=True)

# COMMAND ----------

# Check how many timestamps failed to parse (malformed strings → NULL)
failed_ts = typed_df.filter(
    F.col("order_purchase_timestamp").isNull() &
    bronze_df["order_purchase_timestamp"].isNotNull()
).count()
print(f"Timestamps that failed to parse: {failed_ts:,}")
print("(These rows will be quarantined — order_date will be NULL)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Value Standardisation
# MAGIC
# MAGIC Normalise `order_status` and `payment_type` to a controlled vocabulary.
# MAGIC Values outside the valid set are mapped to `'unknown'` or `'other'` rather
# MAGIC than NULL — this preserves the row for analysis while flagging the anomaly.
# MAGIC
# MAGIC Valid statuses: `delivered`, `shipped`, `processing`, `cancelled`, `returned`, `invoiced`, `unavailable`  
# MAGIC Valid payment types: `credit_card`, `debit_card`, `paypal`, `apple_pay`, `google_pay`, `boleto`, `voucher`

# COMMAND ----------

std_df = pipeline.standardize(typed_df)

print("Order status — before standardisation:")
bronze_df.groupBy("order_status").count().orderBy(F.desc("count")).show()

print("Order status — after standardisation (lowercase, unknown for invalid values):")
std_df.groupBy("order_status").count().orderBy(F.desc("count")).show()

# COMMAND ----------

print("Payment type — after standardisation:")
std_df.groupBy("payment_type").count().orderBy(F.desc("count")).show()

print("Channel — after standardisation (lowercased, null → 'unknown'):")
std_df.groupBy("channel").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Derived Business Columns
# MAGIC
# MAGIC These columns are computed once here and stored in Silver so Gold aggregations
# MAGIC never have to recompute them. Every downstream query benefits from pre-calculated
# MAGIC delivery metrics without touching the raw timestamps.
# MAGIC
# MAGIC | Column | Formula | Business meaning |
# MAGIC |---|---|---|
# MAGIC | `days_to_deliver` | `datediff(delivered, purchased)` | End-to-end fulfilment time |
# MAGIC | `days_to_approve` | `datediff(approved, purchased)` | Payment processing time |
# MAGIC | `is_late_delivery` | `delivered > estimated` | SLA breach flag |
# MAGIC | `delivery_delay_days` | `datediff(delivered, estimated)` | Signed delay (negative = early) |

# COMMAND ----------

derived_df = pipeline.add_derived_columns(std_df)

print("Derived delivery columns — sample (delivered orders only):")
(
    derived_df
    .filter(F.col("order_status") == "delivered")
    .select(
        "order_id",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "days_to_deliver",
        "days_to_approve",
        "is_late_delivery",
        "delivery_delay_days",
    )
    .orderBy("order_id")
    .show(8, truncate=True)
)

# COMMAND ----------

# Summary stats on delivery performance
print("Delivery performance summary:")
(
    derived_df
    .filter(F.col("order_status") == "delivered")
    .agg(
        F.count("order_id")                         .alias("delivered_orders"),
        F.round(F.avg("days_to_deliver"), 1)        .alias("avg_days_to_deliver"),
        F.min("days_to_deliver")                    .alias("min_days_to_deliver"),
        F.max("days_to_deliver")                    .alias("max_days_to_deliver"),
        F.sum(F.when(F.col("is_late_delivery"), 1).otherwise(0)).alias("late_deliveries"),
    )
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Window-Function Deduplication
# MAGIC
# MAGIC Bronze is append-only, so the same `order_id` may appear multiple times
# MAGIC if the source system sent it more than once or if the Bronze pipeline ran
# MAGIC incrementally across multiple batches. Silver must have exactly one row
# MAGIC per `order_id`.
# MAGIC
# MAGIC **Strategy:** `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC)` — keep the most recently ingested version.
# MAGIC This is more efficient than `groupBy + join` for large datasets because it
# MAGIC reads the data once.

# COMMAND ----------

from pyspark.sql.window import Window

# Show duplicates before deduplication
dup_count = (
    derived_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
print(f"order_ids with duplicates (pre-dedup) : {dup_count:,}")
print(f"Total rows before dedup               : {derived_df.count():,}")

deduped_df = pipeline.deduplicate(derived_df)
print(f"Total rows after dedup                : {deduped_df.count():,}")
print(f"Rows removed                          : {derived_df.count() - deduped_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Data Quality Rules and Quarantine
# MAGIC
# MAGIC Four rules are applied. Rows that fail **any** rule are written to the
# MAGIC quarantine Delta table at `bronze/quarantine/orders/` and excluded from Silver.
# MAGIC
# MAGIC | Rule | Condition | Rationale |
# MAGIC |---|---|---|
# MAGIC | R1 | `order_id IS NOT NULL` | Cannot identify the order — row is useless |
# MAGIC | R2 | `customer_id IS NOT NULL` | Cannot attribute revenue to a customer |
# MAGIC | R3 | `order_date IS NOT NULL` | Means `order_purchase_timestamp` was unparseable |
# MAGIC | R4 | `payment_value BETWEEN 0.01 AND 50000` (when not null) | Out-of-range values indicate source system errors |
# MAGIC
# MAGIC The quarantine threshold is `1%` — if more than 1% of rows fail, a WARNING
# MAGIC is emitted. The pipeline does not stop; it continues with clean rows.

# COMMAND ----------

clean_df, quarantine_df = pipeline.apply_dq_rules(deduped_df)

total       = deduped_df.count()
clean_count = clean_df.count()
quar_count  = quarantine_df.count()

print("Data Quality Results:")
print(f"  Total rows evaluated : {total:,}")
print(f"  Clean rows (→ Silver): {clean_count:,}  ({clean_count/total*100:.2f}%)")
print(f"  Quarantined rows     : {quar_count:,}  ({quar_count/total*100:.2f}%)")
print()
dq_threshold = config["data_quality"]["orders"]["null_tolerance_pct"] * 100
print(f"  DQ threshold         : {dq_threshold:.1f}%")
status = "✅ PASS" if quar_count/total <= config["data_quality"]["orders"]["null_tolerance_pct"] else "⚠️  WARN"
print(f"  Status               : {status}")

# COMMAND ----------

# Show what kinds of rows ended up in quarantine
if quar_count > 0:
    print("Sample quarantined rows — inspect to understand failure reasons:")
    quarantine_df.select(
        "order_id",
        "customer_id",
        "order_date",
        "payment_value",
        "order_status",
    ).show(10, truncate=False)

    print("Quarantine breakdown — which rule failed:")
    quarantine_df.agg(
        F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0))    .alias("null_order_id"),
        F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)) .alias("null_customer_id"),
        F.sum(F.when(F.col("order_date").isNull(), 1).otherwise(0))  .alias("null_order_date"),
        F.sum(
            F.when(
                F.col("payment_value").isNotNull() &
                ((F.col("payment_value") < 0.01) | (F.col("payment_value") > 50000)),
                1
            ).otherwise(0)
        ).alias("out_of_range_payment"),
    ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Write to Silver (MERGE INTO)
# MAGIC
# MAGIC The full `SilverOrdersPipeline.run()` writes clean rows to Silver via
# MAGIC `MERGE INTO` on `order_id`, then runs `OPTIMIZE + ZORDER BY (customer_id, order_date)`.
# MAGIC
# MAGIC Running the full pipeline here (rather than calling each step separately)
# MAGIC ensures the Silver table is created if it doesn't exist, all derived columns
# MAGIC are present, and the post-write optimisation runs.

# COMMAND ----------

# Run the complete Silver pipeline end-to-end
# (This re-reads Bronze and re-applies all steps — idempotent by design)
silver_summary = SilverOrdersPipeline(spark, config).run()

print("\nSilver pipeline summary:")
for k, v in silver_summary.items():
    print(f"  {k:<25} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the Silver Orders Table
# MAGIC
# MAGIC Verify the transformation: strings → proper types, derived columns present,
# MAGIC one row per `order_id`.

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)

print(f"Total rows in silver_orders: {silver_df.count():,}")
print()
print("Schema (compare to Bronze — all types are now correct):")
silver_df.printSchema()

# COMMAND ----------

print("Silver orders sample — typed and enriched:")
silver_df.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",   # now TIMESTAMP
    "payment_value",              # now DECIMAL(12,2)
    "order_date",                 # now DATE
    "days_to_deliver",
    "is_late_delivery",
    "delivery_delay_days",
    "_silver_processed_at",
).show(5, truncate=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table History — Silver
# MAGIC
# MAGIC Each MERGE INTO is recorded as a version in the Delta transaction log.
# MAGIC Re-running this notebook produces a new version, not a duplicate table.

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}` LIMIT 5").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel Demo
# MAGIC
# MAGIC Delta time travel lets you read Silver as it existed at any prior version.
# MAGIC In production this is used to reproduce historical pipeline outputs or
# MAGIC investigate when a bad value was introduced.

# COMMAND ----------

from src.utils.delta_utils import get_table_history, read_at_version

history = get_table_history(spark, silver_path, limit=5)
versions = [row["version"] for row in history.collect()]

if len(versions) >= 2:
    prev_version = versions[1]
    print(f"Reading silver_orders at version {prev_version} (previous run):")
    prev_df = read_at_version(spark, silver_path, version=prev_version)
    print(f"  Row count at version {prev_version}: {prev_df.count():,}")
    print(f"  Row count at current version     : {silver_df.count():,}")
else:
    print("Only one version exists — run the pipeline a second time to see time travel in action.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Summary
# MAGIC
# MAGIC | Transformation | Input | Output |
# MAGIC |---|---|---|
# MAGIC | Type casting | Raw strings | Proper timestamps, decimals, dates |
# MAGIC | Standardisation | Free-text statuses | Controlled vocabulary |
# MAGIC | Deduplication | Multiple rows per `order_id` | Exactly one row (latest wins) |
# MAGIC | Derived columns | Raw timestamps | `days_to_deliver`, `is_late_delivery`, etc. |
# MAGIC | DQ rules | All rows | Clean rows in Silver, failed rows in quarantine |
# MAGIC | Write pattern | — | MERGE INTO (idempotent upsert) |
# MAGIC | Post-write | — | OPTIMIZE + ZORDER BY (customer_id, order_date) |
# MAGIC
# MAGIC **Next:** Run `03_gold_revenue_insights_demo.py` to build business-ready
# MAGIC revenue aggregations from this clean Silver table.

# COMMAND ----------

print("✅ Silver transformation complete.")
print(f"   silver_orders  : {silver_df.count():,} clean rows")
print(f"   Quarantine     : {quar_count:,} rows written to {quarantine_path}")
print()
print("Proceed to: 03_gold_revenue_insights_demo.py")
