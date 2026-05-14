# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Gold Revenue Insights Demo
# MAGIC
# MAGIC **Layer:** Gold — Business-Ready Aggregates  
# MAGIC **Source:** `silver_orders`  
# MAGIC **Targets:**
# MAGIC - `s3://ecommerce-lakehouse-prod/gold/revenue_daily/`
# MAGIC - `s3://ecommerce-lakehouse-prod/gold/revenue_monthly/`
# MAGIC - `s3://ecommerce-lakehouse-prod/gold/revenue_by_channel/`
# MAGIC
# MAGIC **Write mode:** MERGE INTO — idempotent, re-runnable
# MAGIC
# MAGIC **Purpose:** Transform clean Silver orders into three pre-aggregated Gold tables
# MAGIC that power executive dashboards, financial reporting, and marketing analysis.
# MAGIC This is the business-facing layer — every metric here maps directly to a KPI
# MAGIC that a stakeholder would recognise.
# MAGIC
# MAGIC **What this notebook demonstrates:**
# MAGIC - `groupBy` aggregations for daily and monthly revenue KPIs
# MAGIC - `LAG()` window function for day-over-day and month-over-month growth rates
# MAGIC - Broadcast join optimisation for channel revenue share calculation
# MAGIC - `.cache()` pattern — Silver read once, aggregated three times
# MAGIC - MERGE INTO + OPTIMIZE for Gold tables
# MAGIC - Business insights: top revenue days, late delivery trends, channel performance
# MAGIC
# MAGIC **Prerequisites:** Run `02_silver_transformation_and_dq_demo.py` first.

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
from pyspark.sql.window import Window
from src.utils.spark_utils import load_config, get_spark, resolve_path

ENV    = os.getenv("ENV_NAME", "dev")
config = load_config(ENV)
spark  = get_spark(config)

silver_path  = resolve_path(config["paths"]["silver"] + "/orders", config)
daily_path   = resolve_path(config["paths"]["gold"]   + "/revenue_daily", config)
monthly_path = resolve_path(config["paths"]["gold"]   + "/revenue_monthly", config)
channel_path = resolve_path(config["paths"]["gold"]   + "/revenue_by_channel", config)

print("Gold output paths:")
print(f"  revenue_daily      : {daily_path}")
print(f"  revenue_monthly    : {monthly_path}")
print(f"  revenue_by_channel : {channel_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read and Cache Silver Orders
# MAGIC
# MAGIC The Gold pipeline builds three aggregations from the same Silver DataFrame.
# MAGIC Caching avoids three separate reads from storage — one read, three results.
# MAGIC `.cache()` keeps the DataFrame in cluster memory for the duration of this notebook.
# MAGIC `.unpersist()` releases it at the end.

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path).cache()

row_count = silver_df.count()
print(f"Silver orders loaded and cached: {row_count:,} rows")
print()
print("Date range covered:")
silver_df.agg(
    F.min("order_date").alias("earliest_order"),
    F.max("order_date").alias("latest_order"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Build and Run the Gold Pipeline
# MAGIC
# MAGIC `GoldRevenueSummaryPipeline.run()` builds all three Gold tables in one pass.
# MAGIC Walk through each table's logic in the sections below to understand
# MAGIC what each metric means and how it is calculated.

# COMMAND ----------

from src.gold.gold_revenue_summary import GoldRevenueSummaryPipeline

gold_pipeline = GoldRevenueSummaryPipeline(spark, config)
gold_summary  = gold_pipeline.run()

print("\nGold pipeline summary:")
for k, v in gold_summary.items():
    print(f"  {k:<20} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Daily Revenue (`gold_revenue_daily`)
# MAGIC
# MAGIC One row per calendar date. The primary table for day-level dashboards.
# MAGIC
# MAGIC **Key metrics:**
# MAGIC - `gross_revenue` — all non-null payment values including cancelled/returned orders
# MAGIC - `net_revenue` — excludes cancelled and returned orders (the real business revenue)
# MAGIC - `dod_revenue_growth` — day-over-day change in net revenue as a decimal fraction
# MAGIC - `late_delivery_rate` — proportion of delivered orders that missed the SLA

# COMMAND ----------

daily_df = spark.read.format("delta").load(daily_path)

print(f"gold_revenue_daily: {daily_df.count():,} rows (one per date)")
print()
daily_df.select(
    "order_date",
    "total_orders",
    "delivered_orders",
    "cancelled_orders",
    "gross_revenue",
    "net_revenue",
    "avg_order_value",
    "unique_customers",
    "dod_revenue_growth",
    "late_delivery_rate",
).orderBy(F.desc("order_date")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight: Top 10 Revenue Days

# COMMAND ----------

print("Top 10 days by net revenue:")
(
    daily_df
    .select(
        "order_date",
        "net_revenue",
        "total_orders",
        "avg_order_value",
        "unique_customers",
        F.round(F.col("dod_revenue_growth") * 100, 2).alias("dod_growth_pct"),
    )
    .orderBy(F.desc("net_revenue"))
    .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight: Late Delivery Trend
# MAGIC
# MAGIC Late delivery rate = `late_delivery_count / delivered_orders`.
# MAGIC High values indicate fulfilment or logistics issues on specific dates.

# COMMAND ----------

print("Days with highest late delivery rates (minimum 10 deliveries):")
(
    daily_df
    .filter(F.col("delivered_orders") >= 10)
    .select(
        "order_date",
        "delivered_orders",
        "late_delivery_count",
        F.round(F.col("late_delivery_rate") * 100, 1).alias("late_pct"),
        F.round(F.col("avg_days_to_deliver"), 1).alias("avg_days_to_deliver"),
    )
    .orderBy(F.desc("late_delivery_rate"))
    .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight: Day-over-Day Revenue Volatility

# COMMAND ----------

print("Days with largest revenue swings (DoD growth > ±20%):")
(
    daily_df
    .filter(F.col("dod_revenue_growth").isNotNull())
    .select(
        "order_date",
        "net_revenue",
        F.round(F.col("dod_revenue_growth") * 100, 1).alias("dod_growth_pct"),
    )
    .orderBy(F.abs(F.col("dod_revenue_growth")).desc())
    .limit(10)
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Monthly Revenue (`gold_revenue_monthly`)
# MAGIC
# MAGIC One row per calendar month. Used for financial period reporting,
# MAGIC board-level summaries, and MoM growth tracking.
# MAGIC
# MAGIC `mom_revenue_growth` is calculated using `LAG(net_revenue, 1)` over
# MAGIC `ORDER BY year_month` — the same window function pattern used for DoD.

# COMMAND ----------

monthly_df = spark.read.format("delta").load(monthly_path)

print(f"gold_revenue_monthly: {monthly_df.count():,} rows (one per month)")
print()
monthly_df.select(
    "year_month",
    "total_orders",
    "gross_revenue",
    "net_revenue",
    "avg_order_value",
    "unique_customers",
    F.round(F.col("mom_revenue_growth") * 100, 2).alias("mom_growth_pct"),
).orderBy("year_month").show(24, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight: Best and Worst Months

# COMMAND ----------

print("Top 5 months by net revenue:")
(
    monthly_df
    .select(
        "year_month",
        "net_revenue",
        "total_orders",
        F.round(F.col("mom_revenue_growth") * 100, 1).alias("mom_growth_pct"),
    )
    .orderBy(F.desc("net_revenue"))
    .show(5, truncate=False)
)

print("Months with strongest growth:")
(
    monthly_df
    .filter(F.col("mom_revenue_growth").isNotNull())
    .select(
        "year_month",
        "net_revenue",
        F.round(F.col("mom_revenue_growth") * 100, 1).alias("mom_growth_pct"),
    )
    .orderBy(F.desc("mom_revenue_growth"))
    .show(5, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Revenue by Channel (`gold_revenue_by_channel`)
# MAGIC
# MAGIC One row per `(order_date, channel)`. Used by marketing teams to measure
# MAGIC acquisition channel effectiveness and calculate revenue attribution.
# MAGIC
# MAGIC `channel_revenue_share` is computed via a broadcast join against daily totals —
# MAGIC a deliberate performance optimisation documented in `gold_revenue_summary.py`.

# COMMAND ----------

channel_df = spark.read.format("delta").load(channel_path)

print(f"gold_revenue_by_channel: {channel_df.count():,} rows (one per date × channel)")
print()
channel_df.select(
    "order_date",
    "channel",
    "total_orders",
    "gross_revenue",
    "net_revenue",
    "avg_order_value",
    F.round(F.col("channel_revenue_share") * 100, 1).alias("revenue_share_pct"),
).orderBy(F.desc("order_date"), F.desc("net_revenue")).show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight: Channel Revenue Share — Overall Average
# MAGIC
# MAGIC Aggregate across all dates to see each channel's overall contribution.

# COMMAND ----------

print("Overall channel performance (aggregated across all dates):")
(
    channel_df
    .groupBy("channel")
    .agg(
        F.sum("total_orders")                                    .alias("total_orders"),
        F.sum("gross_revenue")                                   .alias("total_gross_revenue"),
        F.sum("net_revenue")                                     .alias("total_net_revenue"),
        F.round(F.avg("avg_order_value"), 2)                     .alias("avg_order_value"),
        F.round(F.avg("channel_revenue_share") * 100, 1)        .alias("avg_daily_share_pct"),
    )
    .orderBy(F.desc("total_net_revenue"))
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight: Channel Breakdown for the Most Recent Day

# COMMAND ----------

latest_date = channel_df.agg(F.max("order_date")).collect()[0][0]
print(f"Channel breakdown for most recent date: {latest_date}")
(
    channel_df
    .filter(F.col("order_date") == latest_date)
    .select(
        "channel",
        "total_orders",
        "net_revenue",
        F.round(F.col("channel_revenue_share") * 100, 1).alias("revenue_share_pct"),
        "avg_order_value",
    )
    .orderBy(F.desc("net_revenue"))
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Table Insight: Order Funnel Summary
# MAGIC
# MAGIC Combine daily metrics to build an executive-level funnel view.
# MAGIC This is the kind of query that runs against Gold tables in production
# MAGIC Databricks SQL dashboards.

# COMMAND ----------

print("Executive summary — overall pipeline health:")
(
    daily_df
    .agg(
        F.sum("total_orders")                                        .alias("total_orders"),
        F.sum("delivered_orders")                                    .alias("delivered"),
        F.sum("cancelled_orders")                                    .alias("cancelled"),
        F.sum("returned_orders")                                     .alias("returned"),
        F.round(F.sum("gross_revenue"), 2)                          .alias("total_gross_revenue"),
        F.round(F.sum("net_revenue"), 2)                            .alias("total_net_revenue"),
        F.round(F.avg("avg_order_value"), 2)                        .alias("overall_avg_order_value"),
        F.round(F.avg("late_delivery_rate") * 100, 2)               .alias("avg_late_delivery_pct"),
        F.sum("unique_customers")                                    .alias("total_customer_days"),
    )
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Release Silver Cache

# COMMAND ----------

silver_df.unpersist()
print("Silver DataFrame unpersisted from cache.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary
# MAGIC
# MAGIC | Table | Grain | Key metrics | Primary use |
# MAGIC |---|---|---|---|
# MAGIC | `gold_revenue_daily` | 1 row per date | Net revenue, DoD growth, late delivery rate | Daily dashboards, anomaly detection |
# MAGIC | `gold_revenue_monthly` | 1 row per month | MoM growth, unique customers | Financial reporting, board summaries |
# MAGIC | `gold_revenue_by_channel` | 1 row per date × channel | Revenue share, channel AOV | Marketing attribution, channel ROI |
# MAGIC
# MAGIC All three tables are written via MERGE INTO — re-running this notebook
# MAGIC updates existing rows rather than creating duplicates.
# MAGIC OPTIMIZE runs after every write to keep read performance fast for BI tools.

# COMMAND ----------

print("✅ Gold revenue pipeline complete.")
print()
print(f"  gold_revenue_daily      : {spark.read.format('delta').load(daily_path).count():,} rows")
print(f"  gold_revenue_monthly    : {spark.read.format('delta').load(monthly_path).count():,} rows")
print(f"  gold_revenue_by_channel : {spark.read.format('delta').load(channel_path).count():,} rows")
print()
print("Pipeline complete: Bronze → Silver → Gold")
print("These tables are ready for Databricks SQL, Tableau, Power BI, or downstream APIs.")
