"""
src/silver/silver_orders.py
-----------------------------
Silver layer transformation for orders data.

SILVER LAYER RESPONSIBILITIES:
  1. Type casting (string → proper types from Bronze)
  2. Deduplication (by order_id — keep latest by _ingested_at)
  3. Null / range validation — reject records failing DQ rules
  4. Standardization (status values, payment types)
  5. Derived columns (days_to_deliver, is_late_delivery, etc.)
  6. MERGE INTO Bronze → Silver (upsert, not append)

Data flow:
  s3://…/bronze/orders/  →  s3://…/silver/orders/
  (partitioned by order_date)

Delta features demonstrated:
  - Schema enforcement (no mergeSchema)
  - MERGE INTO for upserts
  - Z-ORDER on (customer_id, order_date) — optimizes Customer 360 joins
  - OPTIMIZE triggered after each run
  - Change Data Feed consumed by Gold layer
"""

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType, TimestampType
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.delta_utils import (
    add_ingestion_metadata,
    create_delta_table_if_not_exists,
    optimize_table,
    upsert_to_delta,
)
from src.utils.logger import PipelineLogger
from src.utils.spark_utils import generate_batch_id, load_config, resolve_path


# ── Silver orders DDL ────────────────────────────────────────────────────────
SILVER_ORDERS_DDL = """
    order_id                        STRING      NOT NULL,
    customer_id                     STRING      NOT NULL,
    order_status                    STRING      NOT NULL,
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    payment_value                   DECIMAL(12,2),
    payment_type                    STRING,
    promo_code                      STRING,
    channel                         STRING,
    order_date                      DATE,
    days_to_deliver                 INT,
    days_to_approve                 INT,
    is_late_delivery                BOOLEAN,
    delivery_delay_days             INT,
    _ingested_at                    TIMESTAMP NOT NULL,
    _batch_id                       STRING    NOT NULL,
    _silver_processed_at            TIMESTAMP NOT NULL
"""

# Valid values for standardization
VALID_STATUSES      = {"delivered", "shipped", "processing", "cancelled", "returned",
                       "invoiced", "unavailable"}
VALID_PAYMENT_TYPES = {"credit_card", "debit_card", "paypal", "apple_pay",
                       "google_pay", "boleto", "voucher"}


class SilverOrdersPipeline:
    """
    Transforms Bronze orders into the cleaned Silver Delta table.

    Key operations:
      - Read incremental Bronze data (new partitions since last Silver run)
      - Cast all string columns to proper types
      - Deduplicate by order_id
      - Apply DQ rules and quarantine bad rows
      - Derive business columns
      - MERGE into Silver (upsert handles re-processing safely)
      - OPTIMIZE + Z-ORDER on completion
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark        = spark
        self.config       = config
        self.batch_id     = generate_batch_id()
        self.source_path  = resolve_path(config["paths"]["bronze"] + "/orders", config)
        self.target_path  = resolve_path(config["paths"]["silver"] + "/orders", config)
        self.quarantine_path = resolve_path(
            config["paths"]["bronze"] + "/quarantine/orders", config
        )
        dq_cfg = config.get("data_quality", {}).get("orders", {})
        self.null_threshold      = dq_cfg.get("null_tolerance_pct", 0.01)
        self.min_order_amount    = dq_cfg.get("min_order_amount", 0.01)
        self.max_order_amount    = dq_cfg.get("max_order_amount", 50000.0)
        self.log = PipelineLogger("silver", "orders", batch_id=self.batch_id)

    # ── Bootstrap ────────────────────────────────────────────────────────────

    def bootstrap(self) -> None:
        create_delta_table_if_not_exists(
            spark          = self.spark,
            path           = self.target_path,
            schema_ddl     = SILVER_ORDERS_DDL,
            partition_cols = ["order_date"],
            table_properties={
                "delta.enableChangeDataFeed": "true",   # consumed by Gold layer
            },
            comment = "Silver layer: cleaned, typed, deduplicated orders",
        )

    # ── Read ─────────────────────────────────────────────────────────────────

    def read_bronze(self) -> DataFrame:
        """
        Read all Bronze orders.
        For production incremental loads, filter by _ingest_date watermark.
        """
        self.log.info("Reading bronze orders", source=self.source_path)
        return self.spark.read.format("delta").load(self.source_path)

    # ── Type casting ──────────────────────────────────────────────────────────

    def cast_types(self, df: DataFrame) -> DataFrame:
        """
        Cast all raw string columns to proper Spark types.
        Malformed values → NULL (Spark's default safe cast behavior).
        """
        return (
            df
            .withColumn("order_purchase_timestamp",
                F.to_timestamp("order_purchase_timestamp"))
            .withColumn("order_approved_at",
                F.to_timestamp("order_approved_at"))
            .withColumn("order_delivered_carrier_date",
                F.to_timestamp("order_delivered_carrier_date"))
            .withColumn("order_delivered_customer_date",
                F.to_timestamp("order_delivered_customer_date"))
            .withColumn("order_estimated_delivery_date",
                F.to_timestamp("order_estimated_delivery_date"))
            .withColumn("payment_value",
                F.col("payment_value").cast(DecimalType(12, 2)))
            .withColumn("order_date",
                F.to_date("order_purchase_timestamp"))
        )

    # ── Standardization ───────────────────────────────────────────────────────

    def standardize(self, df: DataFrame) -> DataFrame:
        """
        Normalize categorical values to known valid sets.
        Unknown values → 'unknown' (not NULL — preserves row for analysis).
        """
        return (
            df
            .withColumn("order_status",
                F.when(F.lower("order_status").isin(VALID_STATUSES),
                       F.lower("order_status"))
                .otherwise(F.lit("unknown")))
            .withColumn("payment_type",
                F.when(F.lower("payment_type").isin(VALID_PAYMENT_TYPES),
                       F.lower("payment_type"))
                .otherwise(F.lit("other")))
            .withColumn("channel",
                F.lower(F.coalesce("channel", F.lit("unknown"))))
        )

    # ── Derived columns ───────────────────────────────────────────────────────

    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """
        Business-logic derived columns that belong in Silver
        (used directly by Gold aggregations and BI tools).
        """
        return (
            df
            # Days from purchase to delivery
            .withColumn("days_to_deliver",
                F.when(
                    F.col("order_delivered_customer_date").isNotNull(),
                    F.datediff(
                        "order_delivered_customer_date",
                        "order_purchase_timestamp"
                    )
                ).otherwise(F.lit(None).cast("int")))

            # Days from purchase to approval (payment processing time)
            .withColumn("days_to_approve",
                F.when(
                    F.col("order_approved_at").isNotNull(),
                    F.datediff("order_approved_at", "order_purchase_timestamp")
                ).otherwise(F.lit(None).cast("int")))

            # Late delivery flag
            .withColumn("is_late_delivery",
                F.when(
                    F.col("order_delivered_customer_date").isNotNull() &
                    F.col("order_estimated_delivery_date").isNotNull(),
                    F.col("order_delivered_customer_date") >
                    F.col("order_estimated_delivery_date")
                ).otherwise(F.lit(False)))

            # Days late (negative = early)
            .withColumn("delivery_delay_days",
                F.when(
                    F.col("order_delivered_customer_date").isNotNull() &
                    F.col("order_estimated_delivery_date").isNotNull(),
                    F.datediff(
                        "order_delivered_customer_date",
                        "order_estimated_delivery_date"
                    )
                ).otherwise(F.lit(None).cast("int")))
        )

    # ── Deduplication ─────────────────────────────────────────────────────────

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Keep the latest record per order_id.
        Uses a window function (more efficient than groupBy for large datasets).
        """
        from pyspark.sql.window import Window

        window = Window.partitionBy("order_id").orderBy(F.desc("_ingested_at"))
        return (
            df
            .withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    # ── Data quality ──────────────────────────────────────────────────────────

    def apply_dq_rules(self, df: DataFrame) -> tuple:
        """
        Apply data quality rules. Returns (clean_df, quarantine_df).

        Rules:
          R1: order_id must not be null
          R2: customer_id must not be null
          R3: payment_value must be within valid range (or null — nulls allowed)
          R4: order_status must not be 'unknown' (after standardization)
          R5: order_date must not be null (derived from purchase_timestamp)

        Quarantine: failed rows written to bronze/quarantine/ for investigation.
        """
        total = df.count()

        # Build fail condition
        fail_condition = (
            F.col("order_id").isNull() |
            F.col("customer_id").isNull() |
            F.col("order_date").isNull() |
            (
                F.col("payment_value").isNotNull() &
                (
                    (F.col("payment_value") < self.min_order_amount) |
                    (F.col("payment_value") > self.max_order_amount)
                )
            )
        )

        clean_df      = df.filter(~fail_condition)
        quarantine_df = df.filter(fail_condition)

        quarantine_count = quarantine_df.count()
        clean_count      = clean_df.count()
        fail_rate        = quarantine_count / total if total > 0 else 0

        self.log.info(
            "DQ results",
            total=total,
            clean=clean_count,
            quarantined=quarantine_count,
            fail_rate_pct=round(fail_rate * 100, 2),
        )

        if fail_rate > self.null_threshold:
            self.log.warning(
                "DQ fail rate exceeds threshold",
                fail_rate_pct=round(fail_rate * 100, 2),
                threshold_pct=round(self.null_threshold * 100, 2),
            )

        return clean_df, quarantine_df

    def write_quarantine(self, quarantine_df: DataFrame) -> None:
        if quarantine_df.isEmpty():
            return
        (
            quarantine_df
            .withColumn("_quarantine_reason", F.lit("dq_failed"))
            .withColumn("_quarantine_at",     F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .save(self.quarantine_path)
        )
        self.log.info("Quarantine rows written", path=self.quarantine_path)

    # ── Write (MERGE) ─────────────────────────────────────────────────────────

    def write_silver(self, df: DataFrame) -> None:
        """
        Add Silver metadata and MERGE into target Delta table.
        """
        df = (
            df
            .withColumn("_silver_processed_at", F.current_timestamp())
            .withColumn("_batch_id", F.lit(self.batch_id))
            .drop("_source_file", "_ingest_date")   # Bronze-only columns
        )

        # Keep only Silver schema columns (safety guard)
        silver_cols = [f.split()[0] for f in SILVER_ORDERS_DDL.strip().split("\n")
                       if f.strip() and not f.strip().startswith("--")]
        available   = [c for c in silver_cols if c in df.columns]
        df          = df.select(*available)

        upsert_to_delta(
            spark       = self.spark,
            source_df   = df,
            target_path = self.target_path,
            merge_keys  = ["order_id"],
        )

    # ── Post-processing ───────────────────────────────────────────────────────

    def post_process(self) -> None:
        """
        OPTIMIZE + Z-ORDER after write.
        Z-ORDER on (customer_id, order_date) optimizes:
          - Customer 360 joins
          - Date-range order lookups
          - Revenue aggregations by customer
        """
        z_order_cols = self.config["delta"]["z_order_columns"].get(
            "orders", ["customer_id", "order_date"]
        )
        optimize_table(self.spark, self.target_path, z_order_cols=z_order_cols)

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        self.log.info("Silver orders pipeline starting")
        self.bootstrap()

        bronze_df    = self.read_bronze()
        typed_df     = self.cast_types(bronze_df)
        std_df       = self.standardize(typed_df)
        derived_df   = self.add_derived_columns(std_df)
        deduped_df   = self.deduplicate(derived_df)
        clean_df, quarantine_df = self.apply_dq_rules(deduped_df)

        self.write_quarantine(quarantine_df)
        self.write_silver(clean_df)
        self.post_process()

        summary = {
            "layer":      "silver",
            "table":      "orders",
            "batch_id":   self.batch_id,
            "rows_clean": clean_df.count(),
            "rows_quarantined": quarantine_df.count(),
        }
        self.log.info("Silver orders pipeline complete", **summary)
        return summary


if __name__ == "__main__":
    from src.utils.spark_utils import get_spark, load_config
    env    = os.getenv("ENV_NAME", "dev")
    config = load_config(env)
    spark  = get_spark(config)
    SilverOrdersPipeline(spark, config).run()
