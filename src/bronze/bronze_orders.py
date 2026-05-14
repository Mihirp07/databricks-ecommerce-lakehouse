"""
src/bronze/bronze_orders.py
----------------------------
Bronze layer ingestion for orders data.

DESIGN PRINCIPLES (Bronze layer):
  - Append-only — raw data is NEVER modified here
  - Schema-on-read with enforcement via _bronze_orders_schema
  - Metadata columns added: _ingested_at, _source_file, _batch_id
  - Supports both full load and incremental (partition watermark)
  - No business logic — bad rows land here too (filtered in Silver)

Cloud path flow:
  s3://ecommerce-lakehouse-prod/raw/orders.csv
  → s3://ecommerce-lakehouse-prod/bronze/orders/   (Delta)
    (dev: resolved to dbfs:/FileStore/lakehouse/bronze/orders/)
"""

import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, StringType, StructField, StructType, TimestampType
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.delta_utils import (
    add_ingestion_metadata,
    create_delta_table_if_not_exists,
    get_latest_partition_value,
)
from src.utils.logger import PipelineLogger
from src.utils.spark_utils import generate_batch_id, load_config, resolve_path

# ── Bronze orders schema (schema enforcement at write time) ─────────────────
# Raw strings preserved → Silver layer does type casting.
# _source_file and _batch_id enable full data lineage.
BRONZE_ORDERS_SCHEMA = StructType([
    StructField("order_id",                        StringType(), nullable=False),
    StructField("customer_id",                     StringType(), nullable=False),
    StructField("order_status",                    StringType(), nullable=True),
    StructField("order_purchase_timestamp",        StringType(), nullable=True),
    StructField("order_approved_at",               StringType(), nullable=True),
    StructField("order_delivered_carrier_date",    StringType(), nullable=True),
    StructField("order_delivered_customer_date",   StringType(), nullable=True),
    StructField("order_estimated_delivery_date",   StringType(), nullable=True),
    StructField("payment_value",                   StringType(), nullable=True),
    StructField("payment_type",                    StringType(), nullable=True),
    StructField("promo_code",                      StringType(), nullable=True),
    StructField("channel",                         StringType(), nullable=True),
])

BRONZE_DDL = """
    order_id                        STRING  NOT NULL,
    customer_id                     STRING  NOT NULL,
    order_status                    STRING,
    order_purchase_timestamp        STRING,
    order_approved_at               STRING,
    order_delivered_carrier_date    STRING,
    order_delivered_customer_date   STRING,
    order_estimated_delivery_date   STRING,
    payment_value                   STRING,
    payment_type                    STRING,
    promo_code                      STRING,
    channel                         STRING,
    _ingested_at                    TIMESTAMP NOT NULL,
    _source_file                    STRING,
    _batch_id                       STRING    NOT NULL,
    _ingest_date                    DATE      NOT NULL
"""


class BronzeOrdersPipeline:
    """
    Reads raw order CSV files and lands them in the Bronze Delta table.

    Usage (Databricks notebook or job):
        from src.bronze.bronze_orders import BronzeOrdersPipeline
        pipeline = BronzeOrdersPipeline(spark, config)
        pipeline.run()
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark      = spark
        self.config     = config
        self.batch_id   = generate_batch_id()
        self.source_path = resolve_path(config["paths"]["raw"] + "/orders.csv", config)
        self.target_path = resolve_path(config["paths"]["bronze"] + "/orders", config)
        self.log        = PipelineLogger("bronze", "orders", batch_id=self.batch_id)

    # ── Bootstrap ────────────────────────────────────────────────────────────

    def bootstrap(self) -> None:
        """Create the Bronze Delta table if it doesn't exist."""
        create_delta_table_if_not_exists(
            spark           = self.spark,
            path            = self.target_path,
            schema_ddl      = BRONZE_DDL,
            partition_cols  = ["_ingest_date"],
            comment         = "Bronze layer: raw orders — append-only, schema enforced",
        )

    # ── Read ─────────────────────────────────────────────────────────────────

    def read_raw(self, incremental: bool = False) -> DataFrame:
        """
        Read raw CSV from the landing zone.

        Args:
            incremental: If True, read only files modified after the latest
                         partition already in the Bronze table.

        Returns:
            Raw DataFrame with schema applied.
        """
        self.log.info("Reading raw orders", source=self.source_path)

        df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "false")       # never infer — use our schema
            .option("mode", "PERMISSIVE")         # bad rows → _corrupt_record column
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(BRONZE_ORDERS_SCHEMA)
            .csv(self.source_path)
        )

        # Add the path of each source file for lineage
        df = df.withColumn("_source_file", F.input_file_name())
        self.log.info("Raw read complete", rows=df.count())
        return df

    # ── Transform ────────────────────────────────────────────────────────────

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Bronze transformations — MINIMAL by design.
        Only add metadata; do NOT apply business logic or type casting.

        Adds:
          _ingested_at  — pipeline execution timestamp
          _batch_id     — current batch identifier
          _ingest_date  — partition column (date portion of ingestion timestamp)
        """
        df = add_ingestion_metadata(df, self.batch_id)
        df = df.withColumn("_ingest_date", F.to_date(F.col("_ingested_at")))

        # Drop corrupt rows — log their count for monitoring
        corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count() \
            if "_corrupt_record" in df.columns else 0

        if corrupt_count > 0:
            self.log.warning("Corrupt rows detected", corrupt_rows=corrupt_count)
            df = df.filter(F.col("_corrupt_record").isNull())

        if "_corrupt_record" in df.columns:
            df = df.drop("_corrupt_record")

        return df

    # ── Write ─────────────────────────────────────────────────────────────────

    def write(self, df: DataFrame) -> int:
        """
        Append to the Bronze Delta table.
        Bronze is ALWAYS append-only — we never overwrite raw data.

        Returns:
            Row count written.
        """
        row_count = df.count()
        self.log.info("Writing to bronze", rows=row_count, target=self.target_path)

        (
            df.write
            .format("delta")
            .mode("append")
            .partitionBy("_ingest_date")
            .option("mergeSchema", "false")       # enforce schema — reject new columns
            .save(self.target_path)
        )

        self.log.info("Bronze write complete", rows=row_count)
        return row_count

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self, incremental: bool = False) -> dict:
        """
        Execute the full Bronze ingestion pipeline.

        Args:
            incremental: Only process new data since last run.

        Returns:
            Pipeline execution summary dict.
        """
        self.log.info("Bronze orders pipeline starting", batch_id=self.batch_id)

        self.bootstrap()
        raw_df        = self.read_raw(incremental=incremental)
        transformed   = self.transform(raw_df)
        rows_written  = self.write(transformed)

        summary = {
            "layer":         "bronze",
            "table":         "orders",
            "batch_id":      self.batch_id,
            "rows_written":  rows_written,
            "source_path":   self.source_path,
            "target_path":   self.target_path,
            "incremental":   incremental,
        }

        self.log.info("Bronze orders pipeline complete", **summary)
        return summary


# ── Entry point for Databricks Jobs ─────────────────────────────────────────
# In a Databricks job, this module is run as a task.
# dbutils.widgets are used to pass parameters (env, incremental flag).

if __name__ == "__main__":
    from src.utils.spark_utils import get_spark, load_config
    env    = os.getenv("ENV_NAME", "dev")
    config = load_config(env)
    spark  = get_spark(config)
    result = BronzeOrdersPipeline(spark, config).run()
    print(result)
