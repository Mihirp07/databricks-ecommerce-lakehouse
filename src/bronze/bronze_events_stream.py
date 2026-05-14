"""
src/bronze/bronze_events_stream.py
------------------------------------
Bronze streaming pipeline — clickstream events via Spark Structured Streaming.

ARCHITECTURE:
  [Raw JSON files (simulated Kafka/S3 event drop)]
       ↓  Auto Loader (cloudFiles)
  [Bronze Delta Table: append-only, partitioned by event_date]
       ↓  downstream Silver streaming job picks up via CDF

WHY AUTO LOADER vs readStream("kafka"):
  Auto Loader (cloudFiles format) is the Databricks-native way to
  efficiently ingest files from cloud storage as a stream. It tracks
  which files have been processed using a checkpoint, scales to
  millions of files, and supports schema inference/enforcement.

  In a real deployment:
    - Kafka → Confluent Connector → S3 → Auto Loader   (AWS)
    - Event Hub → ADLS Gen2 → Auto Loader              (Azure)
  
  In dev/free mode:
    - generate_sample_data.py drops JSON files to /tmp/lakehouse/raw/clickstream/
    - Auto Loader streams from that directory

Cloud paths flow:
  source:     s3://ecommerce-lakehouse-prod/raw/clickstream/
  target:     s3://ecommerce-lakehouse-prod/bronze/events/
  checkpoint: s3://ecommerce-lakehouse-prod/checkpoints/bronze_events/
"""

import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.delta_utils import create_delta_table_if_not_exists
from src.utils.logger import PipelineLogger
from src.utils.spark_utils import generate_batch_id, load_config, resolve_path


# ── Schema for incoming JSON events ─────────────────────────────────────────
# Explicitly defined — never rely on Auto Loader schema inference in production
EVENT_SCHEMA = StructType([
    StructField("event_id",        StringType(), nullable=False),
    StructField("session_id",      StringType(), nullable=True),
    StructField("customer_id",     StringType(), nullable=True),
    StructField("event_type",      StringType(), nullable=True),
    StructField("event_timestamp", StringType(), nullable=True),
    StructField("product_id",      StringType(), nullable=True),
    StructField("page_url",        StringType(), nullable=True),
    StructField("referrer",        StringType(), nullable=True),
    StructField("device_type",     StringType(), nullable=True),
    StructField("user_agent",      StringType(), nullable=True),
    StructField("ip_address",      StringType(), nullable=True),
    StructField("country_code",    StringType(), nullable=True),
])

BRONZE_EVENTS_DDL = """
    event_id        STRING NOT NULL,
    session_id      STRING,
    customer_id     STRING,
    event_type      STRING,
    event_timestamp STRING,
    product_id      STRING,
    page_url        STRING,
    referrer        STRING,
    device_type     STRING,
    user_agent      STRING,
    ip_address      STRING,
    country_code    STRING,
    _ingested_at    TIMESTAMP NOT NULL,
    _source_file    STRING,
    _batch_id       STRING NOT NULL,
    event_date      DATE
"""


class BronzeEventsStreamPipeline:
    """
    Structured Streaming pipeline: JSON files → Bronze Delta (events).

    Supports two trigger modes:
      - continuous:  spark.readStream + writeStream.trigger(processingTime=...)
      - once:        trigger(availableNow=True) — process all available files, then stop
                     (used in scheduled Databricks Jobs for micro-batch pattern)

    Usage:
        pipeline = BronzeEventsStreamPipeline(spark, config)
        query    = pipeline.run(trigger_once=False)   # continuous
        query.awaitTermination()
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark       = spark
        self.config      = config
        self.batch_id    = generate_batch_id()
        self.source_path = resolve_path(
            config["paths"]["raw"] + "/clickstream", config
        )
        self.target_path = resolve_path(
            config["paths"]["bronze"] + "/events", config
        )
        self.checkpoint_path = resolve_path(
            config["paths"]["checkpoint"] + "/bronze_events", config
        )
        self.log = PipelineLogger("bronze", "events_stream", batch_id=self.batch_id)

    # ── Bootstrap ────────────────────────────────────────────────────────────

    def bootstrap(self) -> None:
        create_delta_table_if_not_exists(
            spark          = self.spark,
            path           = self.target_path,
            schema_ddl     = BRONZE_EVENTS_DDL,
            partition_cols = ["event_date"],
            comment        = "Bronze layer: raw clickstream events — streaming append-only",
        )

    # ── Read stream ──────────────────────────────────────────────────────────

    def read_stream(self) -> DataFrame:
        """
        Read from cloud storage using Auto Loader (cloudFiles).

        cloudFiles features used:
          - cloudFiles.format       → input file format (json)
          - cloudFiles.schemaLocation → persists inferred/provided schema
          - cloudFiles.includeExistingFiles → process historical files on first run
          - rescuedDataColumn       → captures any extra/unexpected fields
        
        In production (AWS): source is s3://bucket/raw/clickstream/
        In dev simulation:   source is /tmp/lakehouse/raw/clickstream/
        """
        self.log.info("Starting Auto Loader stream", source=self.source_path)

        schema_location = resolve_path(
            self.config["paths"]["checkpoint"] + "/bronze_events_schema", self.config
        )

        # Auto Loader read — the recommended Databricks ingestion pattern
        # Falls back to regular readStream for non-Databricks environments
        try:
            df = (
                self.spark.readStream
                .format("cloudFiles")                                # Auto Loader
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", schema_location)
                .option("cloudFiles.includeExistingFiles", "true")
                .option("rescuedDataColumn", "_rescued_data")
                .schema(EVENT_SCHEMA)
                .load(self.source_path)
            )
        except Exception:
            # Fallback: standard Structured Streaming (non-Databricks Spark)
            self.log.warning("Auto Loader unavailable — falling back to readStream.json")
            df = (
                self.spark.readStream
                .schema(EVENT_SCHEMA)
                .option("maxFilesPerTrigger", 100)
                .json(self.source_path)
            )

        return df

    # ── Transform ────────────────────────────────────────────────────────────

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Bronze streaming transforms — metadata only, no business logic.
        """
        return (
            df
            .withColumn("_ingested_at",  F.current_timestamp())
            .withColumn("_source_file",  F.input_file_name())
            .withColumn("_batch_id",     F.lit(self.batch_id))
            .withColumn("event_date",
                F.to_date(
                    F.to_timestamp(F.col("event_timestamp"))
                )
            )
        )

    # ── Write stream ─────────────────────────────────────────────────────────

    def _write_batch(self, batch_df: DataFrame, batch_id_int: int) -> None:
        """
        foreachBatch writer — gives full control over each micro-batch.
        Allows us to deduplicate within the batch before writing to Delta.
        """
        if batch_df.isEmpty():
            return

        # Intra-batch deduplication (same event_id in same micro-batch)
        deduped = batch_df.dropDuplicates(["event_id"])

        row_count = deduped.count()
        self.log.info(
            "Writing micro-batch to bronze",
            spark_batch_id=batch_id_int,
            rows=row_count,
        )

        (
            deduped.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .option("mergeSchema", "false")
            .save(self.target_path)
        )

    def write_stream(
        self,
        df: DataFrame,
        trigger_once: bool = False,
        trigger_interval: Optional[str] = None,
    ):
        """
        Configure and start the writeStream.

        Args:
            df:               Transformed streaming DataFrame
            trigger_once:     Use availableNow=True (batch mode, good for scheduled jobs)
            trigger_interval: Processing time interval e.g. "30 seconds"

        Returns:
            StreamingQuery object
        """
        interval = trigger_interval or self.config["streaming"]["trigger_interval"]

        writer = (
            df.writeStream
            .foreachBatch(self._write_batch)
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
        )

        if trigger_once:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=interval)

        query = writer.start()
        self.log.info(
            "Streaming query started",
            query_id=query.id,
            trigger_once=trigger_once,
        )
        return query

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self, trigger_once: bool = False):
        """
        Execute the full streaming pipeline.

        Args:
            trigger_once: If True, process all available files then stop.
                          If False, run continuously.

        Returns:
            StreamingQuery object.
        """
        self.log.info(
            "Bronze events streaming pipeline starting",
            trigger_once=trigger_once,
        )
        self.bootstrap()
        raw_stream         = self.read_stream()
        transformed_stream = self.transform(raw_stream)
        query              = self.write_stream(transformed_stream, trigger_once=trigger_once)
        return query


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from src.utils.spark_utils import get_spark, load_config
    env    = os.getenv("ENV_NAME", "dev")
    config = load_config(env)
    spark  = get_spark(config)

    pipeline = BronzeEventsStreamPipeline(spark, config)
    query    = pipeline.run(trigger_once=True)
    query.awaitTermination()
