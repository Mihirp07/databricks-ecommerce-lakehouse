"""
src/utils/delta_utils.py
------------------------
Reusable Delta Lake operations used across all pipeline layers.

Covers:
  - Table creation with schema enforcement
  - Upsert (MERGE INTO) with SCD Type 1
  - OPTIMIZE + Z-ORDER
  - VACUUM
  - Time travel queries
  - Table properties management
  - Partition pruning helpers
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import get_logger
from src.utils.spark_utils import resolve_path

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------

def create_delta_table_if_not_exists(
    spark: SparkSession,
    path: str,
    schema_ddl: str,
    partition_cols: Optional[List[str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
    comment: str = "",
) -> None:
    """
    Create a Delta table at `path` if it doesn't already exist.
    Uses schema enforcement (no schema evolution unless explicitly allowed).

    Args:
        spark:            SparkSession
        path:             Resolved storage path (DBFS, s3://, abfss://)
        schema_ddl:       DDL string e.g. "id STRING NOT NULL, ts TIMESTAMP"
        partition_cols:   Partition columns list
        table_properties: Extra Delta properties dict
        comment:          Table description
    """
    if DeltaTable.isDeltaTable(spark, path):
        logger.info(f"Delta table already exists at: {path}")
        return

    props = {
        "delta.enableChangeDataFeed": "true",      # enable CDF for downstream CDC
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        **(table_properties or {}),
    }

    props_sql = "\n".join(
        f"  '{k}' = '{v}'," for k, v in props.items()
    ).rstrip(",")

    partition_sql = (
        f"PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
    )

    ddl = f"""
        CREATE TABLE delta.`{path}` (
          {schema_ddl}
        )
        USING DELTA
        {partition_sql}
        TBLPROPERTIES (
          {props_sql}
        )
        COMMENT '{comment}'
    """
    spark.sql(ddl)
    logger.info(f"Created Delta table at: {path}", extra={"partitions": partition_cols})


# ---------------------------------------------------------------------------
# Upsert (SCD Type 1 MERGE)
# ---------------------------------------------------------------------------

def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: List[str],
    update_cols: Optional[List[str]] = None,
    insert_all: bool = True,
) -> Dict[str, int]:
    """
    Perform a MERGE (upsert) into a Delta table — SCD Type 1.

    Matched rows → UPDATE (all columns, or `update_cols` subset)
    Unmatched rows → INSERT

    Args:
        spark:        SparkSession
        source_df:    Incoming DataFrame
        target_path:  Path to target Delta table
        merge_keys:   Columns to match on e.g. ["order_id"]
        update_cols:  Subset of columns to update. None = update all.
        insert_all:   If True, insert unmatched source rows.

    Returns:
        Dict with row counts: {"updated": N, "inserted": N}
    """
    target = DeltaTable.forPath(spark, target_path)

    # Build merge condition
    condition = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )

    # Build SET clause for update
    if update_cols:
        set_clause = {col: f"source.{col}" for col in update_cols}
    else:
        set_clause = {col: f"source.{col}" for col in source_df.columns}

    merger = (
        target.alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdate(set=set_clause)
    )

    if insert_all:
        merger = merger.whenNotMatchedInsertAll()

    merger.execute()

    logger.info(
        "Upsert complete",
        extra={"target": target_path, "merge_keys": merge_keys},
    )
    # Note: DeltaTable.merge() doesn't return row counts directly.
    # Use table history for audit; we return placeholder here.
    return {"updated": -1, "inserted": -1}


# ---------------------------------------------------------------------------
# OPTIMIZE + Z-ORDER
# ---------------------------------------------------------------------------

def optimize_table(
    spark: SparkSession,
    path: str,
    z_order_cols: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
) -> None:
    """
    Run OPTIMIZE on a Delta table, optionally with Z-ORDER.

    Z-ORDER co-locates related data in the same files, dramatically
    reducing the amount of data scanned for filtered queries.

    Args:
        spark:         SparkSession
        path:          Delta table path
        z_order_cols:  Columns to Z-ORDER by (max ~4 for best results)
        where_clause:  Partition filter e.g. "order_date = '2024-01-01'"
                       Limits OPTIMIZE to a specific partition.
    """
    where_sql   = f"WHERE {where_clause}" if where_clause else ""
    z_order_sql = f"ZORDER BY ({', '.join(z_order_cols)})" if z_order_cols else ""

    sql = f"OPTIMIZE delta.`{path}` {where_sql} {z_order_sql}"
    logger.info(f"Running OPTIMIZE: {sql}")
    spark.sql(sql)
    logger.info("OPTIMIZE complete", extra={"path": path, "z_order": z_order_cols})


# ---------------------------------------------------------------------------
# VACUUM
# ---------------------------------------------------------------------------

def vacuum_table(
    spark: SparkSession,
    path: str,
    retention_hours: int = 168,
    dry_run: bool = False,
) -> None:
    """
    VACUUM a Delta table to remove files older than retention_hours.

    WARNING: Setting retention_hours < 168 (7 days) breaks concurrent
    reads and time travel. Only reduce in dev with dry_run=True first.

    Args:
        spark:            SparkSession
        path:             Delta table path
        retention_hours:  Retention window in hours (default 168 = 7 days)
        dry_run:          If True, list files that WOULD be deleted (no-op).
    """
    if dry_run:
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "false")

    dry_run_sql = "DRY RUN" if dry_run else ""
    sql = f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS {dry_run_sql}"
    logger.info(f"Running VACUUM: {sql}")
    spark.sql(sql)
    logger.info("VACUUM complete", extra={"path": path, "retention_hours": retention_hours})


# ---------------------------------------------------------------------------
# Time travel
# ---------------------------------------------------------------------------

def read_at_version(
    spark: SparkSession,
    path: str,
    version: int,
) -> DataFrame:
    """
    Read a Delta table at a specific version (time travel).

    Useful for:
      - Reproducing historical pipeline results
      - Auditing data changes
      - Rolling back bad writes

    Args:
        spark:   SparkSession
        path:    Delta table path
        version: Delta version number

    Returns:
        DataFrame at the specified version.
    """
    logger.info(f"Time travel read: path={path}, version={version}")
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def read_at_timestamp(
    spark: SparkSession,
    path: str,
    ts: datetime,
) -> DataFrame:
    """
    Read a Delta table as it existed at a specific timestamp.

    Args:
        spark: SparkSession
        path:  Delta table path
        ts:    Target datetime (UTC)

    Returns:
        DataFrame at the specified timestamp.
    """
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Time travel read: path={path}, timestamp={ts_str}")
    return spark.read.format("delta").option("timestampAsOf", ts_str).load(path)


def get_table_history(
    spark: SparkSession,
    path: str,
    limit: int = 20,
) -> DataFrame:
    """
    Return the Delta table history (DESCRIBE HISTORY).

    Shows: version, timestamp, operation, operationMetrics, userMetadata.

    Args:
        spark: SparkSession
        path:  Delta table path
        limit: Number of history entries to return

    Returns:
        DataFrame with history records.
    """
    return spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT {limit}")


# ---------------------------------------------------------------------------
# Partition helpers
# ---------------------------------------------------------------------------

def add_ingestion_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """
    Append standard pipeline metadata columns to a DataFrame.
    These columns are required for every Delta table in the lakehouse.

    Columns added:
        _ingested_at  — UTC timestamp of pipeline execution
        _batch_id     — Pipeline run identifier (for lineage)

    Args:
        df:       Input DataFrame
        batch_id: Current pipeline batch ID

    Returns:
        DataFrame with metadata columns appended.
    """
    return df.withColumn(
        "_ingested_at", F.current_timestamp()
    ).withColumn(
        "_batch_id", F.lit(batch_id)
    )


def get_latest_partition_value(
    spark: SparkSession,
    path: str,
    partition_col: str,
) -> Optional[Any]:
    """
    Return the maximum value of a partition column.
    Used for incremental loads — read only data newer than last partition.

    Args:
        spark:         SparkSession
        path:          Delta table path
        partition_col: Partition column name e.g. "order_date"

    Returns:
        Maximum partition value, or None if table is empty.
    """
    try:
        result = (
            spark.read.format("delta")
            .load(path)
            .agg(F.max(F.col(partition_col)))
            .collect()[0][0]
        )
        logger.info(f"Latest partition value: {partition_col}={result}")
        return result
    except Exception as e:
        logger.warning(f"Could not read latest partition: {e}")
        return None
