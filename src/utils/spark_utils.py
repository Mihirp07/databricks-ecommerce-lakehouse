"""
src/utils/spark_utils.py
------------------------
SparkSession factory and cloud path resolver.

KEY DESIGN:
  All pipeline code uses cloud-style paths (s3:// or abfss://).
  resolve_path() transparently remaps those to DBFS or local paths
  when running in dev/free mode. Production code is IDENTICAL —
  only the config flag changes.

Cloud path examples:
  s3://ecommerce-lakehouse-prod/bronze/orders
  abfss://bronze@ecommercedatalake.dfs.core.windows.net/orders

Dev simulation:
  → dbfs:/FileStore/lakehouse/bronze/orders
  → /tmp/lakehouse/bronze/orders  (unit tests, no Databricks)
"""

import os
import re
import uuid
from typing import Any, Dict, Optional

import yaml
from pyspark.sql import SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(env: Optional[str] = None) -> Dict[str, Any]:
    """
    Load YAML config for the given environment.

    Args:
        env: 'dev' | 'prod'. Falls back to ENV_NAME env var, then 'dev'.

    Returns:
        Parsed config dict.
    """
    env = env or os.getenv("ENV_NAME", "dev")
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "configs", f"{env}.yml"
    )
    config_path = os.path.normpath(config_path)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.info(f"Loaded config: env={env}, cloud_provider={config.get('cloud_provider')}")
    return config


# ---------------------------------------------------------------------------
# Cloud path resolver
# ---------------------------------------------------------------------------

_S3_PATTERN    = re.compile(r"^s3a?://([^/]+)(.*)")
_ABFSS_PATTERN = re.compile(r"^abfss://([^@]+)@([^/]+)(.*)")


def resolve_path(cloud_path: str, config: Dict[str, Any]) -> str:
    """
    Resolve a cloud-style path to the actual storage path for this environment.

    In production (simulate_cloud_paths=False):
        's3://ecommerce-lakehouse-prod/bronze/orders'
        → 's3://ecommerce-lakehouse-prod/bronze/orders'   (unchanged)

    In dev (simulate_cloud_paths=True):
        's3://ecommerce-lakehouse-prod/bronze/orders'
        → 'dbfs:/FileStore/lakehouse/bronze/orders'

    For unit tests (local_root set, no Databricks):
        → '/tmp/lakehouse/bronze/orders'

    Args:
        cloud_path: The canonical cloud URI (s3:// or abfss://)
        config:     Loaded environment config dict.

    Returns:
        Resolved path string safe to pass to Spark.
    """
    if not config.get("simulate_cloud_paths", True):
        # Production — use cloud path as-is
        return cloud_path

    use_local = config["storage"].get("local_root") and _is_local_test()
    root = (
        config["storage"]["local_root"]
        if use_local
        else config["storage"]["dbfs_root"]
    )

    # Strip the bucket/container prefix, keep the path suffix
    m_s3 = _S3_PATTERN.match(cloud_path)
    if m_s3:
        suffix = m_s3.group(2)  # e.g. /bronze/orders
        resolved = f"{root.rstrip('/')}{suffix}"
        logger.debug(f"Resolved s3 path: {cloud_path} → {resolved}")
        return resolved

    m_abfss = _ABFSS_PATTERN.match(cloud_path)
    if m_abfss:
        container = m_abfss.group(1)   # e.g. bronze
        suffix    = m_abfss.group(3)   # e.g. /orders
        resolved  = f"{root.rstrip('/')}/{container}{suffix}"
        logger.debug(f"Resolved abfss path: {cloud_path} → {resolved}")
        return resolved

    # Already a local/dbfs path — return unchanged
    return cloud_path


def _is_local_test() -> bool:
    """True when running outside Databricks (pytest, local Spark)."""
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is None


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

def get_spark(config: Optional[Dict[str, Any]] = None, app_name: str = "EcommerceLakehouse") -> SparkSession:
    """
    Build or retrieve the active SparkSession with Delta Lake and
    performance settings applied from config.

    In Databricks the session already exists — this simply configures it.
    Outside Databricks (local / CI) it creates a new local session.

    Args:
        config:   Loaded env config. If None, loads dev config automatically.
        app_name: Spark application name.

    Returns:
        Configured SparkSession.
    """
    if config is None:
        config = load_config()

    spark_cfg   = config.get("spark", {})
    delta_cfg   = config.get("delta", {})
    catalog_cfg = config.get("catalog", {})

    try:
        # Inside Databricks — grab the existing session
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active session")
        logger.info("Using existing Databricks SparkSession")
    except Exception:
        # Outside Databricks — create a local session (for tests / dev laptop)
        logger.info("Creating local SparkSession (non-Databricks environment)")
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        spark = builder.getOrCreate()

    # ── Performance tuning ──────────────────────────────────────────────────
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(spark_cfg.get("shuffle_partitions", 8)),
    )
    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        str(spark_cfg.get("broadcast_threshold_mb", 20) * 1024 * 1024),
    )
    spark.conf.set(
        "spark.sql.adaptive.enabled",
        str(spark_cfg.get("adaptive_enabled", True)).lower(),
    )

    # ── Delta Lake settings ─────────────────────────────────────────────────
    spark.conf.set(
        "spark.databricks.delta.optimizeWrite.enabled",
        str(delta_cfg.get("optimize_write", True)).lower(),
    )
    spark.conf.set(
        "spark.databricks.delta.autoCompact.enabled",
        str(delta_cfg.get("auto_compact", True)).lower(),
    )
    spark.conf.set(
        "spark.databricks.delta.properties.defaults.deletedFileRetentionDuration",
        f"interval {delta_cfg.get('retention_hours', 168)} hours",
    )

    # ── Log level ───────────────────────────────────────────────────────────
    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))

    logger.info(
        "SparkSession configured",
        extra={
            "shuffle_partitions": spark_cfg.get("shuffle_partitions"),
            "adaptive":           spark_cfg.get("adaptive_enabled"),
            "optimize_write":     delta_cfg.get("optimize_write"),
        },
    )
    return spark


# ---------------------------------------------------------------------------
# Batch ID generator
# ---------------------------------------------------------------------------

def generate_batch_id() -> str:
    """
    Generate a short unique batch identifier for lineage tracking.
    Format: first 8 chars of a UUID4  e.g. 'a3f9c12b'
    """
    return uuid.uuid4().hex[:8]
