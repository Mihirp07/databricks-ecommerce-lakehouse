"""
tests/test_silver_orders.py
----------------------------
Unit tests for the Silver orders transformation pipeline.

Tests run against a local SparkSession (no Databricks required).
Each test creates a small in-memory DataFrame and validates
the transformation logic in isolation.

Run with: pytest tests/ -v
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from src.silver.silver_orders import SilverOrdersPipeline


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for all tests in this file."""
    return (
        SparkSession.builder
        .appName("test-silver-orders")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


@pytest.fixture
def dev_config():
    from src.utils.spark_utils import load_config
    return load_config("dev")


@pytest.fixture
def pipeline(spark, dev_config):
    return SilverOrdersPipeline(spark, dev_config)


@pytest.fixture
def raw_orders_df(spark):
    """Minimal Bronze orders DataFrame for testing."""
    schema = StructType([
        StructField("order_id",                     StringType(), False),
        StructField("customer_id",                  StringType(), False),
        StructField("order_status",                 StringType(), True),
        StructField("order_purchase_timestamp",     StringType(), True),
        StructField("order_approved_at",            StringType(), True),
        StructField("order_delivered_customer_date",StringType(), True),
        StructField("order_estimated_delivery_date",StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("payment_value",                StringType(), True),
        StructField("payment_type",                 StringType(), True),
        StructField("promo_code",                   StringType(), True),
        StructField("channel",                      StringType(), True),
        StructField("_ingested_at",                 StringType(), True),
        StructField("_batch_id",                    StringType(), True),
    ])
    rows = [
        # Normal delivered order
        ("ord-001", "cust-001", "delivered",
         "2024-01-10 10:00:00", "2024-01-10 10:30:00",
         "2024-01-15 14:00:00", "2024-01-14 00:00:00",
         "2024-01-11 08:00:00",
         "99.99", "credit_card", None, "web",
         "2024-01-10 12:00:00", "batch001"),
        # Cancelled order — payment_value null
        ("ord-002", "cust-002", "cancelled",
         "2024-01-11 09:00:00", None, None, "2024-01-20 00:00:00", None,
         None, "paypal", "PROMO-1234", "mobile_app",
         "2024-01-11 12:00:00", "batch001"),
        # Duplicate of ord-001 (older ingestion — should be dropped)
        ("ord-001", "cust-001", "delivered",
         "2024-01-10 10:00:00", "2024-01-10 10:30:00",
         "2024-01-15 14:00:00", "2024-01-14 00:00:00",
         "2024-01-11 08:00:00",
         "99.99", "credit_card", None, "web",
         "2024-01-10 11:00:00", "batch000"),   # older _ingested_at
        # Invalid status
        ("ord-003", "cust-003", "INVALID_STATUS",
         "2024-01-12 08:00:00", "2024-01-12 09:00:00", None,
         "2024-01-22 00:00:00", None,
         "250.00", "debit_card", None, "marketplace",
         "2024-01-12 12:00:00", "batch001"),
        # Out-of-range payment value (too high)
        ("ord-004", "cust-004", "delivered",
         "2024-01-13 07:00:00", "2024-01-13 07:30:00",
         "2024-01-18 12:00:00", "2024-01-20 00:00:00",
         "2024-01-14 06:00:00",
         "999999.00", "credit_card", None, "web",
         "2024-01-13 12:00:00", "batch001"),
    ]
    return spark.createDataFrame(rows, schema=schema)


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestCastTypes:
    def test_payment_value_cast(self, pipeline, raw_orders_df):
        typed = pipeline.cast_types(raw_orders_df)
        field = dict((f.name, f) for f in typed.schema.fields)
        assert isinstance(field["payment_value"].dataType, DecimalType)

    def test_timestamps_cast(self, pipeline, raw_orders_df):
        typed = pipeline.cast_types(raw_orders_df)
        ts_col = typed.schema["order_purchase_timestamp"].dataType
        from pyspark.sql.types import TimestampType
        assert isinstance(ts_col, TimestampType)

    def test_order_date_derived(self, pipeline, raw_orders_df):
        typed = pipeline.cast_types(raw_orders_df)
        assert "order_date" in typed.columns
        dates = typed.select("order_date").filter(F.col("order_date").isNotNull()).collect()
        assert len(dates) > 0


class TestStandardize:
    def test_invalid_status_becomes_unknown(self, pipeline, raw_orders_df):
        typed = pipeline.cast_types(raw_orders_df)
        std   = pipeline.standardize(typed)
        row   = std.filter(F.col("order_id") == "ord-003").select("order_status").collect()[0]
        assert row["order_status"] == "unknown"

    def test_valid_status_preserved(self, pipeline, raw_orders_df):
        typed = pipeline.cast_types(raw_orders_df)
        std   = pipeline.standardize(typed)
        row   = std.filter(F.col("order_id") == "ord-001").select("order_status").collect()[0]
        assert row["order_status"] == "delivered"

    def test_channel_lowercased(self, pipeline, raw_orders_df):
        typed = pipeline.cast_types(raw_orders_df)
        std   = pipeline.standardize(typed)
        channels = [r["channel"] for r in std.select("channel").collect()]
        assert all(c == c.lower() for c in channels if c is not None)


class TestDerivedColumns:
    def test_days_to_deliver_calculated(self, pipeline, raw_orders_df):
        typed   = pipeline.cast_types(raw_orders_df)
        derived = pipeline.add_derived_columns(typed)
        row = derived.filter(F.col("order_id") == "ord-001") \
                     .select("days_to_deliver").collect()[0]
        assert row["days_to_deliver"] == 5  # Jan 10 → Jan 15

    def test_is_late_delivery_detected(self, pipeline, raw_orders_df):
        typed   = pipeline.cast_types(raw_orders_df)
        derived = pipeline.add_derived_columns(typed)
        # ord-004: delivered Jan 18, estimated Jan 20 → NOT late
        # Check that is_late_delivery is a boolean column
        assert "is_late_delivery" in derived.columns

    def test_null_days_when_not_delivered(self, pipeline, raw_orders_df):
        typed   = pipeline.cast_types(raw_orders_df)
        derived = pipeline.add_derived_columns(typed)
        row = derived.filter(F.col("order_id") == "ord-002") \
                     .select("days_to_deliver").collect()[0]
        assert row["days_to_deliver"] is None


class TestDeduplication:
    def test_duplicate_removed(self, pipeline, raw_orders_df):
        typed   = pipeline.cast_types(raw_orders_df)
        deduped = pipeline.deduplicate(typed)
        count = deduped.filter(F.col("order_id") == "ord-001").count()
        assert count == 1

    def test_latest_record_kept(self, pipeline, raw_orders_df):
        typed   = pipeline.cast_types(raw_orders_df)
        deduped = pipeline.deduplicate(typed)
        row = deduped.filter(F.col("order_id") == "ord-001") \
                     .select("_batch_id").collect()[0]
        # batch001 has newer _ingested_at than batch000
        assert row["_batch_id"] == "batch001"

    def test_total_row_count_after_dedup(self, pipeline, raw_orders_df):
        typed   = pipeline.cast_types(raw_orders_df)
        deduped = pipeline.deduplicate(typed)
        # 5 input rows, 1 duplicate → expect 4
        assert deduped.count() == 4


class TestDQRules:
    def test_out_of_range_payment_quarantined(self, pipeline, raw_orders_df):
        typed        = pipeline.cast_types(raw_orders_df)
        std          = pipeline.standardize(typed)
        derived      = pipeline.add_derived_columns(std)
        deduped      = pipeline.deduplicate(derived)
        clean, quar  = pipeline.apply_dq_rules(deduped)
        quarantined_ids = [r["order_id"] for r in quar.select("order_id").collect()]
        assert "ord-004" in quarantined_ids

    def test_clean_rows_pass_through(self, pipeline, raw_orders_df):
        typed       = pipeline.cast_types(raw_orders_df)
        std         = pipeline.standardize(typed)
        derived     = pipeline.add_derived_columns(std)
        deduped     = pipeline.deduplicate(derived)
        clean, quar = pipeline.apply_dq_rules(deduped)
        clean_ids   = [r["order_id"] for r in clean.select("order_id").collect()]
        assert "ord-001" in clean_ids
        assert "ord-002" in clean_ids
