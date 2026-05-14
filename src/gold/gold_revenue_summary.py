"""
src/gold/gold_revenue_summary.py
----------------------------------
Gold layer: Daily and monthly revenue summary aggregates.

PURPOSE:
  Serves BI tools, dashboards, and executive reporting.
  These are pre-aggregated, denormalized tables optimized for read speed.

BUSINESS METRICS:
  - Gross Revenue / Net Revenue (after cancellations/returns)
  - Order volume and Average Order Value (AOV)
  - Revenue by channel, payment_type, order_status
  - Day-over-day and month-over-month growth rates
  - Late delivery rate

Gold tables produced:
  gold_daily_revenue      — 1 row per date
  gold_monthly_revenue    — 1 row per year-month
  gold_channel_revenue    — 1 row per channel per date
"""

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.delta_utils import (
    create_delta_table_if_not_exists,
    optimize_table,
    upsert_to_delta,
)
from src.utils.logger import PipelineLogger
from src.utils.spark_utils import generate_batch_id, load_config, resolve_path


GOLD_DAILY_DDL = """
    order_date              DATE        NOT NULL,
    total_orders            BIGINT,
    delivered_orders        BIGINT,
    cancelled_orders        BIGINT,
    returned_orders         BIGINT,
    gross_revenue           DECIMAL(18,2),
    net_revenue             DECIMAL(18,2),
    avg_order_value         DECIMAL(10,2),
    avg_days_to_deliver     DECIMAL(6,2),
    late_delivery_count     BIGINT,
    late_delivery_rate      DECIMAL(5,4),
    unique_customers        BIGINT,
    dod_revenue_growth      DECIMAL(8,4),
    _batch_id               STRING    NOT NULL,
    _updated_at             TIMESTAMP NOT NULL
"""

GOLD_MONTHLY_DDL = """
    year_month              STRING      NOT NULL,
    year                    INT,
    month                   INT,
    total_orders            BIGINT,
    gross_revenue           DECIMAL(18,2),
    net_revenue             DECIMAL(18,2),
    avg_order_value         DECIMAL(10,2),
    unique_customers        BIGINT,
    mom_revenue_growth      DECIMAL(8,4),
    _batch_id               STRING    NOT NULL,
    _updated_at             TIMESTAMP NOT NULL
"""

GOLD_CHANNEL_DDL = """
    order_date              DATE        NOT NULL,
    channel                 STRING      NOT NULL,
    total_orders            BIGINT,
    gross_revenue           DECIMAL(18,2),
    net_revenue             DECIMAL(18,2),
    avg_order_value         DECIMAL(10,2),
    channel_revenue_share   DECIMAL(5,4),
    _batch_id               STRING    NOT NULL,
    _updated_at             TIMESTAMP NOT NULL
"""


class GoldRevenueSummaryPipeline:
    """
    Builds Gold-layer revenue summary tables from Silver orders.

    Demonstrates:
      - Complex PySpark window functions (LAG for growth rates)
      - Broadcast joins (small dimension tables)
      - Partitioned MERGE for efficient upserts
      - OPTIMIZE on Gold tables for BI query performance
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark       = spark
        self.config      = config
        self.batch_id    = generate_batch_id()
        self.silver_path = resolve_path(config["paths"]["silver"] + "/orders", config)
        self.daily_path  = resolve_path(config["paths"]["gold"] + "/revenue_daily", config)
        self.monthly_path = resolve_path(config["paths"]["gold"] + "/revenue_monthly", config)
        self.channel_path = resolve_path(config["paths"]["gold"] + "/revenue_by_channel", config)
        self.log = PipelineLogger("gold", "revenue_summary", batch_id=self.batch_id)

    # ── Bootstrap ────────────────────────────────────────────────────────────

    def bootstrap(self) -> None:
        create_delta_table_if_not_exists(
            self.spark, self.daily_path,   GOLD_DAILY_DDL,
            partition_cols=[], comment="Gold: daily revenue KPIs"
        )
        create_delta_table_if_not_exists(
            self.spark, self.monthly_path, GOLD_MONTHLY_DDL,
            partition_cols=[], comment="Gold: monthly revenue KPIs"
        )
        create_delta_table_if_not_exists(
            self.spark, self.channel_path, GOLD_CHANNEL_DDL,
            partition_cols=[], comment="Gold: revenue by channel and date"
        )

    # ── Read ─────────────────────────────────────────────────────────────────

    def read_silver(self) -> DataFrame:
        self.log.info("Reading silver orders")
        return self.spark.read.format("delta").load(self.silver_path)

    # ── Daily revenue aggregation ─────────────────────────────────────────────

    def build_daily_revenue(self, df: DataFrame) -> DataFrame:
        """
        One row per date with full revenue KPIs.
        Window function adds day-over-day growth rate.
        """
        self.log.info("Building daily revenue aggregation")

        daily = (
            df
            .filter(F.col("order_date").isNotNull())
            .groupBy("order_date")
            .agg(
                F.count("order_id")                            .alias("total_orders"),
                F.sum(F.when(F.col("order_status") == "delivered", 1).otherwise(0))
                                                               .alias("delivered_orders"),
                F.sum(F.when(F.col("order_status") == "cancelled", 1).otherwise(0))
                                                               .alias("cancelled_orders"),
                F.sum(F.when(F.col("order_status") == "returned", 1).otherwise(0))
                                                               .alias("returned_orders"),

                # Gross = all non-null payment values
                F.sum(
                    F.when(F.col("payment_value").isNotNull(), F.col("payment_value"))
                    .otherwise(0)
                ).cast("decimal(18,2)")                        .alias("gross_revenue"),

                # Net = gross minus cancelled + returned orders
                F.sum(
                    F.when(
                        ~F.col("order_status").isin("cancelled", "returned") &
                        F.col("payment_value").isNotNull(),
                        F.col("payment_value")
                    ).otherwise(0)
                ).cast("decimal(18,2)")                        .alias("net_revenue"),

                F.avg("payment_value").cast("decimal(10,2)")  .alias("avg_order_value"),
                F.avg("days_to_deliver").cast("decimal(6,2)") .alias("avg_days_to_deliver"),
                F.sum(F.when(F.col("is_late_delivery"), 1).otherwise(0))
                                                               .alias("late_delivery_count"),
                F.countDistinct("customer_id")                 .alias("unique_customers"),
            )
        )

        # Late delivery rate
        daily = daily.withColumn(
            "late_delivery_rate",
            (F.col("late_delivery_count") / F.col("delivered_orders")).cast("decimal(5,4)")
        )

        # Day-over-day revenue growth using LAG window function
        date_window = Window.orderBy("order_date")
        daily = (
            daily
            .withColumn("_prev_revenue", F.lag("net_revenue", 1).over(date_window))
            .withColumn(
                "dod_revenue_growth",
                F.when(
                    F.col("_prev_revenue").isNotNull() & (F.col("_prev_revenue") != 0),
                    (
                        (F.col("net_revenue") - F.col("_prev_revenue")) /
                        F.col("_prev_revenue")
                    ).cast("decimal(8,4)")
                ).otherwise(F.lit(None).cast("decimal(8,4)"))
            )
            .drop("_prev_revenue")
        )

        return (
            daily
            .withColumn("_batch_id",   F.lit(self.batch_id))
            .withColumn("_updated_at", F.current_timestamp())
        )

    # ── Monthly revenue aggregation ───────────────────────────────────────────

    def build_monthly_revenue(self, df: DataFrame) -> DataFrame:
        """
        Roll daily data up to month level with MoM growth.
        """
        self.log.info("Building monthly revenue aggregation")

        monthly = (
            df
            .filter(F.col("order_date").isNotNull())
            .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
            .withColumn("year",       F.year("order_date"))
            .withColumn("month",      F.month("order_date"))
            .groupBy("year_month", "year", "month")
            .agg(
                F.count("order_id")                           .alias("total_orders"),
                F.sum(
                    F.when(F.col("payment_value").isNotNull(), F.col("payment_value")).otherwise(0)
                ).cast("decimal(18,2)")                       .alias("gross_revenue"),
                F.sum(
                    F.when(
                        ~F.col("order_status").isin("cancelled", "returned") &
                        F.col("payment_value").isNotNull(),
                        F.col("payment_value")
                    ).otherwise(0)
                ).cast("decimal(18,2)")                       .alias("net_revenue"),
                F.avg("payment_value").cast("decimal(10,2)") .alias("avg_order_value"),
                F.countDistinct("customer_id")                .alias("unique_customers"),
            )
        )

        # Month-over-month growth
        ym_window = Window.orderBy("year_month")
        monthly = (
            monthly
            .withColumn("_prev_revenue", F.lag("net_revenue", 1).over(ym_window))
            .withColumn(
                "mom_revenue_growth",
                F.when(
                    F.col("_prev_revenue").isNotNull() & (F.col("_prev_revenue") != 0),
                    (
                        (F.col("net_revenue") - F.col("_prev_revenue")) /
                        F.col("_prev_revenue")
                    ).cast("decimal(8,4)")
                ).otherwise(F.lit(None).cast("decimal(8,4)"))
            )
            .drop("_prev_revenue")
            .withColumn("_batch_id",   F.lit(self.batch_id))
            .withColumn("_updated_at", F.current_timestamp())
        )

        return monthly

    # ── Channel revenue aggregation ───────────────────────────────────────────

    def build_channel_revenue(self, df: DataFrame) -> DataFrame:
        """
        Revenue breakdown by acquisition channel per day.
        Adds channel_revenue_share = channel revenue / total daily revenue.
        Demonstrates broadcast join optimization for small aggregates.
        """
        self.log.info("Building channel revenue aggregation")

        by_channel = (
            df
            .filter(F.col("order_date").isNotNull())
            .groupBy("order_date", "channel")
            .agg(
                F.count("order_id")                           .alias("total_orders"),
                F.sum(
                    F.when(F.col("payment_value").isNotNull(), F.col("payment_value")).otherwise(0)
                ).cast("decimal(18,2)")                       .alias("gross_revenue"),
                F.sum(
                    F.when(
                        ~F.col("order_status").isin("cancelled", "returned") &
                        F.col("payment_value").isNotNull(),
                        F.col("payment_value")
                    ).otherwise(0)
                ).cast("decimal(18,2)")                       .alias("net_revenue"),
                F.avg("payment_value").cast("decimal(10,2)") .alias("avg_order_value"),
            )
        )

        # Daily totals for share calculation — small DF, use broadcast join
        daily_totals = (
            by_channel
            .groupBy("order_date")
            .agg(F.sum("gross_revenue").alias("daily_total"))
        )

        result = (
            by_channel
            .join(F.broadcast(daily_totals), on="order_date", how="left")
            .withColumn(
                "channel_revenue_share",
                F.when(
                    F.col("daily_total") != 0,
                    (F.col("gross_revenue") / F.col("daily_total")).cast("decimal(5,4)")
                ).otherwise(F.lit(0).cast("decimal(5,4)"))
            )
            .drop("daily_total")
            .withColumn("_batch_id",   F.lit(self.batch_id))
            .withColumn("_updated_at", F.current_timestamp())
        )

        return result

    # ── Write ─────────────────────────────────────────────────────────────────

    def write_gold(self, df: DataFrame, path: str, merge_keys: list) -> None:
        upsert_to_delta(
            spark       = self.spark,
            source_df   = df,
            target_path = path,
            merge_keys  = merge_keys,
        )
        optimize_table(self.spark, path)
        self.log.info("Gold table written and optimized", path=path)

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        self.log.info("Gold revenue pipeline starting")
        self.bootstrap()

        silver_df = self.read_silver().cache()   # cache — read 3x for 3 aggregations

        daily_df   = self.build_daily_revenue(silver_df)
        monthly_df = self.build_monthly_revenue(silver_df)
        channel_df = self.build_channel_revenue(silver_df)

        self.write_gold(daily_df,   self.daily_path,   merge_keys=["order_date"])
        self.write_gold(monthly_df, self.monthly_path, merge_keys=["year_month"])
        self.write_gold(channel_df, self.channel_path, merge_keys=["order_date", "channel"])

        silver_df.unpersist()

        summary = {
            "layer":    "gold",
            "table":    "revenue_summary",
            "batch_id": self.batch_id,
            "daily_rows":   daily_df.count(),
            "monthly_rows": monthly_df.count(),
            "channel_rows": channel_df.count(),
        }
        self.log.info("Gold revenue pipeline complete", **summary)
        return summary


if __name__ == "__main__":
    from src.utils.spark_utils import get_spark, load_config
    env    = os.getenv("ENV_NAME", "dev")
    config = load_config(env)
    spark  = get_spark(config)
    GoldRevenueSummaryPipeline(spark, config).run()
