"""
src/quality/expectations.py
-----------------------------
Lightweight data quality expectation framework.
Inspired by Great Expectations but with zero extra dependencies.

Each expectation is a pure function:
    (DataFrame, column, **kwargs) → ExpectationResult

Results are collected into a DQReport and can:
  - Raise an exception (fail-fast mode)
  - Write a JSON report to storage
  - Log warnings (soft mode)
  - Be surfaced in Databricks Jobs run metadata
"""

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class ExpectationResult:
    expectation:   str
    column:        str
    passed:        bool
    observed_value: Any
    expected_value: Any
    row_count:     int
    fail_count:    int
    fail_pct:      float
    details:       str = ""


@dataclass
class DQReport:
    table:         str
    layer:         str
    batch_id:      str
    run_at:        str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    results:       List[ExpectationResult] = field(default_factory=list)
    passed:        bool = True

    def add(self, result: ExpectationResult) -> None:
        self.results.append(result)
        if not result.passed:
            self.passed = False

    def summary(self) -> dict:
        total  = len(self.results)
        failed = sum(1 for r in self.results if not r.passed)
        return {
            "table":         self.table,
            "layer":         self.layer,
            "batch_id":      self.batch_id,
            "run_at":        self.run_at,
            "total_checks":  total,
            "passed_checks": total - failed,
            "failed_checks": failed,
            "overall_pass":  self.passed,
        }

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, indent=2, default=str)

    def print_summary(self) -> None:
        s = self.summary()
        status = "✅ PASSED" if s["overall_pass"] else "❌ FAILED"
        print(f"\n{'='*60}")
        print(f"  DQ Report — {s['table']} ({s['layer']})")
        print(f"  {status}  |  {s['passed_checks']}/{s['total_checks']} checks passed")
        print(f"{'='*60}")
        for r in self.results:
            icon = "✓" if r.passed else "✗"
            print(f"  [{icon}] {r.expectation}({r.column}): "
                  f"observed={r.observed_value}, expected={r.expected_value}")
        print()


# ── Individual expectations ───────────────────────────────────────────────────

def expect_no_nulls(
    df: DataFrame,
    column: str,
    threshold_pct: float = 0.0,
) -> ExpectationResult:
    """
    Expect null rate on `column` to be ≤ threshold_pct.
    threshold_pct=0.0 → zero nulls allowed.
    threshold_pct=0.05 → up to 5% nulls allowed.
    """
    total      = df.count()
    null_count = df.filter(F.col(column).isNull()).count()
    null_pct   = null_count / total if total > 0 else 0.0
    passed     = null_pct <= threshold_pct

    return ExpectationResult(
        expectation    = "expect_no_nulls",
        column         = column,
        passed         = passed,
        observed_value = round(null_pct, 6),
        expected_value = f"<= {threshold_pct}",
        row_count      = total,
        fail_count     = null_count,
        fail_pct       = round(null_pct, 6),
        details        = f"{null_count:,} null values out of {total:,} rows",
    )


def expect_no_duplicates(
    df: DataFrame,
    column: str,
) -> ExpectationResult:
    """Expect zero duplicate values in `column`."""
    total      = df.count()
    distinct   = df.select(column).distinct().count()
    dup_count  = total - distinct
    passed     = dup_count == 0

    return ExpectationResult(
        expectation    = "expect_no_duplicates",
        column         = column,
        passed         = passed,
        observed_value = dup_count,
        expected_value = 0,
        row_count      = total,
        fail_count     = dup_count,
        fail_pct       = round(dup_count / total, 6) if total > 0 else 0.0,
        details        = f"{dup_count:,} duplicate values detected",
    )


def expect_values_in_set(
    df: DataFrame,
    column: str,
    valid_values: List[str],
    threshold_pct: float = 0.0,
) -> ExpectationResult:
    """Expect all non-null values in `column` to be in `valid_values`."""
    total        = df.count()
    invalid_count = df.filter(
        F.col(column).isNotNull() & ~F.col(column).isin(valid_values)
    ).count()
    invalid_pct = invalid_count / total if total > 0 else 0.0
    passed      = invalid_pct <= threshold_pct

    return ExpectationResult(
        expectation    = "expect_values_in_set",
        column         = column,
        passed         = passed,
        observed_value = round(invalid_pct, 6),
        expected_value = f"<= {threshold_pct}",
        row_count      = total,
        fail_count     = invalid_count,
        fail_pct       = round(invalid_pct, 6),
        details        = f"{invalid_count:,} values not in allowed set",
    )


def expect_column_between(
    df: DataFrame,
    column: str,
    min_val: float,
    max_val: float,
    threshold_pct: float = 0.0,
) -> ExpectationResult:
    """Expect non-null values in `column` to fall within [min_val, max_val]."""
    total         = df.count()
    out_of_range  = df.filter(
        F.col(column).isNotNull() &
        ((F.col(column) < min_val) | (F.col(column) > max_val))
    ).count()
    oor_pct = out_of_range / total if total > 0 else 0.0
    passed  = oor_pct <= threshold_pct

    return ExpectationResult(
        expectation    = "expect_column_between",
        column         = column,
        passed         = passed,
        observed_value = round(oor_pct, 6),
        expected_value = f"[{min_val}, {max_val}] with <= {threshold_pct} violations",
        row_count      = total,
        fail_count     = out_of_range,
        fail_pct       = round(oor_pct, 6),
        details        = f"{out_of_range:,} values outside [{min_val}, {max_val}]",
    )


def expect_row_count_between(
    df: DataFrame,
    column: str = "*",
    min_rows: int = 1,
    max_rows: int = 10_000_000_000,
) -> ExpectationResult:
    """Expect total row count to be within [min_rows, max_rows]."""
    total  = df.count()
    passed = min_rows <= total <= max_rows

    return ExpectationResult(
        expectation    = "expect_row_count_between",
        column         = column,
        passed         = passed,
        observed_value = total,
        expected_value = f"[{min_rows}, {max_rows}]",
        row_count      = total,
        fail_count     = 0 if passed else 1,
        fail_pct       = 0.0 if passed else 1.0,
        details        = f"Row count {total:,} {'within' if passed else 'outside'} expected range",
    )


def expect_referential_integrity(
    df: DataFrame,
    column: str,
    reference_df: DataFrame,
    reference_column: str,
    threshold_pct: float = 0.0,
) -> ExpectationResult:
    """
    Expect values in `column` to exist in `reference_df[reference_column]`.
    Used to validate FK relationships (e.g., order.customer_id exists in customers).
    Uses broadcast join if reference_df is small.
    """
    total    = df.filter(F.col(column).isNotNull()).count()
    ref_keys = reference_df.select(reference_column).distinct()
    orphans  = (
        df.filter(F.col(column).isNotNull())
        .join(F.broadcast(ref_keys),
              df[column] == ref_keys[reference_column],
              how="left_anti")
        .count()
    )
    orphan_pct = orphans / total if total > 0 else 0.0
    passed     = orphan_pct <= threshold_pct

    return ExpectationResult(
        expectation    = "expect_referential_integrity",
        column         = f"{column} → {reference_column}",
        passed         = passed,
        observed_value = round(orphan_pct, 6),
        expected_value = f"<= {threshold_pct}",
        row_count      = total,
        fail_count     = orphans,
        fail_pct       = round(orphan_pct, 6),
        details        = f"{orphans:,} orphaned values (no matching {reference_column})",
    )


# ── Pre-built suites ──────────────────────────────────────────────────────────

def run_orders_suite(
    df: DataFrame,
    config: dict,
    batch_id: str,
    customers_df: Optional[DataFrame] = None,
) -> DQReport:
    """
    Run the full DQ suite for the Silver orders table.
    """
    dq_cfg  = config.get("data_quality", {}).get("orders", {})
    report  = DQReport(table="orders", layer="silver", batch_id=batch_id)

    report.add(expect_row_count_between(df, min_rows=1))
    report.add(expect_no_nulls(df, "order_id",    threshold_pct=0.0))
    report.add(expect_no_nulls(df, "customer_id", threshold_pct=0.0))
    report.add(expect_no_nulls(df, "order_date",  threshold_pct=0.0))
    report.add(expect_no_duplicates(df, "order_id"))
    report.add(expect_values_in_set(
        df, "order_status",
        valid_values=["delivered", "shipped", "processing",
                      "cancelled", "returned", "unknown"],
        threshold_pct=0.0,
    ))
    report.add(expect_column_between(
        df, "payment_value",
        min_val       = dq_cfg.get("min_order_amount", 0.01),
        max_val       = dq_cfg.get("max_order_amount", 50000.0),
        threshold_pct = dq_cfg.get("null_tolerance_pct", 0.01),
    ))

    if customers_df is not None:
        report.add(expect_referential_integrity(
            df, "customer_id",
            reference_df     = customers_df,
            reference_column = "customer_id",
            threshold_pct    = 0.001,
        ))

    report.print_summary()
    return report


def run_events_suite(
    df: DataFrame,
    config: dict,
    batch_id: str,
) -> DQReport:
    """DQ suite for Silver events table."""
    dq_cfg = config.get("data_quality", {}).get("events", {})
    report = DQReport(table="events", layer="silver", batch_id=batch_id)

    report.add(expect_row_count_between(df, min_rows=1))
    report.add(expect_no_nulls(df, "event_id", threshold_pct=0.0))
    report.add(expect_no_duplicates(df, "event_id"))
    report.add(expect_values_in_set(
        df, "event_type",
        valid_values=["page_view", "product_view", "add_to_cart",
                      "remove_from_cart", "checkout_start",
                      "checkout_complete", "search", "wishlist_add"],
        threshold_pct=dq_cfg.get("null_tolerance_pct", 0.05),
    ))
    report.add(expect_values_in_set(
        df, "device_type",
        valid_values=["desktop", "mobile", "tablet"],
        threshold_pct=0.01,
    ))

    report.print_summary()
    return report
