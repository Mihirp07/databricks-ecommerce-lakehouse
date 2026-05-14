# Data Dictionary

**Project:** Databricks E-Commerce Lakehouse  
**Last updated:** 2026  
**Layers documented:** Bronze, Silver, Gold  
**Tables documented:** 6 implemented Delta tables across 3 layers

---

## Table of Contents

1. [Lakehouse Architecture Overview](#lakehouse-architecture-overview)
2. [Bronze — `bronze_orders`](#bronze--bronze_orders)
3. [Bronze — `bronze_events`](#bronze--bronze_events)
4. [Silver — `silver_orders`](#silver--silver_orders)
5. [Gold — `gold_revenue_daily`](#gold--gold_revenue_daily)
6. [Gold — `gold_revenue_monthly`](#gold--gold_revenue_monthly)
7. [Gold — `gold_revenue_by_channel`](#gold--gold_revenue_by_channel)
8. [Pipeline Metadata Columns](#pipeline-metadata-columns)
9. [Data Quality Thresholds Reference](#data-quality-thresholds-reference)

---

## Lakehouse Architecture Overview

This project implements the **medallion architecture** — a three-layer data organisation pattern used in production Databricks Lakehouses. Each layer has a distinct contract and responsibility. Data always flows in one direction: Bronze → Silver → Gold. No layer reads from a layer above it.

```
Raw Files (CSV / JSON)
        │
        ▼
  ┌─────────────────────────────────────────────────────────┐
  │  BRONZE  —  s3://ecommerce-lakehouse-prod/bronze/       │
  │  Append-only. Raw values preserved as strings.          │
  │  Schema enforced. Corrupt rows are logged for review.    │
  └──────────────────────────┬──────────────────────────────┘
                             │  MERGE INTO (upsert by primary key)
                             ▼
  ┌─────────────────────────────────────────────────────────┐
  │  SILVER  —  s3://ecommerce-lakehouse-prod/silver/       │
  │  Typed, deduplicated, validated, standardised.          │
  │  Failed rows quarantined. Change Data Feed enabled.      │
  └──────────────────────────┬──────────────────────────────┘
                             │  Aggregation (groupBy + window functions)
                             ▼
  ┌─────────────────────────────────────────────────────────┐
  │  GOLD    —  s3://ecommerce-lakehouse-prod/gold/         │
  │  Pre-aggregated business metrics. BI-ready.             │
  │  Optimised for read performance (OPTIMIZE after write).  │
  └─────────────────────────────────────────────────────────┘
```

### Why three layers?

| Concern | Bronze | Silver | Gold |
|---|---|---|---|
| **Replayability** | Full raw history retained forever | Rebuilt from Bronze at any time | Rebuilt from Silver at any time |
| **Schema changes** | Absorbed here first | Propagated deliberately | Downstream consumers protected |
| **Business logic** | None | Minimal (cleaning, typing) | Full (KPIs, growth rates) |
| **Who reads it** | Pipeline engineers, debugging | Data engineers, data scientists | BI tools, dashboards, APIs |
| **Write pattern** | Append-only | MERGE INTO (upsert) | MERGE INTO (upsert) |
| **Delta features** | Schema enforcement, partitioning | Z-ORDER, Change Data Feed | OPTIMIZE, broadcast join |

**Dev vs production paths:** All pipeline code uses canonical cloud paths (`s3://ecommerce-lakehouse-prod/...`). In the dev environment, `resolve_path()` in `src/utils/spark_utils.py` transparently remaps these to `dbfs:/FileStore/lakehouse/...` via the `simulate_cloud_paths: true` flag in `configs/dev.yml`. No application code changes between environments.

---

## Bronze — `bronze_orders`

**Source file:** `src/bronze/bronze_orders.py`  
**Storage path:** `s3://ecommerce-lakehouse-prod/bronze/orders/`  
**Dev path:** `dbfs:/FileStore/lakehouse/bronze/orders/`  
**Format:** Delta Lake  
**Write mode:** Append-only — raw data is never modified  
**Partition column:** `_ingest_date`  
**Schema evolution:** Disabled (`mergeSchema: false`) — new upstream columns are rejected  

### Purpose

Lands raw order data from the CSV source file into Delta Lake with zero business transformation. Every row from the source — including malformed rows — is captured here. This table is the single source of truth for raw order data and must not be modified after write.

### Grain

One row per source record per ingestion run. If the same `order_id` is re-ingested in a later batch, both rows are present in Bronze (deduplication happens in Silver).

### Primary Key

`order_id` is the natural business key but is **not enforced as unique at this layer** by design. Bronze preserves all raw records including duplicates.

### Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| `order_id` | STRING | NOT NULL | Unique identifier for the order. Read as raw string from source CSV. |
| `customer_id` | STRING | NOT NULL | Identifier of the customer who placed the order. Raw string from source. |
| `order_status` | STRING | Yes | Order lifecycle status as received from source. Not validated or standardised at this layer. |
| `order_purchase_timestamp` | STRING | Yes | Timestamp when the customer placed the order. Stored as raw string — type casting occurs in Silver. |
| `order_approved_at` | STRING | Yes | Timestamp when payment was approved. Null for cancelled orders. Raw string. |
| `order_delivered_carrier_date` | STRING | Yes | Timestamp when the order was handed to the delivery carrier. Null for unshipped orders. Raw string. |
| `order_delivered_customer_date` | STRING | Yes | Timestamp when the customer received the order. Null for orders not yet delivered. Raw string. |
| `order_estimated_delivery_date` | STRING | Yes | Estimated delivery date communicated to the customer at time of purchase. Raw string. |
| `payment_value` | STRING | Yes | Total payment amount for the order. Stored as raw string. Cast to `DECIMAL(12,2)` in Silver. |
| `payment_type` | STRING | Yes | Payment method used (e.g. `credit_card`, `paypal`). Not validated at this layer. |
| `promo_code` | STRING | Yes | Promotional code applied to the order, if any. Null when no promo was used. |
| `channel` | STRING | Yes | Acquisition channel through which the order was placed (e.g. `web`, `mobile_app`). |
| `_ingested_at` | TIMESTAMP | NOT NULL | UTC timestamp when this row was written to Bronze by the pipeline. Set by `current_timestamp()`. |
| `_source_file` | STRING | Yes | Full path of the source file from which this row was read. Set by `input_file_name()`. Used for lineage. |
| `_batch_id` | STRING | NOT NULL | 8-character hex identifier for the pipeline run that produced this row. Generated by `generate_batch_id()`. |
| `_ingest_date` | DATE | NOT NULL | Date portion of `_ingested_at`. Used as the Delta partition column for efficient incremental reads. |

### Corrupt row handling

The CSV reader runs in `PERMISSIVE` mode. Rows that cannot be parsed against the schema are captured in a `_corrupt_record` column. The pipeline logs the count of corrupt rows as a warning, drops them from the write, and they do not appear in the Bronze table. Corrupt rows are not quarantined at this layer — they are lost with a log warning. The quarantine pattern is applied in Silver.

### Delta table properties

```
delta.enableChangeDataFeed = true
delta.autoOptimize.optimizeWrite = true
delta.autoOptimize.autoCompact = true
```

---

## Bronze — `bronze_events`

**Source file:** `src/bronze/bronze_events_stream.py`  
**Storage path:** `s3://ecommerce-lakehouse-prod/bronze/events/`  
**Dev path:** `dbfs:/FileStore/lakehouse/bronze/events/`  
**Format:** Delta Lake  
**Write mode:** Append-only via Spark Structured Streaming  
**Partition column:** `event_date`  
**Ingestion method:** Databricks Auto Loader (`cloudFiles` format) with fallback to `readStream.json`  
**Checkpoint path:** `s3://ecommerce-lakehouse-prod/checkpoints/bronze_events/`  

### Purpose

Captures raw clickstream events from the e-commerce platform in near real-time. In production, events flow from Kafka or an Event Hub into cloud storage, and Auto Loader streams them into this Delta table. In dev, `generate_sample_data.py` drops JSON files into the local raw path and Auto Loader processes them.

### Grain

One row per clickstream event. `event_id` is the natural business key. Intra-batch deduplication is applied via `foreachBatch` before writing — within a single micro-batch, duplicate `event_id` values are dropped using `dropDuplicates(["event_id"])`. Cross-batch duplicates are handled in Silver.

### Primary Key

`event_id` — deduplicated within each micro-batch at the Bronze write step.

### Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| `event_id` | STRING | NOT NULL | UUID uniquely identifying this clickstream event. |
| `session_id` | STRING | Yes | Browser or app session identifier. Groups events within a single user visit. |
| `customer_id` | STRING | Yes | Identifier of the logged-in customer. Null for anonymous (guest) sessions. |
| `event_type` | STRING | Yes | Type of interaction recorded. Raw string from source — not validated at this layer. |
| `event_timestamp` | STRING | Yes | ISO 8601 timestamp when the event occurred in the browser or app. Stored as raw string. |
| `product_id` | STRING | Yes | Product identifier associated with the event. Present for `product_view`, `add_to_cart`, and `wishlist_add` event types. Null for all other types. |
| `page_url` | STRING | Yes | Relative URL of the page where the event was recorded. |
| `referrer` | STRING | Yes | Traffic source that brought the user to the page (e.g. `google.com`, `direct`, `email`). |
| `device_type` | STRING | Yes | Device category of the user's browser or app client. Raw string — validated in Silver. |
| `user_agent` | STRING | Yes | Full HTTP user-agent string from the client browser or app. Used for device and browser analysis. |
| `ip_address` | STRING | Yes | Client IP address at time of event. Used for geolocation enrichment. |
| `country_code` | STRING | Yes | Two-character ISO 3166-1 country code. Populated from IP geolocation or client locale. |
| `_ingested_at` | TIMESTAMP | NOT NULL | UTC timestamp when this row was written to Bronze by the streaming pipeline. |
| `_source_file` | STRING | Yes | Path of the JSON file processed by Auto Loader for this row. Set by `input_file_name()`. |
| `_batch_id` | STRING | NOT NULL | Pipeline run identifier for this streaming job instance. |
| `event_date` | DATE | Yes | Date derived from `event_timestamp` via `to_date(to_timestamp(...))`. Used as the Delta partition column. |

### Streaming configuration

| Setting | Value | Notes |
|---|---|---|
| Trigger mode (continuous) | `processingTime = "30 seconds"` | Configured in `dev.yml` under `streaming.trigger_interval` |
| Trigger mode (scheduled job) | `availableNow = True` | Processes all pending files then stops — used in Databricks Workflows |
| Max files per trigger (fallback) | 100 | Applied when Auto Loader is unavailable |
| Schema location | `s3://…/checkpoints/bronze_events_schema/` | Persisted schema used by Auto Loader across restarts |
| Rescued data column | `_rescued_data` | Captures unexpected fields not in the defined schema |

---

## Silver — `silver_orders`

**Source file:** `src/silver/silver_orders.py`  
**Source table:** `bronze_orders`  
**Storage path:** `s3://ecommerce-lakehouse-prod/silver/orders/`  
**Dev path:** `dbfs:/FileStore/lakehouse/silver/orders/`  
**Format:** Delta Lake  
**Write mode:** MERGE INTO (upsert) — merge key: `order_id`  
**Partition column:** `order_date`  
**Z-ORDER columns:** `customer_id`, `order_date`  
**Change Data Feed:** Enabled — consumed by the Gold layer for incremental propagation  

### Purpose

Produces a clean, typed, deduplicated, and validated view of orders suitable for analytics and Gold-layer aggregation. All business logic decisions made here are deliberate and documented. Rows that fail data quality rules are not silently dropped — they are written to a quarantine table for investigation.

### Grain

One row per `order_id`. Uniqueness is enforced via the MERGE INTO upsert pattern. If the same `order_id` appears in Bronze multiple times (due to re-ingestion or source system updates), Silver retains only the latest version, determined by `_ingested_at` using a window function.

### Primary Key

`order_id` — unique, enforced by MERGE INTO merge condition.

### Deduplication logic

```
Window: PARTITION BY order_id ORDER BY _ingested_at DESC
Keep: row_number() == 1
```

The most recently ingested record for each `order_id` is kept. Earlier duplicates are discarded before the MERGE.

### Schema

| Column | Type | Nullable | Source / Transformation | Description |
|---|---|---|---|---|
| `order_id` | STRING | NOT NULL | Cast from Bronze `order_id` | Unique order identifier. Primary key. |
| `customer_id` | STRING | NOT NULL | Cast from Bronze `customer_id` | Identifier of the customer. Foreign key to customers table (not yet implemented). |
| `order_status` | STRING | NOT NULL | Standardised from Bronze `order_status` | Normalised order status. Valid values: `delivered`, `shipped`, `processing`, `cancelled`, `returned`, `invoiced`, `unavailable`. Values not in this set are mapped to `unknown`. |
| `order_purchase_timestamp` | TIMESTAMP | Yes | `to_timestamp(order_purchase_timestamp)` | Typed purchase timestamp. Malformed strings become NULL via safe cast. |
| `order_approved_at` | TIMESTAMP | Yes | `to_timestamp(order_approved_at)` | Typed approval timestamp. Null for cancelled or unapproved orders. |
| `order_delivered_carrier_date` | TIMESTAMP | Yes | `to_timestamp(order_delivered_carrier_date)` | Typed timestamp when order reached the carrier. |
| `order_delivered_customer_date` | TIMESTAMP | Yes | `to_timestamp(order_delivered_customer_date)` | Typed timestamp of customer delivery. Null for undelivered orders. |
| `order_estimated_delivery_date` | TIMESTAMP | Yes | `to_timestamp(order_estimated_delivery_date)` | Typed estimated delivery timestamp shown to the customer. |
| `payment_value` | DECIMAL(12,2) | Yes | `.cast(DecimalType(12, 2))` from Bronze `payment_value` | Order payment amount. Two decimal places. Null where payment was not captured. |
| `payment_type` | STRING | Yes | Standardised from Bronze `payment_type` | Normalised payment method. Valid values: `credit_card`, `debit_card`, `paypal`, `apple_pay`, `google_pay`, `boleto`, `voucher`. Others mapped to `other`. |
| `promo_code` | STRING | Yes | Passed through from Bronze | Promotional code applied to the order. Null if none. |
| `channel` | STRING | Yes | `lower(coalesce(channel, 'unknown'))` | Lowercased acquisition channel. Null values replaced with `unknown`. |
| `order_date` | DATE | Yes | `to_date(order_purchase_timestamp)` | Date portion of the purchase timestamp. Used as partition column. Null if `order_purchase_timestamp` was malformed. |
| `days_to_deliver` | INT | Yes | `datediff(order_delivered_customer_date, order_purchase_timestamp)` | Calendar days from order placement to customer delivery. Null if order has not been delivered. |
| `days_to_approve` | INT | Yes | `datediff(order_approved_at, order_purchase_timestamp)` | Calendar days from order placement to payment approval. Null if order was not approved. |
| `is_late_delivery` | BOOLEAN | Yes | `order_delivered_customer_date > order_estimated_delivery_date` | `true` if the order was delivered after the estimated delivery date. `false` if on time or early. Null if either timestamp is missing. Defaults to `false` when both timestamps are null. |
| `delivery_delay_days` | INT | Yes | `datediff(order_delivered_customer_date, order_estimated_delivery_date)` | Signed number of days between actual and estimated delivery. Positive = late. Negative = early. Null if either timestamp is missing. |
| `_ingested_at` | TIMESTAMP | NOT NULL | Carried forward from Bronze | Original ingestion timestamp from Bronze. Retained for lineage. |
| `_batch_id` | STRING | NOT NULL | Generated at Silver run time | Identifies the Silver pipeline run that produced or last updated this row. |
| `_silver_processed_at` | TIMESTAMP | NOT NULL | `current_timestamp()` at Silver write time | Timestamp when this row was written to Silver. Distinct from `_ingested_at`. |

### Data quality rules

The following rules are applied after deduplication. Rows that fail any rule are written to the quarantine table (`s3://…/bronze/quarantine/orders/`) and excluded from Silver.

| Rule | Condition | Failure action |
|---|---|---|
| R1 — Order ID not null | `order_id IS NOT NULL` | Quarantine row |
| R2 — Customer ID not null | `customer_id IS NOT NULL` | Quarantine row |
| R3 — Order date not null | `order_date IS NOT NULL` | Quarantine row — implies `order_purchase_timestamp` was unparseable |
| R4 — Payment value in valid range | `payment_value IS NULL OR (payment_value >= 0.01 AND payment_value <= 50000.0)` | Quarantine row if non-null and out of range |

**Threshold monitoring:** If the overall quarantine rate for a run exceeds 1% of total rows (`null_tolerance_pct: 0.01` in `dev.yml`), a WARNING is logged to the pipeline logger. The pipeline does not fail — rows below the threshold still proceed.

**Quarantine table columns** (appended at quarantine write time):

| Column | Type | Description |
|---|---|---|
| `_quarantine_reason` | STRING | Always `dq_failed` for DQ-triggered quarantines |
| `_quarantine_at` | TIMESTAMP | Timestamp when the row was written to quarantine |

### Post-write optimisation

After every MERGE run, the pipeline executes:

```sql
OPTIMIZE delta.`s3://…/silver/orders/`
ZORDER BY (customer_id, order_date)
```

Z-ORDER co-locates data for the same `customer_id` and `order_date` in the same Delta files. This dramatically reduces data scanned for the most common access patterns: customer-level queries in the Gold Customer 360 (not yet implemented) and date-range revenue aggregations in the Gold revenue tables.

---

## Gold — `gold_revenue_daily`

**Source file:** `src/gold/gold_revenue_summary.py`  
**Source table:** `silver_orders`  
**Storage path:** `s3://ecommerce-lakehouse-prod/gold/revenue_daily/`  
**Dev path:** `dbfs:/FileStore/lakehouse/gold/revenue_daily/`  
**Format:** Delta Lake  
**Write mode:** MERGE INTO — merge key: `order_date`  
**Partition column:** None (small table — full table scans are fast)  
**Post-write:** `OPTIMIZE` runs after every write  

### Purpose

Provides one row per calendar date containing all daily revenue KPIs used by executive dashboards and automated reporting. Computed from `silver_orders` using a `groupBy("order_date")` aggregation followed by a `LAG` window function for day-over-day growth.

### Grain

One row per `order_date`. Unique. The MERGE INTO pattern ensures re-running the pipeline for the same date overwrites the existing row rather than creating duplicates.

### Primary Key

`order_date`

### Schema

| Column | Type | Nullable | Description | Calculation |
|---|---|---|---|---|
| `order_date` | DATE | NOT NULL | Calendar date of the orders. Primary key. | Partition key from `silver_orders.order_date` |
| `total_orders` | BIGINT | Yes | Total number of orders placed on this date, regardless of status. | `COUNT(order_id)` |
| `delivered_orders` | BIGINT | Yes | Number of orders with status `delivered` on this date. | `SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END)` |
| `cancelled_orders` | BIGINT | Yes | Number of orders with status `cancelled` on this date. | `SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END)` |
| `returned_orders` | BIGINT | Yes | Number of orders with status `returned` on this date. | `SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END)` |
| `gross_revenue` | DECIMAL(18,2) | Yes | Sum of all non-null `payment_value` amounts on this date. Includes cancelled and returned orders. | `SUM(payment_value)` where `payment_value IS NOT NULL` |
| `net_revenue` | DECIMAL(18,2) | Yes | Revenue excluding cancelled and returned orders. The primary revenue metric for reporting. | `SUM(payment_value)` where `order_status NOT IN ('cancelled', 'returned') AND payment_value IS NOT NULL` |
| `avg_order_value` | DECIMAL(10,2) | Yes | Mean `payment_value` across all orders on this date (including nulls in denominator via Spark `AVG`). | `AVG(payment_value)` |
| `avg_days_to_deliver` | DECIMAL(6,2) | Yes | Mean days from purchase to customer delivery for orders delivered on this date. Null if no deliveries occurred. | `AVG(days_to_deliver)` from Silver |
| `late_delivery_count` | BIGINT | Yes | Number of orders delivered on this date where `is_late_delivery = true`. | `SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END)` |
| `late_delivery_rate` | DECIMAL(5,4) | Yes | Proportion of delivered orders that were late. Null if `delivered_orders = 0`. | `late_delivery_count / delivered_orders` |
| `unique_customers` | BIGINT | Yes | Count of distinct `customer_id` values on this date. Measures reach. | `COUNT(DISTINCT customer_id)` |
| `dod_revenue_growth` | DECIMAL(8,4) | Yes | Day-over-day change in `net_revenue` as a decimal fraction. `0.05` = 5% growth. Null for the first date in the dataset or when previous day revenue is zero. | `(net_revenue - LAG(net_revenue, 1)) / LAG(net_revenue, 1)` over `ORDER BY order_date` |
| `_batch_id` | STRING | NOT NULL | Pipeline run identifier for the Gold run that produced this row. | Set at Gold write time |
| `_updated_at` | TIMESTAMP | NOT NULL | Timestamp when this row was last written or updated. | `current_timestamp()` at Gold write time |

---

## Gold — `gold_revenue_monthly`

**Source file:** `src/gold/gold_revenue_summary.py`  
**Source table:** `silver_orders`  
**Storage path:** `s3://ecommerce-lakehouse-prod/gold/revenue_monthly/`  
**Dev path:** `dbfs:/FileStore/lakehouse/gold/revenue_monthly/`  
**Format:** Delta Lake  
**Write mode:** MERGE INTO — merge key: `year_month`  
**Partition column:** None  

### Purpose

Monthly rollup of revenue KPIs for trend analysis, period comparisons, and financial reporting. Computed in the same pipeline run as `gold_revenue_daily`, reading from the cached `silver_orders` DataFrame.

### Grain

One row per calendar month in `yyyy-MM` format. Unique.

### Primary Key

`year_month`

### Schema

| Column | Type | Nullable | Description | Calculation |
|---|---|---|---|---|
| `year_month` | STRING | NOT NULL | Calendar month in `yyyy-MM` format (e.g. `2024-01`). Primary key. | `date_format(order_date, 'yyyy-MM')` |
| `year` | INT | Yes | Calendar year extracted from `order_date`. | `year(order_date)` |
| `month` | INT | Yes | Calendar month number (1–12) extracted from `order_date`. | `month(order_date)` |
| `total_orders` | BIGINT | Yes | Total number of orders placed in this month. | `COUNT(order_id)` |
| `gross_revenue` | DECIMAL(18,2) | Yes | Sum of all non-null `payment_value` amounts in this month. Includes cancelled and returned. | `SUM(payment_value)` where not null |
| `net_revenue` | DECIMAL(18,2) | Yes | Revenue excluding cancelled and returned orders. Primary monthly revenue figure. | `SUM(payment_value)` excluding cancelled/returned statuses |
| `avg_order_value` | DECIMAL(10,2) | Yes | Mean `payment_value` for the month. | `AVG(payment_value)` |
| `unique_customers` | BIGINT | Yes | Count of distinct customers who placed orders in this month. | `COUNT(DISTINCT customer_id)` |
| `mom_revenue_growth` | DECIMAL(8,4) | Yes | Month-over-month change in `net_revenue` as a decimal fraction. Null for the first month or when prior month revenue is zero. | `(net_revenue - LAG(net_revenue, 1)) / LAG(net_revenue, 1)` over `ORDER BY year_month` |
| `_batch_id` | STRING | NOT NULL | Pipeline run identifier. | Set at Gold write time |
| `_updated_at` | TIMESTAMP | NOT NULL | Timestamp when this row was last written or updated. | `current_timestamp()` at Gold write time |

---

## Gold — `gold_revenue_by_channel`

**Source file:** `src/gold/gold_revenue_summary.py`  
**Source table:** `silver_orders`  
**Storage path:** `s3://ecommerce-lakehouse-prod/gold/revenue_by_channel/`  
**Dev path:** `dbfs:/FileStore/lakehouse/gold/revenue_by_channel/`  
**Format:** Delta Lake  
**Write mode:** MERGE INTO — merge keys: `order_date`, `channel`  
**Partition column:** None  

### Purpose

Revenue breakdown by acquisition channel per day. Used by marketing teams to measure channel effectiveness, attribute revenue, and calculate channel-level ROI. The `channel_revenue_share` column is computed via a broadcast join against the daily total — a deliberate performance optimisation documented in the source code.

### Grain

One row per `(order_date, channel)` combination. Composite primary key.

### Primary Key

Composite: `(order_date, channel)`

### Schema

| Column | Type | Nullable | Description | Calculation |
|---|---|---|---|---|
| `order_date` | DATE | NOT NULL | Calendar date. Part of composite primary key. | Grouped from `silver_orders.order_date` |
| `channel` | STRING | NOT NULL | Acquisition channel. Part of composite primary key. Values sourced from Silver `channel` column (lowercased, nulls replaced with `unknown`). | Grouped from `silver_orders.channel` |
| `total_orders` | BIGINT | Yes | Total orders placed via this channel on this date. | `COUNT(order_id)` |
| `gross_revenue` | DECIMAL(18,2) | Yes | Sum of all non-null payment values for this channel and date. | `SUM(payment_value)` where not null |
| `net_revenue` | DECIMAL(18,2) | Yes | Revenue from this channel excluding cancelled and returned orders. | `SUM(payment_value)` excluding cancelled/returned |
| `avg_order_value` | DECIMAL(10,2) | Yes | Mean payment value for this channel on this date. | `AVG(payment_value)` |
| `channel_revenue_share` | DECIMAL(5,4) | Yes | This channel's gross revenue as a proportion of total gross revenue across all channels on the same date. `0.45` = 45% share. Computed via broadcast join against daily totals. | `gross_revenue / SUM(gross_revenue) OVER (PARTITION BY order_date)` via broadcast join |
| `_batch_id` | STRING | NOT NULL | Pipeline run identifier. | Set at Gold write time |
| `_updated_at` | TIMESTAMP | NOT NULL | Timestamp when this row was last written or updated. | `current_timestamp()` at Gold write time |

### Broadcast join note

`channel_revenue_share` requires the total daily revenue across all channels. Rather than a self-join or window function over a potentially large table, the pipeline computes a small `daily_totals` DataFrame (`order_date → daily_total`) and passes it to `F.broadcast()`. This eliminates the shuffle that a standard join would require, which is significant when `gold_revenue_by_channel` grows to thousands of rows.

---

## Pipeline Metadata Columns

Every table in every layer carries a standard set of pipeline metadata columns. These are never sourced from the upstream data — they are added by the pipeline code.

| Column | Layers present | Type | Description |
|---|---|---|---|
| `_ingested_at` | Bronze, Silver | TIMESTAMP | UTC timestamp when the row was first written to Bronze. Carried forward to Silver for lineage. Set by `current_timestamp()` in `delta_utils.add_ingestion_metadata()`. |
| `_source_file` | Bronze only | STRING | Full file path of the source file for this row. Set by Spark's `input_file_name()`. Dropped in Silver write. |
| `_batch_id` | Bronze, Silver, Gold | STRING | 8-character hex string identifying the pipeline run. Generated by `generate_batch_id()` in `spark_utils.py`. Each layer generates its own `_batch_id` independently. |
| `_ingest_date` | Bronze only | DATE | Date partition column derived from `_ingested_at`. Used for partition pruning on incremental Bronze reads. Dropped in Silver write. |
| `_silver_processed_at` | Silver only | TIMESTAMP | UTC timestamp when the row was written to the Silver table. Distinct from `_ingested_at`. |
| `_updated_at` | Gold only | TIMESTAMP | UTC timestamp when the Gold row was last written or updated via MERGE. |

---

## Data Quality Thresholds Reference

Thresholds are configured in `configs/dev.yml` and `configs/prod.yml` under the `data_quality` key. The pipeline reads these at runtime — no code changes are required to adjust them between environments.

### Orders

| Threshold | Key | Default (dev) | Description |
|---|---|---|---|
| Max null rate on critical columns | `null_tolerance_pct` | `0.01` (1%) | If the quarantine rate for a run exceeds this, a WARNING is emitted. Pipeline does not fail. |
| Duplicate tolerance | `duplicate_tolerance_pct` | `0.0` (0%) | Zero duplicates allowed in Silver — enforced by MERGE INTO upsert. |
| Minimum payment value | `min_order_amount` | `0.01` | Non-null `payment_value` below this threshold causes the row to be quarantined. |
| Maximum payment value | `max_order_amount` | `50000.0` | Non-null `payment_value` above this threshold causes the row to be quarantined. |

### Events

| Threshold | Key | Default (dev) | Description |
|---|---|---|---|
| Max null rate | `null_tolerance_pct` | `0.05` (5%) | Higher tolerance than orders — clickstream data is noisier by nature. |
| Duplicate tolerance | `duplicate_tolerance_pct` | `0.02` (2%) | Small tolerance for cross-batch event duplicates from the streaming source. |

### Quarantine location

Failed rows from Silver DQ checks are written to:

```
s3://ecommerce-lakehouse-prod/bronze/quarantine/orders/
dev: dbfs:/FileStore/lakehouse/bronze/quarantine/orders/
```

The quarantine table is a Delta table and can be queried directly for root-cause analysis without affecting any pipeline layer.
