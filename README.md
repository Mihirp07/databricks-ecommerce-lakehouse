# Databricks E-Commerce Lakehouse

**Production-grade data engineering pipeline** built on Databricks Lakehouse Architecture. Ingests, transforms, and serves 600K+ daily records across streaming clickstream events, batch orders, and product catalog data using Apache Spark, Delta Lake, and the medallion architecture pattern.

> Designed as a real-world production system. All pipeline code uses **cloud-native S3/ADLS paths** and is transparently simulated via DBFS for free local execution.

---

## Architecture

```
Kafka / Event Hub   PostgreSQL CDC   REST API (Catalog)
       │                  │                │
       └──────────────────┴────────────────┘
                          │
            s3://ecommerce-lakehouse-prod/raw/
                 (dev: dbfs:/FileStore/lakehouse/raw/)
                          │
         ┌────────────────▼─────────────────┐
         │         BRONZE LAYER              │
         │  s3://…/bronze/  (Delta)          │
         │  · Append-only, schema enforced   │
         │  · Streaming: Auto Loader events  │
         │  · Batch: orders, customers, products │
         └────────────────┬─────────────────┘
                          │  MERGE INTO
         ┌────────────────▼─────────────────┐
         │         SILVER LAYER              │
         │  s3://…/silver/  (Delta)          │
         │  · Type casting, deduplication    │
         │  · DQ rules, quarantine bad rows  │
         │  · Z-ORDER for join optimization  │
         │  · Change Data Feed enabled       │
         └──────────────┬──────────────┬────┘
                    DQ Gate            │
                   (blocks Gold)       │
         ┌─────────────▼──────────────▼────┐
         │          GOLD LAYER              │
         │  s3://…/gold/  (Delta)           │
         │  · Revenue KPIs (daily/monthly)  │
         │  · Customer 360 (LTV, segments)  │
         │  · Product performance metrics   │
         └──────────────────────────────────┘
                          │
         Databricks SQL · Tableau · Power BI · APIs
```

---

## Cloud Simulation — Run Free, Deploy to Real Cloud

This project uses **real cloud-style paths** (`s3://` and `abfss://`) throughout all pipeline code. A single config flag (`simulate_cloud_paths: true`) transparently redirects them to DBFS or local storage, letting you run the full pipeline for free.

| Environment | Storage path | What changes |
|---|---|---|
| **dev** (free) | `dbfs:/FileStore/lakehouse/...` | One config flag |
| **AWS prod** | `s3://ecommerce-lakehouse-prod/...` | IAM instance profile on cluster |
| **Azure prod** | `abfss://bronze@ecommercedatalake.dfs.core.windows.net/...` | Service principal credentials |

No application code changes required between environments. See `configs/dev.yml` vs `configs/prod.yml`.

---

## Key Features

| Feature | Implementation |
|---|---|
| Streaming ingestion | Spark Structured Streaming + Auto Loader (`cloudFiles` format) |
| Batch ingestion | PySpark CSV/JSON readers with schema enforcement |
| Medallion architecture | Bronze → Silver → Gold with clear separation of concerns |
| Delta Lake upserts | `MERGE INTO` via `DeltaTable.merge()` — idempotent re-runs |
| Schema enforcement | `mergeSchema: false` — new columns rejected at Bronze |
| Z-ORDER optimization | `ZORDER BY (customer_id, order_date)` on Silver for join performance |
| Data quality | Custom expectation framework — null rates, value sets, referential integrity |
| Quarantine pattern | Bad rows land in `bronze/quarantine/` — no silent data loss |
| Change Data Feed | Silver → Gold incremental propagation via CDF |
| Time travel | `versionAsOf` and `timestampAsOf` for audit and rollback |
| VACUUM | Configurable retention window, dry-run mode |
| Orchestration | Databricks Workflows YAML — DAG with fan-out, DQ gate, scheduling |
| Unit tests | `pytest` suite with local SparkSession — no cluster required |

---

## Project Structure

```
databricks-ecommerce-lakehouse/
├── configs/
│   ├── dev.yml                   ← Cloud paths + dev simulation flag
│   ├── prod.yml                  ← Real S3/ADLS paths, no simulation
│   └── schema_registry/          ← JSON schemas for each Delta table
├── src/
│   ├── utils/
│   │   ├── spark_utils.py        ← SparkSession factory + path resolver
│   │   ├── delta_utils.py        ← MERGE, OPTIMIZE, VACUUM, time travel
│   │   └── logger.py             ← Structured JSON logging
│   ├── bronze/
│   │   ├── bronze_orders.py      ← Batch CSV ingestion → Bronze Delta
│   │   └── bronze_events_stream.py ← Auto Loader streaming → Bronze Delta
│   ├── silver/
│   │   └── silver_orders.py      ← Cast + validate + dedupe + MERGE
│   ├── gold/
│   │   └── gold_revenue_summary.py ← Daily/monthly KPIs + channel breakdown
│   └── quality/
│       └── expectations.py       ← DQ expectation framework
├── notebooks/                    ← Databricks notebooks (coming soon)
├── jobs/
│   └── full_pipeline_job.yml     ← Databricks Workflows job definition
├── tests/
│   └── test_silver_orders.py     ← pytest unit tests (no cluster needed)
├── scripts/
│   └── generate_sample_data.py   ← Synthetic data generator (600K+ rows)
└── docs/
    └── data_dictionary.md
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- Java 11 (for local PySpark)
- Databricks account (free Community Edition works)

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/databricks-ecommerce-lakehouse.git
cd databricks-ecommerce-lakehouse
pip install -r requirements.txt
```

### 2. Generate synthetic data

```bash
python scripts/generate_sample_data.py --env dev --rows 100000 --events 500000
```

This creates realistic e-commerce data in `/tmp/lakehouse/raw/`:
- `orders.csv` — 100K orders with realistic status distribution
- `customers.csv` — 50K customers with segments and LTV
- `products.csv` — 10K products across 10 categories
- `order_items.csv` — ~250K line items
- `clickstream/` — 500K events partitioned by date

### 3. Run the pipeline locally

```bash
# Bronze ingestion
python -m src.bronze.bronze_orders

# Silver transformation
python -m src.silver.silver_orders

# Gold aggregation
python -m src.gold.gold_revenue_summary
```

### 4. Run tests

```bash
pytest tests/ -v
```

### 5. On Databricks (Community Edition)

1. Upload the repo to Databricks Repos
2. Set `ENV_NAME=dev` as a cluster environment variable
3. Run `scripts/generate_sample_data.py` to populate the raw landing zone
4. Run each pipeline module in order: Bronze → Silver → Gold (see step 3 above)
5. To deploy as a scheduled job: import `jobs/full_pipeline_job.yml` via the Workflows UI

---

## Delta Lake Features Demonstrated

### Schema Enforcement
```python
# Bronze write — rejects new columns added upstream
df.write.format("delta").option("mergeSchema", "false").save(path)
```

### MERGE INTO (Upsert)
```python
DeltaTable.forPath(spark, path).alias("target")
  .merge(source.alias("source"), "target.order_id = source.order_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
```

### Z-ORDER for Query Performance
```python
spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (customer_id, order_date)")
# Result: 10-100x faster filtered reads on customer_id or date ranges
```

### Time Travel
```python
# Read table as it existed last week
spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)

# Roll back to a specific version
spark.read.format("delta").option("versionAsOf", 5).load(path)
```

### Change Data Feed
```python
# Silver → Gold incremental propagation
spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", last_gold_version) \
  .load(silver_path)
```

---

## Performance Optimizations Applied

| Optimization | Where | Impact |
|---|---|---|
| Partition by `_ingest_date` | Bronze | Skip non-relevant partitions on incremental reads |
| Partition by `order_date` | Silver | Efficient date-range aggregations in Gold |
| Z-ORDER `(customer_id, order_date)` | Silver orders | 10-100x faster customer-scoped queries |
| `optimizeWrite: true` | All layers | Auto-sizes output files (~128 MB each) |
| `autoCompact: true` | All layers | Merges small files automatically post-write |
| `broadcast()` join | Gold channel | Eliminates shuffle for small dimension join |
| `.cache()` | Gold pipeline | Silver read once, aggregated 3× |
| Adaptive Query Execution | Spark session | Auto-adjusts partition count post-shuffle |
| Window function dedup | Silver | Single-pass deduplication vs self-join |

---

## Data Quality Framework

Custom DQ expectations with configurable thresholds per table:

```python
report = DQReport(table="orders", layer="silver", batch_id=batch_id)
report.add(expect_no_nulls(df, "order_id",     threshold_pct=0.0))
report.add(expect_no_duplicates(df, "order_id"))
report.add(expect_column_between(df, "payment_value", min_val=0.01, max_val=50000))
report.add(expect_referential_integrity(df, "customer_id", customers_df, "customer_id"))

# DQ gate in Databricks Workflow: failure blocks all Gold tasks
if not report.passed:
    raise DQException(f"DQ failed: {report.summary()}")
```

Failed rows are written to `bronze/quarantine/` — never silently dropped.

---

## Real Cloud Deployment

### AWS

```yaml
# configs/prod.yml
simulate_cloud_paths: false
paths:
  bronze: "s3://ecommerce-lakehouse-prod/bronze"
  silver: "s3://ecommerce-lakehouse-prod/silver"
  gold:   "s3://ecommerce-lakehouse-prod/gold"
```

Attach an IAM Instance Profile to the Databricks cluster with `s3:GetObject` and `s3:PutObject` on the bucket. No code changes needed.

### Azure

```yaml
paths:
  bronze: "abfss://bronze@ecommercedatalake.dfs.core.windows.net/"
  silver: "abfss://silver@ecommercedatalake.dfs.core.windows.net/"
  gold:   "abfss://gold@ecommercedatalake.dfs.core.windows.net/"
```

Mount ADLS Gen2 via Service Principal or configure OAuth in the cluster's Spark config.

---

## Certification Context

Built to demonstrate skills assessed by the **Databricks Certified Data Engineer Professional** exam:

- Advanced Spark transformations (window functions, broadcast joins, AQE)
- Delta Lake internals (transaction log, file layout, OPTIMIZE mechanics)
- Structured Streaming with exactly-once semantics
- Production patterns (schema enforcement, DQ gates, quarantine, lineage)
- Lakehouse architecture with medallion layers
- Workflow orchestration with task dependencies and failure handling

---

## License

MIT
