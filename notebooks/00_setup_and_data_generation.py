# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 — Setup and Data Generation
# MAGIC
# MAGIC **Purpose:** Bootstrap the development environment and generate realistic synthetic
# MAGIC e-commerce data in the raw landing zone. This is the entry point for the full
# MAGIC pipeline demo.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Verifies the project is on the Python path
# MAGIC 2. Loads the `dev` environment config (cloud paths → DBFS simulation)
# MAGIC 3. Generates ~660K rows of synthetic data across 5 datasets
# MAGIC 4. Confirms raw files are in place for the Bronze ingestion notebook
# MAGIC
# MAGIC **Run this once** before running notebooks 01, 02, and 03.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup

# COMMAND ----------

import os
import sys
from pathlib import Path

# Add project root to Python path so src.* imports work in Databricks Repos
# When running locally, adjust this path to your repo root
project_root = str(Path(os.getcwd()).parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Project root: {project_root}")
print(f"Python path includes project root: {project_root in sys.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Environment Configuration
# MAGIC
# MAGIC The config layer is the foundation of the cloud simulation pattern.
# MAGIC All pipeline code uses canonical `s3://` paths. In dev, `resolve_path()`
# MAGIC transparently maps them to `dbfs:/FileStore/lakehouse/` (or `/tmp/lakehouse/`
# MAGIC when running outside Databricks).

# COMMAND ----------

from src.utils.spark_utils import load_config, get_spark, resolve_path

ENV = os.getenv("ENV_NAME", "dev")
config = load_config(ENV)

print(f"Environment      : {config['environment']}")
print(f"Cloud provider   : {config['cloud_provider']}")
print(f"Simulate paths   : {config['simulate_cloud_paths']}")
print()
print("Canonical cloud paths (production targets):")
for layer, path in config["paths"].items():
    resolved = resolve_path(path, config)
    print(f"  {layer:<12} {path}")
    print(f"  {'':12} → resolved: {resolved}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialise SparkSession
# MAGIC
# MAGIC In Databricks the session already exists — `get_spark()` configures it
# MAGIC with Delta Lake and performance settings from the config, then returns
# MAGIC the active session. It never creates a duplicate.

# COMMAND ----------

spark = get_spark(config)

print(f"Spark version    : {spark.version}")
print(f"Shuffle partitions : {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"Adaptive enabled   : {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Optimize write     : {spark.conf.get('spark.databricks.delta.optimizeWrite.enabled', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Data
# MAGIC
# MAGIC The generator creates realistic e-commerce data using the `Faker` library
# MAGIC with fixed seeds for reproducibility. Data is written to the raw landing zone
# MAGIC using the same `resolve_path()` function the production pipeline uses.
# MAGIC
# MAGIC | Dataset | Rows | Description |
# MAGIC |---|---|---|
# MAGIC | `orders.csv` | 100,000 | Orders with realistic status distribution and ~2% dirty data |
# MAGIC | `customers.csv` | 50,000 | Customers with segments, LTV, and US state distribution |
# MAGIC | `products.csv` | 10,000 | Products across 10 categories |
# MAGIC | `order_items.csv` | ~250,000 | Line items (~2.5 items per order average) |
# MAGIC | `clickstream/` | 500,000 | Events partitioned by date, spanning last 30 days |

# COMMAND ----------

import subprocess

raw_path = resolve_path(config["paths"]["raw"], config)
print(f"Writing raw data to: {raw_path}")
print(f"(Cloud equivalent : {config['paths']['raw']})")
print()

# Run the generator — same arguments the production bootstrap script uses
result = subprocess.run(
    [
        sys.executable,
        f"{project_root}/scripts/generate_sample_data.py",
        "--env",      ENV,
        "--rows",     "100000",
        "--events",   "500000",
        "--products", "10000",
        "--customers","50000",
    ],
    capture_output=True,
    text=True,
)

print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr)
    raise RuntimeError("Data generation failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Raw Landing Zone
# MAGIC
# MAGIC Confirm every expected file is present before triggering Bronze ingestion.

# COMMAND ----------

import os

expected_files = ["orders.csv", "customers.csv", "products.csv", "order_items.csv"]

print(f"Checking raw landing zone: {raw_path}")
print()

all_present = True
for fname in expected_files:
    fpath = os.path.join(raw_path, fname)
    if os.path.exists(fpath):
        size_mb = os.path.getsize(fpath) / (1024 * 1024)
        print(f"  ✓  {fname:<25} {size_mb:.1f} MB")
    else:
        print(f"  ✗  {fname:<25} MISSING")
        all_present = False

# Check clickstream partition count
cs_path = os.path.join(raw_path, "clickstream")
if os.path.exists(cs_path):
    partitions = [d for d in os.listdir(cs_path) if d.startswith("date=")]
    print(f"  ✓  clickstream/             {len(partitions)} date partitions")
else:
    print(f"  ✗  clickstream/             MISSING")
    all_present = False

print()
if all_present:
    print("✅ Raw landing zone is ready. Proceed to notebook 01.")
else:
    print("❌ Some files are missing. Re-run the data generation cell above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Data Preview
# MAGIC
# MAGIC Sanity-check the raw orders file before ingestion.
# MAGIC Note that all values are raw strings at this point — type casting
# MAGIC happens in the Silver layer, not here.

# COMMAND ----------

import pandas as pd

orders_preview = pd.read_csv(
    os.path.join(raw_path, "orders.csv"),
    nrows=5,
    dtype=str,  # match Bronze schema — everything is a string
)

print(f"Raw orders — first 5 rows (all columns are raw strings, as Bronze will receive them):")
print(orders_preview.to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Status Distribution Preview
# MAGIC
# MAGIC The generator injects ~2% dirty data (null payment values, out-of-range amounts)
# MAGIC to give the Silver DQ rules something to catch.

# COMMAND ----------

orders_sample = pd.read_csv(os.path.join(raw_path, "orders.csv"), dtype=str)

print("Order status distribution:")
print(orders_sample["order_status"].value_counts().to_string())
print()
print(f"Null payment_value rows : {orders_sample['payment_value'].isna().sum():,}")
print(f"Total rows              : {len(orders_sample):,}")
print()
print("✅ Setup complete. Run notebook 01 next.")
