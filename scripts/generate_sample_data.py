"""
scripts/generate_sample_data.py
--------------------------------
Generates realistic synthetic e-commerce data and writes it to the
raw landing zone (cloud path → resolved to DBFS or local in dev).

Produces:
  - orders.csv          ~100K rows
  - order_items.csv     ~300K rows
  - customers.csv       ~50K rows
  - products.csv        ~10K rows
  - clickstream/        partitioned JSON files (~500K events)

Run from project root:
  python scripts/generate_sample_data.py --env dev --rows 100000

Requirements:
  pip install faker pandas pyarrow pyyaml
"""

import argparse
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pandas as pd
import yaml
from faker import Faker

# ── Add project root to path ────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

# ── Constants ────────────────────────────────────────────────────────────────
CATEGORIES    = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books",
                 "Beauty", "Toys", "Automotive", "Food & Grocery", "Health"]
ORDER_STATUSES = ["delivered", "shipped", "processing", "cancelled", "returned"]
STATUS_WEIGHTS = [0.60, 0.20, 0.10, 0.07, 0.03]
PAYMENT_TYPES  = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
EVENT_TYPES    = ["page_view", "product_view", "add_to_cart", "remove_from_cart",
                  "checkout_start", "checkout_complete", "search", "wishlist_add"]
EVENT_WEIGHTS  = [0.35, 0.25, 0.15, 0.05, 0.08, 0.04, 0.06, 0.02]
DEVICES        = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.45, 0.45, 0.10]

# ── Path resolver (mirrors src/utils/spark_utils.py logic) ──────────────────
def load_config(env: str) -> dict:
    cfg_path = Path(__file__).parent.parent / "configs" / f"{env}.yml"
    with open(cfg_path) as f:
        return yaml.safe_load(f)

def resolve_path(cloud_path: str, config: dict) -> str:
    """Standalone version of resolve_path for non-Spark scripts."""
    if not config.get("simulate_cloud_paths", True):
        return cloud_path
    root = config["storage"]["local_root"]
    # Strip s3:// bucket prefix
    if cloud_path.startswith("s3://") or cloud_path.startswith("s3a://"):
        suffix = "/" + cloud_path.split("/", 3)[-1] if cloud_path.count("/") >= 3 else ""
        return f"{root.rstrip('/')}{suffix}"
    # Strip abfss:// container prefix
    if cloud_path.startswith("abfss://"):
        parts  = cloud_path.replace("abfss://", "").split("/", 1)
        suffix = f"/{parts[1]}" if len(parts) > 1 else ""
        container = parts[0].split("@")[0]
        return f"{root.rstrip('/')}/{container}{suffix}"
    return cloud_path

# ── Generators ───────────────────────────────────────────────────────────────

def gen_products(n: int = 10_000) -> pd.DataFrame:
    print(f"  Generating {n:,} products...")
    rows = []
    for _ in range(n):
        category = random.choice(CATEGORIES)
        price    = round(random.uniform(4.99, 999.99), 2)
        rows.append({
            "product_id":          str(uuid.uuid4()),
            "product_name":        fake.catch_phrase()[:80],
            "category":            category,
            "subcategory":         fake.word().capitalize(),
            "brand":               fake.company()[:40],
            "price":               price,
            "cost":                round(price * random.uniform(0.3, 0.7), 2),
            "weight_kg":           round(random.uniform(0.1, 20.0), 2),
            "stock_quantity":      random.randint(0, 5000),
            "is_active":           random.choices([True, False], weights=[0.92, 0.08])[0],
            "created_at":          fake.date_time_between(
                                       start_date="-3y", end_date="-1y"
                                   ).isoformat(),
            "updated_at":          fake.date_time_between(
                                       start_date="-1y", end_date="now"
                                   ).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_customers(n: int = 50_000) -> pd.DataFrame:
    print(f"  Generating {n:,} customers...")
    rows = []
    states = ["CA", "TX", "FL", "NY", "WA", "IL", "PA", "OH", "GA", "NC"]
    for _ in range(n):
        signup = fake.date_time_between(start_date="-4y", end_date="-1m")
        rows.append({
            "customer_id":          str(uuid.uuid4()),
            "first_name":           fake.first_name(),
            "last_name":            fake.last_name(),
            "email":                fake.unique.email(),
            "phone":                fake.phone_number()[:20],
            "city":                 fake.city(),
            "state":                random.choice(states),
            "zip_code":             fake.zipcode(),
            "country":              "US",
            "segment":              random.choices(
                                        ["bronze", "silver", "gold", "platinum"],
                                        weights=[0.50, 0.30, 0.15, 0.05]
                                    )[0],
            "signup_date":          signup.date().isoformat(),
            "last_login_date":      fake.date_time_between(
                                        start_date=signup, end_date="now"
                                    ).date().isoformat(),
            "is_subscribed_email":  random.choices([True, False], weights=[0.65, 0.35])[0],
            "lifetime_orders":      random.randint(1, 200),
            "lifetime_value":       round(random.uniform(10.0, 15000.0), 2),
        })
    return pd.DataFrame(rows)


def gen_orders(
    customers: pd.DataFrame,
    n: int = 100_000,
) -> pd.DataFrame:
    print(f"  Generating {n:,} orders...")
    customer_ids = customers["customer_id"].tolist()
    rows = []
    for _ in range(n):
        purchase_ts   = fake.date_time_between(start_date="-2y", end_date="now")
        approved_ts   = purchase_ts + timedelta(minutes=random.randint(5, 60))
        delivered_ts  = approved_ts + timedelta(days=random.randint(2, 14))
        estimated_ts  = approved_ts + timedelta(days=random.randint(3, 10))
        status        = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]

        # Introduce ~2% dirty data (nulls on non-critical fields)
        payment_value = round(random.uniform(5.0, 2500.0), 2)
        if random.random() < 0.02:
            payment_value = None   # simulate missing payment value

        rows.append({
            "order_id":                      str(uuid.uuid4()),
            "customer_id":                   random.choice(customer_ids),
            "order_status":                  status,
            "order_purchase_timestamp":      purchase_ts.isoformat(),
            "order_approved_at":             approved_ts.isoformat() if status != "cancelled" else None,
            "order_delivered_carrier_date":  (purchase_ts + timedelta(days=1)).isoformat()
                                             if status in ("delivered", "shipped") else None,
            "order_delivered_customer_date": delivered_ts.isoformat()
                                             if status == "delivered" else None,
            "order_estimated_delivery_date": estimated_ts.isoformat(),
            "payment_value":                 payment_value,
            "payment_type":                  random.choice(PAYMENT_TYPES),
            "promo_code":                    fake.bothify("PROMO-####") if random.random() < 0.15 else None,
            "channel":                       random.choices(
                                                 ["web", "mobile_app", "marketplace", "social"],
                                                 weights=[0.45, 0.35, 0.15, 0.05]
                                             )[0],
        })
    return pd.DataFrame(rows)


def gen_order_items(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    avg_items_per_order: float = 2.5,
) -> pd.DataFrame:
    product_ids    = products["product_id"].tolist()
    product_prices = dict(zip(products["product_id"], products["price"]))
    rows = []
    print(f"  Generating order items (~{int(len(orders) * avg_items_per_order):,} rows)...")
    for _, order in orders.iterrows():
        n_items = max(1, int(random.gauss(avg_items_per_order, 1.2)))
        for i in range(n_items):
            pid      = random.choice(product_ids)
            quantity = random.randint(1, 5)
            price    = product_prices.get(pid, 29.99)
            rows.append({
                "order_item_id":    str(uuid.uuid4()),
                "order_id":         order["order_id"],
                "product_id":       pid,
                "seller_id":        str(uuid.uuid4())[:8],
                "item_sequence":    i + 1,
                "quantity":         quantity,
                "unit_price":       price,
                "total_price":      round(price * quantity, 2),
                "freight_value":    round(random.uniform(0.0, 25.0), 2),
            })
    return pd.DataFrame(rows)


def gen_clickstream_events(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    n: int = 500_000,
) -> List[dict]:
    """Generate streaming-style clickstream events as line-delimited JSON."""
    print(f"  Generating {n:,} clickstream events...")
    customer_ids = customers["customer_id"].tolist()
    product_ids  = products["product_id"].tolist()
    rows = []
    for _ in range(n):
        ts = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
        rows.append({
            "event_id":         str(uuid.uuid4()),
            "session_id":       str(uuid.uuid4())[:16],
            "customer_id":      random.choice(customer_ids) if random.random() > 0.2 else None,
            "event_type":       event_type,
            "event_timestamp":  ts.isoformat(),
            "product_id":       random.choice(product_ids)
                                if event_type in ("product_view", "add_to_cart", "wishlist_add")
                                else None,
            "page_url":         f"/{fake.uri_path()}",
            "referrer":         random.choices(
                                    ["google.com", "facebook.com", "instagram.com", "direct", "email"],
                                    weights=[0.35, 0.20, 0.15, 0.20, 0.10]
                                )[0],
            "device_type":      random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
            "user_agent":       fake.user_agent(),
            "ip_address":       fake.ipv4_public(),
            "country_code":     "US",
        })
    return rows


# ── Writer ───────────────────────────────────────────────────────────────────

def write_data(config: dict, args: argparse.Namespace) -> None:
    raw_cloud_path = config["paths"]["raw"]
    raw_path       = resolve_path(raw_cloud_path, config)

    print(f"\n📁 Writing raw data to: {raw_path}")
    print(f"   (Cloud path: {raw_cloud_path})\n")

    os.makedirs(raw_path, exist_ok=True)

    # ── Products ─────────────────────────────────────────────────────────────
    products = gen_products(args.products)
    products.to_csv(f"{raw_path}/products.csv", index=False)
    print(f"  ✓ products.csv  ({len(products):,} rows)")

    # ── Customers ────────────────────────────────────────────────────────────
    customers = gen_customers(args.customers)
    customers.to_csv(f"{raw_path}/customers.csv", index=False)
    print(f"  ✓ customers.csv ({len(customers):,} rows)")

    # ── Orders ───────────────────────────────────────────────────────────────
    orders = gen_orders(customers, args.rows)
    orders.to_csv(f"{raw_path}/orders.csv", index=False)
    print(f"  ✓ orders.csv    ({len(orders):,} rows)")

    # ── Order Items ──────────────────────────────────────────────────────────
    items = gen_order_items(orders, products)
    items.to_csv(f"{raw_path}/order_items.csv", index=False)
    print(f"  ✓ order_items.csv ({len(items):,} rows)")

    # ── Clickstream (partitioned by date) ────────────────────────────────────
    events   = gen_clickstream_events(customers, products, args.events)
    events_df = pd.DataFrame(events)
    events_df["event_date"] = pd.to_datetime(events_df["event_timestamp"]).dt.date

    events_dir = f"{raw_path}/clickstream"
    os.makedirs(events_dir, exist_ok=True)

    for date, group in events_df.groupby("event_date"):
        date_dir = f"{events_dir}/date={date}"
        os.makedirs(date_dir, exist_ok=True)
        out_path = f"{date_dir}/events.json"
        group.drop(columns=["event_date"]).to_json(
            out_path, orient="records", lines=True
        )

    print(f"  ✓ clickstream/   ({len(events_df):,} events, "
          f"{events_df['event_date'].nunique()} partitions)")

    # ── Summary ──────────────────────────────────────────────────────────────
    total = len(products) + len(customers) + len(orders) + len(items) + len(events_df)
    print(f"\n✅ Total rows generated: {total:,}")
    print(f"   Raw landing zone:     {raw_path}")
    print(f"   Cloud equivalent:     {raw_cloud_path}")


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce data for the Lakehouse pipeline"
    )
    parser.add_argument("--env",       default="dev",     help="Environment: dev | prod")
    parser.add_argument("--rows",      type=int, default=100_000, help="Number of orders")
    parser.add_argument("--events",    type=int, default=500_000, help="Number of clickstream events")
    parser.add_argument("--products",  type=int, default=10_000,  help="Number of products")
    parser.add_argument("--customers", type=int, default=50_000,  help="Number of customers")
    args = parser.parse_args()

    print("=" * 60)
    print("  E-Commerce Lakehouse — Synthetic Data Generator")
    print(f"  Environment : {args.env}")
    print(f"  Orders      : {args.rows:,}")
    print(f"  Events      : {args.events:,}")
    print("=" * 60)

    config = load_config(args.env)
    write_data(config, args)


if __name__ == "__main__":
    main()
