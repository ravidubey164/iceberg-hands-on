"""
Load dim_products.ndjson (from fetch_products.py) into the dim_products
Iceberg table. This is the slow-changing dimension: seeded once and rarely
refreshed, so each run does a full overwrite rather than an incremental
append/upsert.

Usage:
    python setup/load_dim_products.py [--in dim_products.ndjson]

Reads AWS credentials from the environment and cloud resource names from .env
(see config.py).
"""
import argparse
import json
import os

import pyarrow as pa
from pyiceberg.catalog import load_catalog

# config.py/schemas.py live one level up at the experiments root.
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GLUE_DATABASE as DATABASE
from config import REGION, WAREHOUSE

HERE = os.path.dirname(os.path.abspath(__file__))

DIM_PRODUCTS_ARROW_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("base_currency", pa.string()),
        ("quote_currency", pa.string()),
        ("quote_increment", pa.string()),
        ("base_increment", pa.string()),
        ("display_name", pa.string()),
        ("min_market_funds", pa.string()),
        ("margin_enabled", pa.bool_()),
        ("post_only", pa.bool_()),
        ("limit_only", pa.bool_()),
        ("cancel_only", pa.bool_()),
        ("status", pa.string()),
        ("status_message", pa.string()),
        ("trading_disabled", pa.bool_()),
        ("fx_stablecoin", pa.bool_()),
        ("max_slippage_percentage", pa.string()),
        ("auction_mode", pa.bool_()),
        ("high_bid_limit_percentage", pa.string()),
    ]
)


def get_catalog():
    return load_catalog(
        "glue_005",
        **{
            "type": "glue",
            "warehouse": WAREHOUSE,
            "glue.region": REGION,
            "s3.region": REGION,
        },
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", default=os.path.join(HERE, "dim_products.ndjson"))
    args = parser.parse_args()

    with open(args.in_path) as f:
        products = [json.loads(line) for line in f if line.strip()]

    rows = [{field.name: p.get(field.name) for field in DIM_PRODUCTS_ARROW_SCHEMA} for p in products]
    arrow_table = pa.Table.from_pylist(rows, schema=DIM_PRODUCTS_ARROW_SCHEMA)

    catalog = get_catalog()
    table = catalog.load_table((DATABASE, "dim_products"))
    table.overwrite(arrow_table)
    print(f"[overwrite] {arrow_table.num_rows} rows -> dim_products")


if __name__ == "__main__":
    main()
