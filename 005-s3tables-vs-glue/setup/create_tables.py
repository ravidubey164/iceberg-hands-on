"""
Create the Glue Data Catalog database + the three self-managed Iceberg tables
for the 005 "iceberg cost crossover" experiment, in the general-purpose bucket
provisioned by provision.py.

Usage:
    python setup/create_tables.py

Reads AWS credentials from the environment and cloud resource names from .env
(see config.py and .env.example). Designed to be idempotent: safe to re-run.
"""
from pyiceberg.catalog import load_catalog

# config.py/schemas.py live one level up at the experiments root.
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GLUE_DATABASE as DATABASE
from config import REGION, WAREHOUSE
from schemas import (
    DIM_PRODUCTS_SCHEMA,
    ORDER_EVENTS_PARTITION_SPEC,
    ORDER_EVENTS_SCHEMA,
    ORDERS_CURRENT_SCHEMA,
    TABLE_PROPERTIES,
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


def ensure_namespace(catalog):
    existing = [ns[0] for ns in catalog.list_namespaces()]
    if DATABASE in existing:
        print(f"[ok] Namespace '{DATABASE}' already exists")
    else:
        print(f"[create] Creating namespace '{DATABASE}'...")
        catalog.create_namespace(DATABASE)
        print(f"[ok] Created namespace '{DATABASE}'")


def ensure_table(catalog, name, schema, partition_spec=None, properties=None):
    identifier = (DATABASE, name)
    if catalog.table_exists(identifier):
        print(f"[ok] Table '{DATABASE}.{name}' already exists")
        return catalog.load_table(identifier)

    print(f"[create] Creating table '{DATABASE}.{name}'...")
    kwargs = {"identifier": identifier, "schema": schema}
    if partition_spec is not None:
        kwargs["partition_spec"] = partition_spec
    if properties is not None:
        kwargs["properties"] = properties
    table = catalog.create_table(**kwargs)
    print(f"[ok] Created table '{DATABASE}.{name}'")
    return table


def main():
    print(f"Region: {REGION}")
    print(f"Warehouse: {WAREHOUSE}")
    catalog = get_catalog()
    ensure_namespace(catalog)

    ensure_table(catalog, "order_events", ORDER_EVENTS_SCHEMA, ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    # Same trades as order_events, but written once an hour (fewer, larger files) so
    # compaction cost can be plotted against file RATE, not raw volume.
    ensure_table(catalog, "order_events_hourly", ORDER_EVENTS_SCHEMA, ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    ensure_table(catalog, "orders_current", ORDERS_CURRENT_SCHEMA, properties=TABLE_PROPERTIES)
    ensure_table(catalog, "dim_products", DIM_PRODUCTS_SCHEMA)

    print("\n--- Summary ---")
    print(f"Database: {DATABASE}")
    print("Tables: order_events, order_events_hourly, orders_current, dim_products")


if __name__ == "__main__":
    main()
