"""
Reset the 005 "iceberg cost crossover" tables for a clean run — purges the
current data and recreates empty tables (same schemas, matched target file
size) so both catalogs start identical at t=0.

Resets BOTH the self-managed (Glue Catalog) and managed (S3 Tables) sides by
default, so the managed-vs-semi-managed comparison isn't skewed by one side
having a different write history. Use --glue-only to leave S3 Tables untouched.

Usage:
    python setup/reset_tables.py --yes            # both sides
    python setup/reset_tables.py --yes --glue-only

Reads AWS credentials from the environment and cloud resource names from .env
(see config.py).
"""
import argparse

from pyiceberg.catalog import load_catalog

# config.py/schemas.py live one level up at the experiments root.
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GLUE_DATABASE as DATABASE
from config import REGION, WAREHOUSE, get_s3tables_bucket_arn
from create_tables import ensure_namespace, ensure_table
from create_s3tables import (
    get_catalog as s3_get_catalog,
    ensure_namespace as s3_ensure_namespace,
    ensure_table as s3_ensure_table,
    configure_maintenance,
)
from schemas import (
    DIM_PRODUCTS_SCHEMA,
    ORDER_EVENTS_PARTITION_SPEC,
    ORDER_EVENTS_SCHEMA,
    ORDERS_CURRENT_SCHEMA,
    TABLE_PROPERTIES,
)

TABLES = ["order_events", "order_events_hourly", "orders_current", "dim_products"]


def get_catalog():
    return load_catalog(
        "glue_005_reset",
        **{
            "type": "glue",
            "warehouse": WAREHOUSE,
            "glue.region": REGION,
            "s3.region": REGION,
        },
    )


def reset_glue():
    catalog = get_catalog()
    print(f"Purging Glue tables in '{DATABASE}':")
    for name in TABLES:
        identifier = (DATABASE, name)
        try:
            catalog.purge_table(identifier)
            print(f"[purge] Dropped Glue '{name}' (metadata + data files)")
        except Exception as exc:
            print(f"[skip] Could not purge Glue '{name}' (may not exist yet): {exc}")

    ensure_namespace(catalog)
    ensure_table(catalog, "order_events", ORDER_EVENTS_SCHEMA,
                 partition_spec=ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    ensure_table(catalog, "order_events_hourly", ORDER_EVENTS_SCHEMA,
                 partition_spec=ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    ensure_table(catalog, "orders_current", ORDERS_CURRENT_SCHEMA, properties=TABLE_PROPERTIES)
    ensure_table(catalog, "dim_products", DIM_PRODUCTS_SCHEMA)
    print("[ok] Recreated empty Glue tables")


def reset_s3tables():
    bucket_arn = get_s3tables_bucket_arn()
    catalog = s3_get_catalog(bucket_arn)
    print(f"Dropping S3 Tables in '{DATABASE}':")
    for name in TABLES:
        identifier = (DATABASE, name)
        try:
            # S3 Tables' REST catalog rejects a plain drop_table; it only allows purge=True.
            catalog.purge_table(identifier)
            print(f"[purge] Dropped S3 Tables '{name}'")
        except Exception as exc:
            print(f"[skip] Could not purge S3 Tables '{name}' (may not exist yet): {exc}")

    s3_ensure_namespace(catalog)
    s3_ensure_table(catalog, "order_events", ORDER_EVENTS_SCHEMA,
                    ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    s3_ensure_table(catalog, "order_events_hourly", ORDER_EVENTS_SCHEMA,
                    ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    s3_ensure_table(catalog, "orders_current", ORDERS_CURRENT_SCHEMA, properties=TABLE_PROPERTIES)
    s3_ensure_table(catalog, "dim_products", DIM_PRODUCTS_SCHEMA)
    configure_maintenance(bucket_arn, ["order_events", "order_events_hourly", "orders_current"])
    print("[ok] Recreated empty S3 Tables + reconfigured maintenance")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--yes", action="store_true", help="skip the confirmation prompt (required for non-interactive use)"
    )
    parser.add_argument(
        "--glue-only", action="store_true", help="reset only the Glue tables, leave S3 Tables alone"
    )
    args = parser.parse_args()

    scope = "Glue only" if args.glue_only else "Glue AND S3 Tables"
    print(f"About to PURGE + recreate empty tables ({scope}). ALL current data will be lost.")
    if not args.yes:
        confirm = input("Type 'reset' to continue: ")
        if confirm != "reset":
            print("Aborted.")
            return

    reset_glue()
    if not args.glue_only:
        reset_s3tables()
    print("[done] Reset complete. Re-run setup/setup_glue_optimizer.py "
          "(optimizers don't survive a table recreate).")


if __name__ == "__main__":
    main()
