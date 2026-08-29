"""
Create the namespace + three Iceberg tables in the S3 Tables bucket
provisioned by provision.py — the managed baseline for the 005 "iceberg cost
crossover" experiment. Same schemas as create_tables.py (see schemas.py), so
both sides compare apples-to-apples; S3 Tables handles compaction/snapshot
cleanup automatically, no maintenance runtime to attach here.

Usage:
    python setup/create_s3tables.py

Connects via the Amazon S3 Tables Iceberg REST endpoint (SigV4-signed), per
https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html
Reads AWS credentials from the environment and cloud resource names from .env
(see config.py). Designed to be idempotent: safe to re-run.
"""
from pyiceberg.catalog import load_catalog
import boto3

# config.py/schemas.py live one level up at the experiments root.
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GLUE_DATABASE as NAMESPACE
from config import REGION, get_s3tables_bucket_arn
from schemas import (
    DIM_PRODUCTS_SCHEMA,
    ORDER_EVENTS_PARTITION_SPEC,
    ORDER_EVENTS_SCHEMA,
    ORDERS_CURRENT_SCHEMA,
    TABLE_PROPERTIES,
    TARGET_FILE_SIZE_MB,
    SNAPSHOT_RETAIN_MIN,
    SNAPSHOT_MAX_AGE_HOURS,
)


def get_catalog(bucket_arn: str):
    return load_catalog(
        "s3tables_005",
        **{
            "type": "rest",
            "warehouse": bucket_arn,
            "uri": f"https://s3tables.{REGION}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": REGION,
        },
    )


def ensure_namespace(catalog):
    existing = [ns[0] for ns in catalog.list_namespaces()]
    if NAMESPACE in existing:
        print(f"[ok] Namespace '{NAMESPACE}' already exists")
    else:
        print(f"[create] Creating namespace '{NAMESPACE}'...")
        catalog.create_namespace(NAMESPACE)
        print(f"[ok] Created namespace '{NAMESPACE}'")


def ensure_table(catalog, name, schema, partition_spec=None, properties=None):
    identifier = (NAMESPACE, name)
    if catalog.table_exists(identifier):
        print(f"[ok] Table '{NAMESPACE}.{name}' already exists")
        return catalog.load_table(identifier)

    print(f"[create] Creating table '{NAMESPACE}.{name}'...")
    kwargs = {"identifier": identifier, "schema": schema}
    if partition_spec is not None:
        kwargs["partition_spec"] = partition_spec
    if properties is not None:
        kwargs["properties"] = properties
    table = catalog.create_table(**kwargs)
    print(f"[ok] Created table '{NAMESPACE}.{name}'")
    return table


def configure_maintenance(bucket_arn: str, tables: list[str]):
    """Match S3 Tables' managed maintenance to the Glue optimizer config so the
    comparison is fair: same compaction target file size, same snapshot retention."""
    s3t = boto3.client("s3tables", region_name=REGION)
    for name in tables:
        s3t.put_table_maintenance_configuration(
            tableBucketARN=bucket_arn,
            namespace=NAMESPACE,
            name=name,
            type="icebergCompaction",
            value={
                "status": "enabled",
                "settings": {"icebergCompaction": {"targetFileSizeMB": TARGET_FILE_SIZE_MB}},
            },
        )
        s3t.put_table_maintenance_configuration(
            tableBucketARN=bucket_arn,
            namespace=NAMESPACE,
            name=name,
            type="icebergSnapshotManagement",
            value={
                "status": "enabled",
                "settings": {
                    "icebergSnapshotManagement": {
                        "minSnapshotsToKeep": SNAPSHOT_RETAIN_MIN,
                        "maxSnapshotAgeHours": SNAPSHOT_MAX_AGE_HOURS,
                    }
                },
            },
        )
        print(f"[ok] S3 Tables maintenance configured for {NAMESPACE}.{name} "
              f"(compaction {TARGET_FILE_SIZE_MB}MB, snapshots keep {SNAPSHOT_RETAIN_MIN}/{SNAPSHOT_MAX_AGE_HOURS}h)")


def main():
    bucket_arn = get_s3tables_bucket_arn()
    print(f"Region: {REGION}")
    print(f"Table bucket: {bucket_arn}")
    catalog = get_catalog(bucket_arn)
    ensure_namespace(catalog)

    ensure_table(catalog, "order_events", ORDER_EVENTS_SCHEMA, ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    # Second cadence: same trades, written hourly (fewer, larger files).
    ensure_table(catalog, "order_events_hourly", ORDER_EVENTS_SCHEMA, ORDER_EVENTS_PARTITION_SPEC, properties=TABLE_PROPERTIES)
    ensure_table(catalog, "orders_current", ORDERS_CURRENT_SCHEMA, properties=TABLE_PROPERTIES)
    ensure_table(catalog, "dim_products", DIM_PRODUCTS_SCHEMA)

    configure_maintenance(bucket_arn, ["order_events", "order_events_hourly", "orders_current"])

    print("\n--- Summary ---")
    print(f"Namespace: {NAMESPACE}")
    print("Tables: order_events, order_events_hourly, orders_current, dim_products")


if __name__ == "__main__":
    main()
