"""
Shared Iceberg schemas for the 005 "iceberg cost crossover" experiment tables.
Used by both create_tables.py (self-managed, Glue Catalog) and
create_s3tables.py (managed baseline, S3 Tables) so both sides compare
apples-to-apples on identical table shapes.
"""
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

# order_events: streaming append, real Coinbase trades. High cadence, tiny
# files -> the compaction stress table. Partitioned by day(time) so
# partition-level compaction triggers (Glue's >100-files rule) are visible.
ORDER_EVENTS_SCHEMA = Schema(
    NestedField(field_id=1, name="product_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="trade_id", field_type=LongType(), required=False),
    NestedField(field_id=3, name="side", field_type=StringType(), required=False),
    NestedField(field_id=4, name="size", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="price", field_type=DoubleType(), required=False),
    NestedField(field_id=6, name="time", field_type=TimestampType(), required=False),
    NestedField(field_id=7, name="fetched_at", field_type=TimestampType(), required=False),
)
ORDER_EVENTS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="event_date")
)

# orders_current: CDC upsert (MERGE INTO), one row per product_id holding its
# latest trade. Generates delete files + snapshot churn -> maintenance-hygiene
# stress table. No partitioning: small, one row per trading pair.
ORDERS_CURRENT_SCHEMA = Schema(
    NestedField(field_id=1, name="product_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="last_trade_id", field_type=LongType(), required=False),
    NestedField(field_id=3, name="side", field_type=StringType(), required=False),
    NestedField(field_id=4, name="size", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="price", field_type=DoubleType(), required=False),
    NestedField(field_id=6, name="trade_time", field_type=TimestampType(), required=False),
    NestedField(field_id=7, name="updated_at", field_type=TimestampType(), required=False),
)

# dim_products: slow-changing dimension, seeded from the real /products
# endpoint. Field set mirrors the raw API response, verified 2026-08-02.
DIM_PRODUCTS_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="base_currency", field_type=StringType(), required=False),
    NestedField(field_id=3, name="quote_currency", field_type=StringType(), required=False),
    NestedField(field_id=4, name="quote_increment", field_type=StringType(), required=False),
    NestedField(field_id=5, name="base_increment", field_type=StringType(), required=False),
    NestedField(field_id=6, name="display_name", field_type=StringType(), required=False),
    NestedField(field_id=7, name="min_market_funds", field_type=StringType(), required=False),
    NestedField(field_id=8, name="margin_enabled", field_type=BooleanType(), required=False),
    NestedField(field_id=9, name="post_only", field_type=BooleanType(), required=False),
    NestedField(field_id=10, name="limit_only", field_type=BooleanType(), required=False),
    NestedField(field_id=11, name="cancel_only", field_type=BooleanType(), required=False),
    NestedField(field_id=12, name="status", field_type=StringType(), required=False),
    NestedField(field_id=13, name="status_message", field_type=StringType(), required=False),
    NestedField(field_id=14, name="trading_disabled", field_type=BooleanType(), required=False),
    NestedField(field_id=15, name="fx_stablecoin", field_type=BooleanType(), required=False),
    NestedField(field_id=16, name="max_slippage_percentage", field_type=StringType(), required=False),
    NestedField(field_id=17, name="auction_mode", field_type=BooleanType(), required=False),
    NestedField(field_id=18, name="high_bid_limit_percentage", field_type=StringType(), required=False),
)

# Compaction target file size, applied identically to the order_events and
# orders_current tables on BOTH the Glue and S3 Tables sides so the managed-vs-
# semi-managed comparison is apples-to-apples. 64 MB is the S3 Tables minimum,
# matchable on Glue via write.target-file-size-bytes.
TARGET_FILE_SIZE_MB = 64
TABLE_PROPERTIES = {
    "write.target-file-size-bytes": str(TARGET_FILE_SIZE_MB * 1024 * 1024),
}
# Snapshot retention applied to both sides so snapshot expiry actually fires
# within the run window (keep the newest snapshot, expire anything older than 1
# day). Glue uses days; S3 Tables uses hours (24) — same intent, matched.
SNAPSHOT_RETAIN_MIN = 1
SNAPSHOT_MAX_AGE_HOURS = 24

