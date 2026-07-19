"""create a partitioned Iceberg table and append four orders."""

import datetime as dt

import pyarrow as pa
from catalog import CATALOG_CONFIG
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType

catalog = RestCatalog("local", **CATALOG_CONFIG)
catalog.create_namespace_if_not_exists("retail")

schema = Schema(
    NestedField(1, "order_id", LongType(), required=False),
    NestedField(2, "customer", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
    NestedField(4, "order_date", DateType(), required=False),
)
spec = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="order_date")
)
table = catalog.create_table_if_not_exists("retail.orders", schema=schema, partition_spec=spec)

rows = pa.Table.from_pydict(
    {
        "order_id": [1, 2, 3, 4],
        "customer": ["Alice", "Bob", "Carol", "Dan"],
        "amount": [120.50, 75.00, 240.75, 15.25],
        "order_date": [
            dt.date(2026, 1, 5),
            dt.date(2026, 1, 5),
            dt.date(2026, 1, 6),
            dt.date(2026, 1, 7),
        ],
    },
    schema=table.schema().as_arrow(),  # match Iceberg's field IDs and types
)
table.append(rows)  # commits the first snapshot

print("wrote", table.scan().to_arrow().num_rows, "rows")
print("first snapshot:", table.metadata.snapshots[0].snapshot_id)
