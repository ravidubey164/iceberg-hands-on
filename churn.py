"""
Each operation commits a new snapshot, so the table ends with three:
append, append, overwrite.
"""

import datetime as dt

import pyarrow as pa
from catalog import CATALOG_CONFIG
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import EqualTo

table = RestCatalog("local", **CATALOG_CONFIG).load_table("retail.orders")

# a new order arrives -> a second snapshot
table.append(
    pa.Table.from_pydict(
        {
            "order_id": [5],
            "customer": ["Erin"],
            "amount": [999.99],
            "order_date": [dt.date(2026, 1, 8)],
        },
        schema=table.schema().as_arrow(),
    )
)

# Bob cancels -> a row-level delete, a third snapshot
table.delete(EqualTo("order_id", 2))

table.refresh()
print("snapshot_id\toperation\ttimestamp_ms")
for s in table.metadata.snapshots:
    print(f"{s.snapshot_id}\t{s.summary.operation.value}\t{s.timestamp_ms}")
