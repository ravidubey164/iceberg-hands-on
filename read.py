"""
Query the current table with DuckDB, then time-travel back to the first snapshot. 
DuckDB never wrote this table; it just reads the same Iceberg REST catalog pyiceberg wrote to.
"""

import duckdb
from catalog import CATALOG_CONFIG
from pyiceberg.catalog.rest import RestCatalog

# grab the first snapshot id from the catalog (for the time-travel query)
first_snap = (
    RestCatalog("local", **CATALOG_CONFIG)
    .load_table("retail.orders")
    .metadata.snapshots[0]
    .snapshot_id
)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")

# SeaweedFS S3 API, path-style, no credentials => unsigned/anonymous
con.execute("SET s3_endpoint='localhost:8333';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_region='us-east-1';")

# attach the SeaweedFS Iceberg REST catalog (no auth in allow-all mode)
con.execute("""
ATTACH 'warehouse' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'
);
""")

print("current rows:")
con.sql("SELECT * FROM lake.retail.orders ORDER BY order_id").show()

print(f"time travel to first snapshot {first_snap}:")
con.sql(
    f"SELECT * FROM lake.retail.orders AT (VERSION => {first_snap}) ORDER BY order_id"
).show()
