# Shared connection config for both the writer (pyiceberg) and reader (DuckDB).
# Points at SeaweedFS: Iceberg REST catalog on 8181, S3 API on 8333, anonymous.
CATALOG_CONFIG = {
    "uri": "http://localhost:8181",          # Iceberg REST catalog
    "warehouse": "s3://warehouse",
    "s3.endpoint": "http://localhost:8333",  # SeaweedFS S3 API
    "s3.region": "us-east-1",
    "s3.anonymous": "true",                  # send unsigned requests (allow-all mode)
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
}
