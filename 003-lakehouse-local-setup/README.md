# Local Iceberg lakehouse: SeaweedFS + pyiceberg + DuckDB

Companion code for part 3 of the Data Lakehouse series. One container (SeaweedFS,
which is both the S3 store and the Iceberg REST catalog), pyiceberg to write, and
DuckDB to read. No JVM, no Spark, no Trino.

Verified on SeaweedFS 4.39, pyiceberg 0.11.1, duckdb 1.5.4, s3fs 2026.6.0,
pyarrow 25.0.0.

For the Spark + Trino version of the same setup, see [`../iceberg-spark-trino`](../iceberg-spark-trino).

## Prerequisites

- Docker with Compose v2 (only SeaweedFS runs in Docker)
- Python 3.12+ and: `pip install "pyiceberg[pyarrow,s3fs]" duckdb`

## Run it

```bash
docker compose up -d          # start SeaweedFS (S3 on :8333, Iceberg REST on :8181)

python write.py               # Step 3: create the table + append 4 orders
python read.py                # Step 4: DuckDB reads the current table
python update_orders.py       # Step 5: a new order (append) + a delete (overwrite)
python read.py                # Step 5: current rows + time travel to the first snapshot

docker compose down -v        # stop and wipe
```

`read.py` prints the current rows and then the same query `AT (VERSION => <first_snapshot>)`,
so after `churn.py` you'll see the deleted order (Bob, #2) alive again in the past.

## Important Notes

- **`py-io-impl` = FsspecFileIO is required.** pyiceberg's default pyarrow FileIO
  uploads with HTTP chunked transfer encoding, and SeaweedFS's allow-all gateway
  stores that chunk framing inside the object. The first write "succeeds" and the
  next read dies in the Avro decoder (`Negative size passed to PyBytes_FromStringAndSize`).
  s3fs sends a plain `Content-Length` upload, so it round-trips cleanly. That's why
  `s3fs` is in the pip install.
- **Anonymous means unsigned.** SeaweedFS allow-all rejects *signed* requests, so
  pyiceberg uses `s3.anonymous=true` and DuckDB is given no S3 secret.
- **DuckDB's REST catalog defaults to OAuth2.** `ATTACH` fails with
  `AUTHORIZATION_TYPE is 'oauth2'` until you set `AUTHORIZATION_TYPE 'none'`.
