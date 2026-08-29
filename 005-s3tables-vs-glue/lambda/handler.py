"""
The whole producer, in one file: every 5 minutes an EventBridge schedule runs
this Lambda, which does three things in order:

  1. poll the Coinbase Exchange public API for new trades (no auth, no key),
  2. write one micro-batch into BOTH catalogs (append to order_events, MERGE
     into orders_current) so the self-managed and managed sides stay identical,
  3. record a metrics snapshot of both sides (snapshot/file counts, Glue
     optimizer DPU-hours, S3 Tables job status) to the ops bucket.

Dedupe uses a per-catalog high-water mark kept as one small S3 object, so a
one-sided failure retries cleanly without double-writing or dropping a batch.
Reserved concurrency is 1 (set at deploy), so two ticks never race on it.
"""
import json
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import requests
from pyiceberg.catalog import load_catalog

from config import (
    GLUE_DATABASE as DATABASE,
    HIGH_WATER_KEY,
    METRICS_PREFIX,
    OPS_BUCKET_NAME,
    PAIRS,
    REGION,
    WAREHOUSE,
    get_s3tables_bucket_arn,
)

API_URL = "https://api.exchange.coinbase.com/products/{product_id}/trades"
HEADERS = {"User-Agent": "iceberg-maint-eval-ingest"}
TABLES = ["order_events", "order_events_hourly", "orders_current"]
GLUE_OPT_TYPES = ["compaction", "retention", "orphan_file_deletion"]

ORDER_EVENTS_SCHEMA = pa.schema([
    ("product_id", pa.string()), ("trade_id", pa.int64()), ("side", pa.string()),
    ("size", pa.float64()), ("price", pa.float64()),
    ("time", pa.timestamp("us")), ("fetched_at", pa.timestamp("us")),
])
ORDERS_CURRENT_SCHEMA = pa.schema([
    ("product_id", pa.string()), ("last_trade_id", pa.int64()), ("side", pa.string()),
    ("size", pa.float64()), ("price", pa.float64()),
    ("trade_time", pa.timestamp("us")), ("updated_at", pa.timestamp("us")),
])


# --- catalogs -----------------------------------------------------------------

def glue_catalog():
    return load_catalog("glue", type="glue", warehouse=WAREHOUSE,
                        **{"glue.region": REGION, "s3.region": REGION})


def s3tables_catalog():
    props = {
        "type": "rest", "warehouse": get_s3tables_bucket_arn(),
        "uri": f"https://s3tables.{REGION}.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true", "rest.signing-name": "s3tables",
        "rest.signing-region": REGION,
    }
    ca = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")
    if ca:
        props["ssl.ca-bundle"] = ca
    return load_catalog("s3tables", **props)


CATALOGS = {"glue": glue_catalog, "s3tables": s3tables_catalog}


# --- high-water state (one small S3 object) -----------------------------------

def _s3():
    return boto3.client("s3", region_name=REGION)


def load_high_water() -> dict:
    """{"glue": {pair: last_trade_id}, "s3tables": {...}} — per catalog so a
    write failure on one side never advances the other's mark."""
    try:
        body = _s3().get_object(Bucket=OPS_BUCKET_NAME, Key=HIGH_WATER_KEY)["Body"].read()
        state = json.loads(body)
    except Exception as exc:
        if "NoSuchKey" not in str(exc) and "404" not in str(exc):
            raise
        state = {}
    for ck in CATALOGS:
        state.setdefault(ck, {})
        for pair in PAIRS:
            state[ck].setdefault(pair, 0)
    return state


def save_high_water(state: dict):
    _s3().put_object(Bucket=OPS_BUCKET_NAME, Key=HIGH_WATER_KEY,
                     Body=json.dumps(state).encode(), ContentType="application/json")


# --- ingest -------------------------------------------------------------------

def fetch_trades(product_id: str, limit: int = 100) -> list[dict]:
    resp = requests.get(API_URL.format(product_id=product_id),
                        params={"limit": limit}, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def write_batch(catalog, events: list[dict], now: datetime):
    append_events(catalog, "order_events", events, now)
    upsert_current(catalog, events, now)


def append_events(catalog, table_name: str, events: list[dict], now: datetime):
    rows = [{
        "product_id": e["product_id"], "trade_id": int(e["trade_id"]), "side": e["side"],
        "size": float(e["size"]), "price": float(e["price"]),
        "time": _ts(e["time"]), "fetched_at": now,
    } for e in events]
    catalog.load_table((DATABASE, table_name)).append(
        pa.Table.from_pylist(rows, schema=ORDER_EVENTS_SCHEMA))


def upsert_current(catalog, events: list[dict], now: datetime):
    latest: dict[str, dict] = {}
    for e in events:
        tid = int(e["trade_id"])
        if e["product_id"] not in latest or tid > latest[e["product_id"]]["last_trade_id"]:
            latest[e["product_id"]] = {
                "product_id": e["product_id"], "last_trade_id": tid, "side": e["side"],
                "size": float(e["size"]), "price": float(e["price"]),
                "trade_time": _ts(e["time"]), "updated_at": now,
            }
    latest_tbl = pa.Table.from_pylist(list(latest.values()), schema=ORDERS_CURRENT_SCHEMA)
    catalog.load_table((DATABASE, "orders_current")).upsert(latest_tbl, join_cols=["product_id"])


# --- hourly table (second cadence: same trades, one big file per hour) ---------

def _hourly_buffer_key(ck: str) -> str:
    return f"staging/hourly/{ck}.json"


def load_hourly_buffer(ck: str) -> dict:
    try:
        body = _s3().get_object(Bucket=OPS_BUCKET_NAME, Key=_hourly_buffer_key(ck))["Body"].read()
        return json.loads(body)
    except Exception as exc:
        if "NoSuchKey" in str(exc) or "404" in str(exc):
            return {"hour_bucket": None, "events": []}
        raise


def save_hourly_buffer(ck: str, buf: dict):
    _s3().put_object(Bucket=OPS_BUCKET_NAME, Key=_hourly_buffer_key(ck),
                     Body=json.dumps(buf).encode(), ContentType="application/json")


def flush_hourly_if_due(catalog, ck: str, now: datetime):
    """Flush the previous hour's buffer to order_events_hourly in one batch when the
    UTC hour rolls over. Called every tick, even with no new trades, so the boundary
    still rolls. Same daily volume as order_events but far fewer, larger files."""
    buf = load_hourly_buffer(ck)
    current_hour = now.strftime("%Y-%m-%dT%H")
    if buf["hour_bucket"] == current_hour:
        return
    if buf["hour_bucket"] is not None and buf["events"]:
        append_events(catalog, "order_events_hourly", buf["events"], now)
        print(f"[{ck}] flushed {len(buf['events'])} events -> order_events_hourly (hour {buf['hour_bucket']})")
    save_hourly_buffer(ck, {"hour_bucket": current_hour, "events": []})


def buffer_hourly(ck: str, events: list[dict]):
    """Buffer the SAME events just written to order_events, so the hourly table stays
    record-for-record identical to the 5-min table. Called only after the 5-min write
    and high-water advance succeed, so a failed/retried tick never double-buffers."""
    if not events:
        return
    buf = load_hourly_buffer(ck)
    buf["events"].extend(events)
    save_hourly_buffer(ck, buf)


def ingest(now: datetime) -> dict:
    trades = {p: fetch_trades(p) for p in PAIRS}
    state = load_high_water()
    result = {}
    for ck, loader in CATALOGS.items():
        hw = state[ck]
        new_hw, events = dict(hw), []
        for p in PAIRS:
            fresh = [t for t in trades[p] if int(t["trade_id"]) > hw[p]]
            if fresh:
                new_hw[p] = max(int(t["trade_id"]) for t in fresh)
                events += [{**t, "product_id": p} for t in fresh]
        catalog = loader()
        # Roll the hourly boundary first, even on ticks with no new events.
        flush_hourly_if_due(catalog, ck, now)
        if not events:
            result[ck] = 0
            continue
        append_events(catalog, "order_events", events, now)
        upsert_current(catalog, events, now)
        state[ck] = new_hw
        save_high_water(state)  # persist per catalog so a later side's failure can't roll this back
        buffer_hourly(ck, events)  # only after the 5-min write succeeded -> tables stay in sync
        result[ck] = len(events)
        print(f"[{ck}] wrote {len(events)} events")
    return result


# --- metrics ------------------------------------------------------------------

def _table_metrics(tbl) -> dict:
    cur = tbl.current_snapshot()
    s = dict(getattr(cur.summary, "additional_properties", {}) or {}) if cur else {}
    df = int(s.get("total-data-files", 0) or 0)
    size = int(s.get("total-files-size", 0) or 0)
    return {
        "n_snapshots": len(tbl.metadata.snapshots),
        "data_files": df, "delete_files": int(s.get("total-delete-files", 0) or 0),
        "total_files_size": size, "avg_data_file_bytes": round(size / df) if df else 0,
        "records": int(s.get("total-records", 0) or 0),
    }


def _dpu_hours(run: dict) -> float:
    """A Glue optimizer run reports NumberOfDpus + JobDurationInHour (nested under a
    per-job-type metrics block). DPU-hours = dpus x hours; that x $0.44 is the real
    Glue maintenance $. Search nested dicts so it works for compaction/retention/orphan."""
    dpus = hours = 0.0

    def walk(obj):
        nonlocal dpus, hours
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "NumberOfDpus":
                    dpus = float(v)
                elif k == "JobDurationInHour":
                    hours = float(v)
                else:
                    walk(v)

    walk(run)
    return round(dpus * hours, 6)


def _run_scalar(run: dict, field: str) -> float:
    """Largest value of `field` anywhere in one run dict. The optimizer reports the
    same figure twice (top-level `metrics` + per-job `IcebergMetrics`); taking the max
    over a single run avoids double-counting before we sum across runs."""
    best = 0.0

    def walk(obj):
        nonlocal best
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == field:
                    try:
                        best = max(best, float(v))
                    except (TypeError, ValueError):
                        pass
                else:
                    walk(v)
        elif isinstance(obj, list):
            for i in obj:
                walk(i)

    walk(run)
    return best


def _s3_prefix_bytes(bucket: str, prefix: str) -> tuple[int, int]:
    """Real bytes + object count under an S3 prefix — the actual storage bill, which
    includes un-expired snapshots and orphan files the snapshot summary doesn't count."""
    s3 = _s3()
    total = objects = 0
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            total += obj["Size"]
            objects += 1
        if not resp.get("IsTruncated"):
            return total, objects
        token = resp["NextContinuationToken"]



def capture_metrics(now: str):
    records = []
    for ck, loader in CATALOGS.items():
        try:
            catalog = loader()
            for name in TABLES:
                records.append({"ts": now, "type": "table", "catalog": ck, "table": name,
                                **_table_metrics(catalog.load_table((DATABASE, name)))})
        except Exception as exc:
            records.append({"ts": now, "type": "table", "catalog": ck, "error": str(exc)})

    try:
        glue = boto3.client("glue", region_name=REGION)
        acct = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
        for name in TABLES:
            for t in GLUE_OPT_TYPES:
                runs = glue.list_table_optimizer_runs(
                    CatalogId=acct, DatabaseName=DATABASE, TableName=name, Type=t
                ).get("TableOptimizerRuns", [])
                dpu_hours = round(sum(_dpu_hours(r) for r in runs), 6)
                files_compacted = int(sum(_run_scalar(r, "NumberOfFilesCompacted") for r in runs))
                bytes_compacted = int(sum(_run_scalar(r, "NumberOfBytesCompacted") for r in runs))
                files_deleted = int(sum(
                    _run_scalar(r, "NumberOfDataFilesDeleted")
                    + _run_scalar(r, "NumberOfManifestFilesDeleted")
                    + _run_scalar(r, "NumberOfManifestListsDeleted")
                    + _run_scalar(r, "NumberOfOrphanFilesDeleted")
                    for r in runs))
                records.append({"ts": now, "type": "glue_run", "table": name, "opt_type": t,
                                "run_count": len(runs), "dpu_hours": dpu_hours,
                                "est_cost_usd": round(dpu_hours * 0.44, 6),
                                "files_compacted": files_compacted, "bytes_compacted": bytes_compacted,
                                "files_deleted": files_deleted,
                                "latest": runs[-1] if runs else None})
    except Exception as exc:
        records.append({"ts": now, "type": "glue_run", "error": str(exc)})

    # Storage bill (self-managed side): real bytes under each table's warehouse prefix,
    # including un-expired snapshots + orphans the snapshot summary omits. S3 Tables
    # storage lives in a managed bucket that isn't directly listable, so we fall back to
    # its snapshot-summary total (captured above as total_files_size) for that side.
    try:
        warehouse_prefix = WAREHOUSE.split("/", 3)[-1]  # strip s3://bucket/
        bucket = WAREHOUSE.split("/")[2]
        for name in TABLES:
            total, objects = _s3_prefix_bytes(bucket, f"{warehouse_prefix}/{DATABASE}.db/{name}/")
            records.append({"ts": now, "type": "storage", "catalog": "glue", "table": name,
                            "storage_bytes": total, "object_count": objects})
    except Exception as exc:
        records.append({"ts": now, "type": "storage", "catalog": "glue", "error": str(exc)})


    try:
        s3t = boto3.client("s3tables", region_name=REGION)
        arn = get_s3tables_bucket_arn()
        for name in TABLES:
            r = s3t.get_table_maintenance_job_status(tableBucketARN=arn, namespace=DATABASE, name=name)
            records.append({"ts": now, "type": "s3t_status", "table": name, "status": r.get("status")})
    except Exception as exc:
        records.append({"ts": now, "type": "s3t_status", "error": str(exc)})

    dt = _ts(now)
    key = f"{METRICS_PREFIX}{dt:%Y/%m/%d/%H%M%S}.ndjson"
    _s3().put_object(Bucket=OPS_BUCKET_NAME, Key=key,
                     Body="\n".join(json.dumps(r, default=str) for r in records).encode(),
                     ContentType="application/x-ndjson")
    print(f"[metrics] wrote {len(records)} records -> {key}")


# --- entrypoint ---------------------------------------------------------------

def handler(event, context):
    now = datetime.now(timezone.utc)
    written = ingest(now)
    capture_metrics(now.isoformat())
    return {"written": written}
