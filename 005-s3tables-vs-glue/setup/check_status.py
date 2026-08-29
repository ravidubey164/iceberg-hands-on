"""
Status report for the running experiment: table record counts/sizes, runs vs
failures, maintenance activity, and cost since the restart baseline. Run any
time via `make status`. Prints Markdown to stdout and also saves it to
STATUS.md so it can be opened in a Markdown preview or pasted into the post.
All timestamps are converted to MYT (UTC+8).
"""
import json
import math
import sys
from datetime import datetime, timedelta, timezone

import boto3

# config.py/schemas.py live one level up at the experiments root.
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import METRICS_PREFIX, OPS_BUCKET_NAME, REGION
from schemas import SNAPSHOT_MAX_AGE_HOURS, SNAPSHOT_RETAIN_MIN, TARGET_FILE_SIZE_MB

MYT = timezone(timedelta(hours=8))
FUNCTION = "iceberg-maint-eval-ingest"
RULE = "iceberg-maint-eval-ingest-5m"
TABLES = ["order_events", "order_events_hourly", "orders_current"]
REPORT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "STATUS.md")


def to_myt(dt: datetime) -> str:
    return dt.astimezone(MYT).strftime("%Y-%m-%d %H:%M:%S MYT")


def human_bytes(n: int) -> str:
    size = float(n)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{n} B"
        size /= 1024
    return f"{size:.1f} TB"


def format_s3t_status(status: dict) -> str:
    """status is {'icebergCompaction': {'status': 'Not_Yet_Run'}, ...} -> short "key: value" list."""
    parts = []
    for job, detail in status.items():
        short = job.replace("iceberg", "")
        parts.append(f"{short}={detail.get('status', '?')}")
    return ", ".join(parts)


# --- data gathering -------------------------------------------------------

def get_restart_ts() -> datetime:
    s3 = boto3.client("s3", region_name=REGION)
    body = s3.get_object(Bucket=OPS_BUCKET_NAME, Key="meta/restart_timestamp.txt")["Body"].read()
    return datetime.fromisoformat(body.decode().strip().replace("Z", "+00:00"))


def get_lambda_and_schedule():
    lam = boto3.client("lambda", region_name=REGION)
    cfg = lam.get_function(FunctionName=FUNCTION)["Configuration"]
    rule = boto3.client("events", region_name=REGION).describe_rule(Name=RULE)
    return cfg, rule


def get_run_stats(restart_dt: datetime):
    """Invocations/errors since restart, straight from Lambda's own CloudWatch metrics."""
    cw = boto3.client("cloudwatch", region_name=REGION)
    now = datetime.now(timezone.utc)

    # CloudWatch caps a single GetMetricStatistics call at 1440 datapoints. A fixed
    # 300s period only covers ~5 days before that limit hits, so widen the period as
    # the run gets longer (rounded up to a whole minute, CloudWatch's granularity).
    elapsed_seconds = (now - restart_dt).total_seconds()
    period = max(300, math.ceil(elapsed_seconds / 1400 / 60) * 60)

    def total(metric):
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName=metric,
            Dimensions=[{"Name": "FunctionName", "Value": FUNCTION}],
            StartTime=restart_dt, EndTime=now, Period=period, Statistics=["Sum"],
        )
        return int(sum(dp["Sum"] for dp in resp["Datapoints"]))

    return total("Invocations"), total("Errors")


def list_metrics_snapshots(s3):
    """All metrics ndjson keys, sorted chronologically (names are zero-padded dates)."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=OPS_BUCKET_NAME, Prefix=METRICS_PREFIX):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return sorted(keys)


def load_ndjson(s3, key):
    body = s3.get_object(Bucket=OPS_BUCKET_NAME, Key=key)["Body"].read().decode()
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def index_records(records, type_, key_fields):
    return {
        tuple(r.get(k) for k in key_fields): r
        for r in records
        if r.get("type") == type_ and "error" not in r
    }


def sample_keys(keys, max_n=48):
    """Evenly-spaced subset (always incl. first + last) so the trend stays cheap
    to load even after thousands of 5-minute snapshots pile up."""
    if len(keys) <= max_n:
        return keys
    step = len(keys) / max_n
    idx = sorted({int(i * step) for i in range(max_n)} | {len(keys) - 1})
    return [keys[i] for i in idx]


def build_trend_section(s3, keys) -> list:
    lines = ["## Snapshot & file-count trend", ""]
    if len(keys) < 2:
        lines += ["*(not enough history yet)*", ""]
        return lines
    series = {}  # (catalog, table) -> list of (n_snapshots, data_files)
    for k in sample_keys(keys):
        for r in load_ndjson(s3, k):
            if r.get("type") == "table" and "error" not in r:
                series.setdefault((r["catalog"], r["table"]), []).append(
                    (r.get("n_snapshots", 0), r.get("data_files", 0))
                )
    lines.append("Sampled across the run (start → peak → now). Retention should *bound* snapshots;")
    lines.append("compaction should keep data files from running away.")
    lines.append("")
    lines.append("| Catalog | Table | Snapshots (start/peak/now) | Data files (start/peak/now) |")
    lines.append("|---|---|---|---|")
    for catalog in ["glue", "s3tables"]:
        for table in TABLES:
            sv = series.get((catalog, table))
            if not sv:
                continue
            snaps = [x[0] for x in sv]
            files = [x[1] for x in sv]
            lines.append(
                f"| {catalog} | {table} | {snaps[0]}/{max(snaps)}/{snaps[-1]} | "
                f"{files[0]}/{max(files)}/{files[-1]} |"
            )
    lines.append("")
    for table in TABLES:
        g = series.get(("glue", table))
        st = series.get(("s3tables", table))
        if g and st and g[-1][0] and st[-1][0]:
            gs, ss = g[-1][0], st[-1][0]
            if abs(gs - ss) <= max(3, 0.05 * max(gs, ss)):
                lines.append(
                    f"- `{table}`: snapshot counts track each other (glue {gs} vs s3tables {ss}) — "
                    "managed snapshot management isn't reaping faster than Glue retention here; both gate on max-age."
                )
            else:
                lines.append(
                    f"- `{table}`: snapshot counts diverge (glue {gs} vs s3tables {ss}) — one side expires faster."
                )
    lines.append("")
    return lines


# --- report -----------------------------------------------------------------

def build_report() -> str:
    s3 = boto3.client("s3", region_name=REGION)
    restart_dt = get_restart_ts()
    now = datetime.now(timezone.utc)
    elapsed = now - restart_dt

    cfg, rule = get_lambda_and_schedule()
    invocations, errors = get_run_stats(restart_dt)
    keys = list_metrics_snapshots(s3)

    latest = load_ndjson(s3, keys[-1]) if keys else []
    prev = load_ndjson(s3, keys[-2]) if len(keys) > 1 else []
    latest_tables = index_records(latest, "table", ["catalog", "table"])
    prev_tables = index_records(prev, "table", ["catalog", "table"])
    latest_glue_runs = index_records(latest, "glue_run", ["table", "opt_type"])
    latest_s3t_status = index_records(latest, "s3t_status", ["table"])
    latest_storage = index_records(latest, "storage", ["catalog", "table"])

    lines = []
    lines.append("# 005 iceberg-maint-eval — Status Report")
    lines.append("")
    lines.append(f"Generated: {to_myt(now)}  ")
    lines.append(f"Restart baseline: {to_myt(restart_dt)}  ")
    lines.append(f"Elapsed since restart: {elapsed.days}d {elapsed.seconds // 3600}h {(elapsed.seconds // 60) % 60}m")
    lines.append("")

    lines.append("## Lambda & schedule")
    lines.append("")
    lines.append(f"- Function `{FUNCTION}`: state=**{cfg['State']}**, last update=**{cfg['LastUpdateStatus']}**")
    lines.append(f"- Schedule `{rule['Name']}`: state=**{rule['State']}**, expr=`{rule['ScheduleExpression']}`")
    lines.append(f"- Invocations since restart: **{invocations}**")
    fail_rate = f"{(errors / invocations * 100):.1f}%" if invocations else "n/a"
    lines.append(f"- Failures since restart: **{errors}** ({fail_rate} failure rate)")
    lines.append(f"- Metrics snapshots recorded: **{len(keys)}**")
    lines.append("")

    lines.append("## Table stats")
    lines.append("")
    lines.append("| Catalog | Table | Records | + Last Run | Data Files | Delete Files | Avg File Size | Total Size | Snapshots |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for catalog in ["glue", "s3tables"]:
        for table in TABLES:
            cur = latest_tables.get((catalog, table))
            if not cur:
                lines.append(f"| {catalog} | {table} | (no data yet) | | | | | | |")
                continue
            prv = prev_tables.get((catalog, table))
            delta = cur["records"] - prv["records"] if prv else None
            delta_str = f"+{delta}" if delta is not None else "n/a"
            lines.append(
                f"| {catalog} | {table} | {cur['records']:,} | {delta_str} | "
                f"{cur['data_files']:,} | {cur['delete_files']:,} | "
                f"{human_bytes(cur['avg_data_file_bytes'])} | {human_bytes(cur['total_files_size'])} | "
                f"{cur['n_snapshots']} |"
            )
    lines.append("")

    lines.append("## Maintenance activity (self-managed side, Glue optimizer)")
    lines.append("")
    lines.append("| Table | Optimizer | Runs so far | DPU-hours | Est. cost | Work done |")
    lines.append("|---|---|---|---|---|---|")
    total_dpu = 0.0
    oe_bytes_compacted = 0
    for table in TABLES:
        for opt_type in ["compaction", "retention", "orphan_file_deletion"]:
            r = latest_glue_runs.get((table, opt_type))
            dpu = r.get("dpu_hours", 0.0) if r else 0.0
            cost = r.get("est_cost_usd", 0.0) if r else 0.0
            total_dpu += dpu
            if opt_type == "compaction":
                fc = r.get("files_compacted", 0) if r else 0
                bc = r.get("bytes_compacted", 0) if r else 0
                if table == "order_events":
                    oe_bytes_compacted = bc
                work = f"{fc:,} files → {human_bytes(bc)} rewritten" if fc else "—"
            else:
                fd = r.get("files_deleted", 0) if r else 0
                work = f"{fd:,} files deleted" if fd else "nothing deleted yet"
            lines.append(f"| {table} | {opt_type} | {r['run_count'] if r else 0} | {dpu:.4f} | ${cost:.4f} | {work} |")
    lines.append(f"| **total** | | | **{total_dpu:.4f}** | **${total_dpu * 0.44:.4f}** | |")
    lines.append("")
    lines.append(
        "> These DPU-hours are summed from the optimizer's own run history (a rolling window that "
        "can reach back before the restart baseline), so this total runs a bit high. The authoritative "
        "since-restart maintenance cost is the `EUN1-Optimization-DPU-Hour` line in the cost section below."
    )
    lines.append("")
    oe_live = latest_tables.get(("glue", "order_events"))
    if oe_bytes_compacted and oe_live:
        ratio = oe_bytes_compacted / oe_live["total_files_size"] if oe_live["total_files_size"] else 0
        lines.append(
            f"- **Write amplification:** compaction has rewritten {human_bytes(oe_bytes_compacted)} on "
            f"`order_events` so far, against a current live size of {human_bytes(oe_live['total_files_size'])} "
            f"(~{ratio:.1f}x). Every time a partition re-crosses the file threshold you pay to rewrite the "
            "same logical data again — the hidden cost of aggressive compaction."
        )
        lines.append("")

    lines.append("## Storage (real S3 bytes, self-managed side)")
    lines.append("")
    lines.append("Actual bytes under each table's warehouse prefix — includes un-expired snapshots")
    lines.append("and orphan files the snapshot summary omits, so it's the true storage bill.")
    lines.append("")
    lines.append("| Table | Storage | Objects |")
    lines.append("|---|---|---|")
    for table in TABLES:
        r = latest_storage.get(("glue", table))
        if r:
            lines.append(f"| {table} | {human_bytes(r['storage_bytes'])} | {r['object_count']:,} |")
        else:
            lines.append(f"| {table} | (no data yet) | |")
    lines.append("")

    lines.append("## Managed side (S3 Tables maintenance status)")
    lines.append("")
    lines.append("| Table | Status |")
    lines.append("|---|---|")
    for table in TABLES:
        r = latest_s3t_status.get((table,))
        lines.append(f"| {table} | {format_s3t_status(r['status']) if r else 'unknown'} |")
    lines.append("")

    lines += build_trend_section(s3, keys)
    lines += build_cost_section(restart_dt, now, elapsed)
    lines += build_insights(latest_tables, invocations, errors)

    return "\n".join(lines)


def classify_usage_type(ut: str):
    """Map a Cost Explorer usage-type string to (side, bucket) for the head-to-head.

    side in {selfmanaged, s3tables, shared}. S3 Tables usage types carry a
    'Tables-' infix (checked first); general S3 / Glue optimizer are the
    self-managed side; everything else (Lambda, Config, KMS, CE API...) is shared.
    """
    if "Tables-CompactedObjects" in ut or "Tables-MonitoredObjects" in ut:
        return ("s3tables", "maintenance")
    if "Tables-TimedStorage" in ut:
        return ("s3tables", "storage")
    if "Tables-Requests" in ut or "Tables-ProcessedBytes" in ut:
        return ("s3tables", "requests")
    if "Optimization-DPU-Hour" in ut:  # Glue optimizer = self-managed maintenance compute
        return ("selfmanaged", "maintenance")
    if "Catalog-" in ut or ut.endswith("-Request"):
        return ("selfmanaged", "catalog")
    if "TimedStorage-ByteHrs" in ut:  # general S3 (Tables-* already handled above)
        return ("selfmanaged", "storage")
    if "Requests-Tier" in ut:  # general S3 (Tables-* already handled above)
        return ("selfmanaged", "requests")
    return ("shared", "infra")


def build_cost_section(restart_dt, now, elapsed) -> list:
    lines = ["## Cost since restart (by usage type)", ""]
    ce = boto3.client("ce", region_name="us-east-1")
    start_date = restart_dt.strftime("%Y-%m-%d")
    end_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        # One DAILY call, grouped by usage type, gives both the aggregate split and
        # the per-day trajectory — cheaper than two calls ($0.01 each).
        resp = ce.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="DAILY",
            Metrics=["UnblendedCost", "UsageQuantity"],
            GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
        )
    except Exception as exc:
        lines.append(f"*(cost query failed: {exc})*")
        lines.append("")
        return lines

    split = {"selfmanaged": {}, "s3tables": {}, "shared": {}}
    qty = {}     # usage_type -> summed UsageQuantity
    daily = {}   # date -> {selfmanaged, s3tables} maintenance+storage only
    total_cost = 0.0
    for day in resp["ResultsByTime"]:
        d = day["TimePeriod"]["Start"]
        daily.setdefault(d, {"selfmanaged": 0.0, "s3tables": 0.0})
        for g in day["Groups"]:
            ut = g["Keys"][0]
            cost = float(g["Metrics"]["UnblendedCost"]["Amount"])
            qty[ut] = qty.get(ut, 0.0) + float(g["Metrics"]["UsageQuantity"]["Amount"])
            total_cost += cost
            side, bucket = classify_usage_type(ut)
            split[side][bucket] = split[side].get(bucket, 0.0) + cost
            if side in ("selfmanaged", "s3tables") and bucket in ("maintenance", "storage"):
                daily[d][side] += cost

    def s(side, bucket):
        return split[side].get(bucket, 0.0)

    def q_of(sub):
        return sum(v for k, v in qty.items() if sub in k)

    self_maint, self_store = s("selfmanaged", "maintenance"), s("selfmanaged", "storage")
    self_req, self_cat = s("selfmanaged", "requests"), s("selfmanaged", "catalog")
    s3t_maint, s3t_store, s3t_req = s("s3tables", "maintenance"), s("s3tables", "storage"), s("s3tables", "requests")
    self_total = self_maint + self_store + self_req + self_cat
    s3t_total = s3t_maint + s3t_store + s3t_req

    lines.append("### Head-to-head: maintenance + storage + requests, by side")
    lines.append("")
    lines.append("| Line item | Self-managed (Glue) | S3 Tables (managed) |")
    lines.append("|---|---|---|")
    lines.append(f"| Maintenance compute | ${self_maint:.4f} | ${s3t_maint:.4f} |")
    lines.append(f"| Storage | ${self_store:.4f} | ${s3t_store:.4f} |")
    lines.append(f"| Requests | ${self_req:.4f}\\* | ${s3t_req:.4f} |")
    lines.append(f"| Catalog | ${self_cat:.4f} | included |")
    lines.append(f"| **Subtotal** | **${self_total:.4f}** | **${s3t_total:.4f}** |")
    lines.append("")
    lines.append(
        "\\* self-managed request count is partly inflated by this experiment's own per-tick "
        "bucket listing (the real-byte capture LISTs the warehouse every 5 min). Treat the "
        "maintenance + storage rows as the clean comparison; the managed side hides that "
        "listing behind its monitoring fee."
    )
    lines.append("")

    self_get = int(q_of("Requests-Tier2") - q_of("Tables-Requests-Tier2"))
    s3t_get = int(q_of("Tables-Requests-Tier2"))
    lines.append("### Per-object / per-request drivers")
    lines.append("")
    lines.append(f"- Glue optimizer DPU-hours billed: **{q_of('Optimization-DPU-Hour'):.4f}** (= ${self_maint:.4f})")
    lines.append(f"- S3 Tables monitored objects: **{q_of('Tables-MonitoredObjects'):.1f}** object-mo · compacted objects: **{int(q_of('Tables-CompactedObjects'))}** (together = ${s3t_maint:.4f})")
    lines.append(f"- GET (Tier2) requests: self-managed **{self_get:,}** vs S3 Tables **{s3t_get:,}** — the small-file read-amplification signal (self-managed count partly instrumentation)")
    lines.append("")

    lines.append("### Daily trajectory — maintenance + storage, by side (crossover chart data)")
    lines.append("")
    lines.append("| Date | Self-managed | S3 Tables |")
    lines.append("|---|---|---|")
    for d in sorted(daily):
        lines.append(f"| {d} | ${daily[d]['selfmanaged']:.4f} | ${daily[d]['s3tables']:.4f} |")
    lines.append("")

    shared_total = sum(split["shared"].values())
    lines.append("### Shared / non-comparison costs")
    lines.append("")
    lines.append(
        f"Producer Lambda, AWS Config, KMS, Cost Explorer API, CloudWatch, ECR, Secrets: "
        f"**${shared_total:.4f}**. Both architectures would incur most of this; the Cost Explorer "
        f"line is this report's own `make status` runs at $0.01 each."
    )
    lines.append("")

    elapsed_days = max(elapsed.total_seconds() / 86400, 1e-9)
    daily_rate = total_cost / elapsed_days
    lines.append(f"- Grand total so far (all services): **${total_cost:.4f}**")
    lines.append(f"- **Maintenance head-to-head: self-managed ${self_maint:.4f} vs S3 Tables ${s3t_maint:.4f}**")
    lines.append(f"- Run-rate estimate: **${daily_rate:.4f}/day** (~${daily_rate * 30:.2f}/month at this rate)")
    lines.append("- Caveat: Cost Explorer data lags several hours; run-rate is noisy until the window is longer than a day.")
    lines.append("")
    return lines


def build_insights(latest_tables, invocations, errors) -> list:
    lines = ["## Insights", ""]

    for table in TABLES:
        g = latest_tables.get(("glue", table))
        s = latest_tables.get(("s3tables", table))
        if g and s:
            diff = g["records"] - s["records"]
            if diff == 0:
                lines.append(f"- `{table}` is in sync across both catalogs ({g['records']:,} records each).")
            else:
                lines.append(f"- `{table}` **differs** between catalogs: glue={g['records']:,} vs s3tables={s['records']:,} (diff {diff:+,}) — check for a one-sided write failure.")

    for catalog in ["glue", "s3tables"]:
        oe = latest_tables.get((catalog, "order_events"))
        if oe and oe["avg_data_file_bytes"]:
            target_bytes = TARGET_FILE_SIZE_MB * 1024 * 1024
            pct = oe["avg_data_file_bytes"] / target_bytes * 100
            if pct < 10:
                lines.append(
                    f"- `{catalog}/order_events` avg data file is {human_bytes(oe['avg_data_file_bytes'])}, "
                    f"only {pct:.1f}% of the {TARGET_FILE_SIZE_MB}MB target — expected this early, "
                    "small-file buildup is exactly what the optimizer/maintenance job should be cleaning up over time."
                )

    if invocations:
        lines.append(f"- Failure rate so far: {errors}/{invocations} invocations ({errors / invocations * 100:.1f}%).")

    lines.append(
        f"- Retention policy: keep min {SNAPSHOT_RETAIN_MIN} snapshot(s), max age {SNAPSHOT_MAX_AGE_HOURS}h — "
        "watch the Snapshots column above over time to confirm it's actually bounding snapshot growth, not just accumulating."
    )
    lines.append("")
    return lines


if __name__ == "__main__":
    report = build_report()
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"Status report saved -> {REPORT_PATH}")
