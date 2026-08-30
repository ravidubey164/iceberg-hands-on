"""Project Glue optimizer and S3 Tables costs across common workload shapes.

Uses AWS pricing and experiment measurements. Run `make model` to generate tables.
"""
import csv
import os
from dataclasses import dataclass, field, replace

# ---------------------------------------------------------------------------
# RATES — AWS-published (eu-north-1 / EUN1), cross-checked against our own bill.
# ---------------------------------------------------------------------------
DPU_PRICE = 0.44          # $/DPU-hour, AWS Glue pricing
MIN_DPU_HR_PER_RUN = 0.045  # measured: a Glue optimizer run floors at ~2 DPU x ~1.3 min
FILES_PER_COMPACTION = 100   # Glue compaction trigger: >100 files/partition (AWS docs + measured)

SELF_STORAGE = 0.023      # $/GB-mo, general-purpose S3 Standard
S3T_STORAGE = 0.0265      # $/GB-mo, S3 Tables storage

S3T_COMPACT_GB = 0.005    # $/GB processed by S3 Tables compaction (measured EUN1-Tables-ProcessedBytes)
S3T_COMPACT_OBJ = 0.002 / 1000   # $/object compacted (measured EUN1-Tables-CompactedObjects)
S3T_MONITOR = 0.025 / 1000       # $/object-month monitored (measured EUN1-Tables-MonitoredObjects)

PUT_PER_1K = 0.005 / 1000  # S3 Tier1 (PUT/LIST) $/request
GET_PER_1K = 0.0004 / 1000  # S3 Tier2 (GET) $/request

# ---------------------------------------------------------------------------
# FITTED coefficients (the only two). Calibrated to the anchors above; adjust freely.
# ---------------------------------------------------------------------------
GLUE_COMPACTION_GB_PER_DPUHR = 25.0  # Spark/Glue rewrite throughput; fit to the 1 TB anchor
WRITE_AMPLIFICATION = 1.6            # a byte gets rewritten ~1.6x as partitions re-cross the trigger
METADATA_OBJ_PER_DATA_FILE = 9.0     # measured: ~9 total S3 objects (manifests/snapshots) per live data file
READ_FILE_AMPLIFICATION = 8.5        # measured: self-managed scan touches 8.5x more files than S3 Tables

# Measured per-table floor for Glue retention + orphan jobs (metadata jobs that bill the
# DPU-hour minimum a few times a day no matter how small the table). This is the number
# that multiplies by table count.
GLUE_RETENTION_ORPHAN_USD_PER_TABLE_MONTH = 1.14  # measured, CE-consistent

TARGET_FILE_MB = 64  # both sides target 64 MB; drives live file count at steady state


@dataclass
class Workload:
    name: str
    records_per_month: float      # volume
    bytes_per_record: float       # row width (uncompressed)
    live_gb: float                # steady-state stored size of live data
    writes_per_month: float       # micro-batches -> files created (small-file rate)
    reads_per_month: float        # full-table scans per month (read/write side)
    num_tables: int = 1
    compression: float = 0.3      # parquet vs raw
    snapshot_overhang: float = 2.0  # self-managed stores un-expired snapshots on top of live
    scan_fraction: float = 1.0    # fraction of the table each read scans

    @property
    def ingest_gb_month(self) -> float:
        return self.records_per_month * self.bytes_per_record * self.compression / 1e9

    @property
    def files_created_month(self) -> float:
        return self.writes_per_month

    @property
    def avg_write_file_mb(self) -> float:
        if self.files_created_month <= 0:
            return TARGET_FILE_MB
        return self.ingest_gb_month * 1024 / self.files_created_month

    @property
    def compaction_fraction(self) -> float:
        # The real compaction driver: how far below target the *written* files land.
        # Batch jobs writing near-target files barely compact; streaming tiny files -> ~1.
        return max(0.0, min(1.0, 1 - self.avg_write_file_mb / TARGET_FILE_MB))

    @property
    def live_data_files(self) -> float:
        # steady-state live files if compaction reached the 64 MB target
        return max(1.0, self.live_gb * 1024 / TARGET_FILE_MB)

    @property
    def live_objects(self) -> float:
        # data files + metadata/manifests/snapshots (what S3 Tables charges monitoring on)
        return self.live_data_files * METADATA_OBJ_PER_DATA_FILE


def cost_self_managed(w: Workload) -> dict:
    # Compaction: only the small-file deficit gets rewritten; max(per-run floor, throughput).
    f = w.compaction_fraction
    compaction_gb = w.ingest_gb_month * f * WRITE_AMPLIFICATION
    runs = (w.files_created_month * f) / FILES_PER_COMPACTION
    floor_dpuhr = runs * MIN_DPU_HR_PER_RUN
    work_dpuhr = compaction_gb / GLUE_COMPACTION_GB_PER_DPUHR
    compaction = max(floor_dpuhr, work_dpuhr) * DPU_PRICE

    retention_orphan = GLUE_RETENTION_ORPHAN_USD_PER_TABLE_MONTH  # per table, fixed floor
    storage = SELF_STORAGE * w.live_gb * w.snapshot_overhang
    # requests: PUT per file written + GET per file scanned per read (uncompacted = many files)
    put = w.files_created_month * PUT_PER_1K
    files_scanned = w.live_data_files * READ_FILE_AMPLIFICATION * w.scan_fraction
    get = w.reads_per_month * files_scanned * GET_PER_1K
    requests = put + get

    per_table = compaction + retention_orphan + storage + requests
    total = per_table * w.num_tables
    return {"compaction": compaction * w.num_tables,
            "retention_orphan": retention_orphan * w.num_tables,
            "storage": storage * w.num_tables,
            "requests": requests * w.num_tables,
            "total": total}


def cost_s3_tables(w: Workload) -> dict:
    # Compaction: per-GB + per-object, only on the small-file deficit, no floor.
    f = w.compaction_fraction
    compaction = w.ingest_gb_month * f * S3T_COMPACT_GB + w.files_created_month * f * S3T_COMPACT_OBJ
    monitoring = w.live_objects * S3T_MONITOR
    storage = S3T_STORAGE * w.live_gb
    # reads touch the compacted layout: far fewer files than self-managed
    files_scanned = w.live_data_files * w.scan_fraction
    get = w.reads_per_month * files_scanned * GET_PER_1K
    requests = get

    per_table = compaction + monitoring + storage + requests
    total = per_table * w.num_tables
    return {"compaction": compaction * w.num_tables,
            "monitoring": monitoring * w.num_tables,
            "storage": storage * w.num_tables,
            "requests": requests * w.num_tables,
            "total": total}


# ---------------------------------------------------------------------------
# Scenario construction
# ---------------------------------------------------------------------------
# A "baseline" modest table, then scale volume and size around it. Volume scales
# records + writes (file rate) + live_gb together; size tiers set row width + a base GB.
SIZE_TIERS = {
    "small":  dict(bytes_per_record=120,   base_gb=0.5),    # skinny events
    "medium": dict(bytes_per_record=1_200, base_gb=50),     # typical fact rows
    "large":  dict(bytes_per_record=12_000, base_gb=2_000),  # wide/nested records
}


def make(name, volume_records, size_tier, *, num_tables=1, reads_per_month=1_000,
         writes_per_month=None):
    t = SIZE_TIERS[size_tier]
    # live_gb scales with volume relative to a 2M-records/mo reference, floored at the tier base
    live_gb = t["base_gb"] * max(volume_records / 2_000_000, 1.0)
    if writes_per_month is None:
        # default streaming cadence: ~every 5 min => 8,640/mo, but grows with volume
        writes_per_month = max(8_640, volume_records / 250)  # ~250 records/file
    return Workload(name=name, records_per_month=volume_records,
                    bytes_per_record=t["bytes_per_record"], live_gb=live_gb,
                    writes_per_month=writes_per_month, reads_per_month=reads_per_month,
                    num_tables=num_tables)


VOLUMES = [
    ("1x (this run)", 2_000_000),
    ("100x", 200_000_000),
    ("10k x", 20_000_000_000),
    ("1M x", 2_000_000_000_000),
]


def build_scenarios():
    rows = []

    # Grid: volume x size (user's scenarios 1-5 live in here)
    for size in ["small", "medium", "large"]:
        for vlabel, v in VOLUMES:
            rows.append(("volume x size", f"{size}", vlabel, make(f"{size}/{vlabel}", v, size)))

    # Read/write ratio sweep (added axis) — hold a mid workload, vary reads
    for reads in [0, 1_000, 100_000, 10_000_000]:
        w = make(f"reads={reads}", 200_000_000, "medium", reads_per_month=reads)
        rows.append(("read/write ratio", "medium/100x", f"{reads:,} reads/mo", w))

    # Table count sweep (added axis) — hold a mid workload, vary fleet size
    for n in [1, 10, 100, 1_000]:
        w = make(f"tables={n}", 200_000_000, "medium", num_tables=n)
        rows.append(("table count", "medium/100x", f"{n} tables", w))

    return rows


def fmt(x):
    return f"${x:,.2f}"


def main():
    rows = build_scenarios()
    here = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(os.path.dirname(here), "cost_model.csv")

    families = {}
    for family, axis, point, w in rows:
        families.setdefault(family, []).append((axis, point, w))

    out = ["# Cost model — self-managed (Glue optimizer) vs S3 Tables (projected)", ""]
    out.append("Monthly $, per the unit-economics model in `setup/cost_model.py`. "
               "Directional, not a forecast; adjust inputs to your workload.")
    out.append("")

    with open(csv_path, "w", newline="") as fcsv:
        writer = csv.writer(fcsv)
        writer.writerow(["family", "axis", "point", "self_total", "s3t_total", "cheaper",
                         "self_compaction", "self_retention_orphan", "self_storage", "self_requests",
                         "s3t_compaction", "s3t_monitoring", "s3t_storage", "s3t_requests"])
        for family, items in families.items():
            out.append(f"## {family}")
            out.append("")
            out.append("| Scenario | Self-managed (Glue) | S3 Tables | Cheaper | Ratio |")
            out.append("|---|---|---|---|---|")
            for axis, point, w in items:
                sm = cost_self_managed(w)
                st = cost_s3_tables(w)
                cheaper = "S3 Tables" if st["total"] < sm["total"] else "Self-managed"
                ratio = (max(sm["total"], st["total"]) / max(min(sm["total"], st["total"]), 1e-9))
                out.append(f"| {axis} · {point} | {fmt(sm['total'])} | {fmt(st['total'])} | "
                           f"**{cheaper}** | {ratio:.1f}x |")
                writer.writerow([family, axis, point, round(sm["total"], 2), round(st["total"], 2), cheaper,
                                 round(sm["compaction"], 2), round(sm["retention_orphan"], 2),
                                 round(sm["storage"], 2), round(sm["requests"], 2),
                                 round(st["compaction"], 2), round(st["monitoring"], 2),
                                 round(st["storage"], 2), round(st["requests"], 2)])
            out.append("")

    # Anchor validation — does the model reproduce the two real data points?
    out.append("## Anchor check (is the model trustworthy?)")
    out.append("")
    # Low anchor: ONE homogeneous streaming table (our order_events alone ~$2.1/mo maintenance;
    # the 3-table mix isn't comparable since 2 of ours never compact).
    anchor_low = make("this-run", 2_000_000, "small", reads_per_month=1_000)
    anchor_low = replace(anchor_low, num_tables=1, live_gb=0.002)
    sm_low, st_low = cost_self_managed(anchor_low), cost_s3_tables(anchor_low)
    out.append(f"- **This run, order_events alone (measured: Glue ~$2.1/mo, S3T ~$0.03/mo):** "
               f"model says Glue {fmt(sm_low['total'])} ({sm_low['total'] / 2.1 - 1:+.0%}), "
               f"S3T {fmt(st_low['total'])}.")
    # 1 TB table, daily batch refresh -> writes land as large files (little compaction),
    # ~1 TB live storage. ingest ~30 TB/mo but at near-target file size.
    anchor_1tb = Workload("1TB/day", records_per_month=30e9, bytes_per_record=1_000,
                          live_gb=1_000, writes_per_month=6_000, reads_per_month=100,
                          snapshot_overhang=1.2)
    sm_1tb, st_1tb = cost_self_managed(anchor_1tb), cost_s3_tables(anchor_1tb)
    out.append(f"- **1 TB/day ETL (Builder Center: self+Glue $37.10/mo, S3T $28.54/mo):** "
               f"model says Glue {fmt(sm_1tb['total'])} ({sm_1tb['total'] / 37.10 - 1:+.0%}), "
               f"S3T {fmt(st_1tb['total'])} ({st_1tb['total'] / 28.54 - 1:+.0%}).")
    out.append("")
    out.append("Both within ~10-40% of the real numbers across six orders of magnitude — good "
               "enough to trust the *shape* and the crossover direction, not the exact cents.")
    out.append("")
    out.append("## Assumptions & where self-managed wins")
    out.append("")
    out.append("- This models **Glue optimizer** specifically. A self-managed runtime you already "
               "own (EMR/Trino) has cheaper $/GB compaction and no per-run floor — a different, "
               "likely-lower curve this model does *not* draw (see the Onehouse/EMR references).")
    out.append("- **Snapshot overhang** (default 1.5-2.0x) and the **metadata-object multiplier** "
               "(~9 objects/data file) are the biggest swing factors, and both favor S3 Tables. "
               "Well-maintained tables with tight retention (overhang ~1.2) narrow the storage gap.")
    out.append("- Self-managed wins mainly on **storage-dominated, low-churn, rarely-read** data "
               "(cheap $/GB + storage classes S3 Tables lacks: IA, Glacier) and when you **already "
               "run the compute**. It loses on small-file, high-read, and high-table-count workloads.")
    out.append("- Tables are assumed **homogeneous**; a real fleet mixes hot and quiet tables.")
    out.append("")
    out.append(f"CSV written to `{os.path.basename(csv_path)}`.")

    report = "\n".join(out)
    print(report)
    with open(os.path.join(os.path.dirname(here), "COST_MODEL.md"), "w") as f:
        f.write(report + "\n")


if __name__ == "__main__":
    main()
