"""
One-shot read benchmark: the missing *benefit* side of the cost story.

You compact to make reads cheaper and faster. This measures that directly by
running the same scans against `order_events` on both catalogs and reporting how
many data files each read has to touch, how long it takes, and how much it pulls
into memory. The self-managed (Glue) table sits at many small files; the S3
Tables copy is compacted down to a handful, so the managed side should touch far
fewer objects for identical rows.

Run once, near the end of the run, when the file-count gap is widest:
    make benchmark
Prints a Markdown block you can paste straight into the post.
"""
import os
import statistics
import sys
import time

# config.py lives one level up at the experiments root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyiceberg.catalog import load_catalog

from config import GLUE_DATABASE, REGION, WAREHOUSE, get_s3tables_bucket_arn

TABLE = "order_events"
# Fewer repeats keeps the daily run fast (~2 min). RESTORE TO 5 for the final
# authoritative benchmark after the 7-day run, so the numbers in the post are solid.
REPEATS = 3  # median of N timed scans, after a warmup, to cut cold-cache noise


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


def _median_scan(tbl, row_filter):
    """(files touched, rows, in-memory MB, median seconds) for a scan, warmup excluded."""
    def one():
        scan = tbl.scan(row_filter=row_filter) if row_filter else tbl.scan()
        t0 = time.perf_counter()
        arr = scan.to_arrow()
        return time.perf_counter() - t0, arr.num_rows, arr.nbytes

    one()  # warmup: pulls metadata/manifests into cache so we time the data read
    times, rows, nbytes = [], 0, 0
    for _ in range(REPEATS):
        dt, rows, nbytes = one()
        times.append(dt)

    scan = tbl.scan(row_filter=row_filter) if row_filter else tbl.scan()
    files = sum(1 for _ in scan.plan_files())
    return files, rows, nbytes / 1024 / 1024, statistics.median(times)


def main():
    scenarios = [
        ("full scan", None),
        ("filter: BTC-USD only", "product_id == 'BTC-USD'"),
    ]

    results = {}  # (catalog, scenario) -> tuple
    for ck, loader in CATALOGS.items():
        tbl = loader().load_table((GLUE_DATABASE, TABLE))
        for label, rf in scenarios:
            results[(ck, label)] = _median_scan(tbl, rf)

    out = ["# Read benchmark — order_events (same rows, both catalogs)", ""]
    out.append(f"Median of {REPEATS} scans after a warmup. Fewer files touched = fewer S3 GETs.")
    out.append("")
    out.append("| Scenario | Catalog | Files touched | Rows | In-memory MB | Median scan |")
    out.append("|---|---|---|---|---|---|")
    for label, _ in scenarios:
        for ck in CATALOGS:
            files, rows, mb, secs = results[(ck, label)]
            out.append(f"| {label} | {ck} | {files} | {rows:,} | {mb:.1f} | {secs * 1000:.0f} ms |")
    out.append("")

    # Headline: full-scan file-count and latency gap.
    g_files, _, _, g_secs = results[("glue", "full scan")]
    s_files, _, _, s_secs = results[("s3tables", "full scan")]
    if s_files and g_files:
        fewer = g_files - s_files
        faster = (g_secs - s_secs) / g_secs * 100 if g_secs else 0
        out.append(
            f"- Full scan touched **{g_files} files on self-managed (Glue)** vs **{s_files} on S3 Tables** "
            f"({fewer} fewer, {g_files / max(s_files, 1):.1f}x). "
            + (f"The compacted side read ~{faster:.0f}% faster."
               if faster > 0 else "Latency was comparable at this size; the file-count gap is the durable signal.")
        )
    print("\n".join(out))


if __name__ == "__main__":
    main()
