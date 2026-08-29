# 005-S3-Tables-vs-Glue

Run the same Iceberg workload on **self-managed AWS (Glue Catalog + S3)** and **S3 Tables** side by
side, then measure what each one costs to keep healthy.

**Coinbase API** (live trades) → **one writer** → two catalogs, each maintained by its own
fully-managed job runner: S3 Tables' automatic maintenance vs the Glue Data Catalog optimizer
(compaction, snapshot retention, orphan-file deletion — [as of 2026, Glue does all three](https://docs.aws.amazon.com/glue/latest/dg/compaction-management.html)).

Companion repo for Part 5 of the Data Lakehouse blog series: *"S3 Tables vs the Glue Optimizer: What
Does the Managed Premium Actually Buy?"* See [ARCHITECTURE.md](ARCHITECTURE.md) for the layered diagrams.

---

## What you're building

| Piece | Role |
|---|---|
| `order_events` | Append-only trade stream. The small-file / compaction stress table. |
| `orders_current` | CDC upsert (MERGE INTO). The snapshot- and delete-file-churn table. |
| `dim_products` | Slow-changing dimension. Stands in for "lots of quiet tables". |
| Self-managed side | Glue Data Catalog + a general-purpose S3 bucket. You run maintenance. |
| S3 Tables side | A table bucket with the same three tables. AWS runs maintenance. |

Both sides get **identical data** from one writer, so cost numbers are apples-to-apples.

---

## Why this setup?

- **Real data, not a generator.** Polling the Coinbase public API gives genuine write cadence and
  burstiness (volatility = real small-file storms). A synthetic Poisson rate wouldn't.
- **Two catalogs, one writer.** The writer appends/merges into both Glue and S3 Tables in the same
  run. Same rows, same schema, so any cost difference is the *maintenance*, not the data.
- **Matched config on both sides.** Same target file size (64MB) and snapshot retention (keep 1 /
  max 24h) on both catalogs, so a difference in outcome is the runtime, not the settings.
- **`.env`-driven names.** No bucket/database names are hardcoded, so you can clone and run against
  your own account without collisions.

---

## Prerequisites

- An AWS account and credentials (`aws sts get-caller-identity` should work).
- Python 3.12+ and [`uv`](https://docs.astral.sh/uv/).
- Permissions to create: S3 buckets, an S3 Tables bucket, a Glue database + tables, and an IAM role
  for the Glue optimizer.
- Region with S3 Tables available (this repo defaults to `eu-north-1`).

> Bucket names are globally unique across all of AWS. Pick names nobody else has taken (suffix with
> your initials or account id).

## Setup

```bash
cd experiments
uv venv && uv pip install -r requirements.txt
cp .env.example .env      # then edit .env with your own bucket names + AWS profile
```

Fill in `.env` (the names with defaults are fine to leave as-is):

```
AWS_PROFILE=your-profile
AWS_REGION=eu-north-1
GENERAL_BUCKET_NAME=your-unique-bucket-name
S3TABLES_BUCKET_NAME=your-unique-bucket-name-tables
```

---

## Option A — the 10-minute taste

Prove the whole loop end to end without waiting a week. All setup scripts live in `setup/`.

**1. Provision buckets + both sets of tables**

```bash
python setup/provision.py          # S3 bucket + S3 Tables bucket
python setup/create_tables.py      # Glue Catalog tables
python setup/create_s3tables.py    # S3 Tables tables (same schema)
python setup/fetch_products.py     # seed dim_products source
python setup/load_dim_products.py  # load the dimension
```

**2. Write a small batch into both catalogs**

Poll for ~2 minutes, then flush:

```bash
python poll_trades.py --pairs BTC-USD,ETH-USD --interval 20 --max-hours 0.03
python write_events.py
```

You'll see append/upsert counts for both `glue` and `s3tables`.

**3. Enable self-managed maintenance (Glue optimizer)**

```bash
python setup/setup_glue_optimizer.py   # turns on all 3 jobs: compaction, retention, orphan-file deletion
```

**4. Read the cost off the CLI**

```bash
# Glue optimizer's last compaction run (files compacted, DPU-hours):
aws glue get-table-optimizer \
  --catalog-id <ACCOUNT_ID> --database-name iceberg_cost_crossover \
  --table-name order_events --type compaction

# S3 Tables maintenance status (compaction / snapshot / orphan):
aws s3tables get-table-maintenance-job-status \
  --table-bucket-arn <S3TABLES_BUCKET_ARN> \
  --namespace iceberg_cost_crossover --name order_events
```

That's the entire measurement loop: write → maintain → read the numbers. Everything after this is
just *scale and time*.

---

## Option B — the full run (real small-file accumulation)

The 10-minute taste won't build up enough small files to make compaction cost interesting. For the
numbers in the blog, let the poller run for days:

```bash
python poll_trades.py --pairs BTC-USD,ETH-USD --interval 60 --max-hours 168
```

Then run `write_events.py` on a schedule (every 5 minutes) so files accumulate the way a real
streaming table's would. Any scheduler works — cron, a systemd timer, an EventBridge rule, or a
`while true; do python write_events.py; sleep 300; done` loop.

> How I ran it: `poll_trades.py` and a 5-minute `write_events.py` timer as `systemd` user services
> for 7 days. That's a personal-rig detail, not a requirement — see the note in
> [ARCHITECTURE.md](ARCHITECTURE.md). Both scripts persist state (`poller_state.json`,
> `writer_state.json`), so a crash or restart resumes cleanly instead of double-writing or losing
> data.

**Gotcha:** `poll_trades.py` writes to a local `order_events.ndjson`; `write_events.py` flushes new
lines into both catalogs and tracks a **separate offset per catalog**. If one catalog fails (e.g. an
expired token), the next run retries just that side — no double-writes, no lost rows.

---

## Reset

To wipe both catalogs and start clean (matched config re-applied on both sides):

```bash
python setup/reset_tables.py --yes     # purges + recreates Glue AND S3 Tables (--glue-only to skip S3)
python setup/setup_glue_optimizer.py   # the optimizer must be re-enabled after a table recreate
```

**Gotcha:** the Glue optimizer does **not** survive a table purge + recreate. Re-run
`setup_glue_optimizer.py` after any reset. Also clear the local state files
(`order_events.ndjson`, `poller_state.json`, `writer_state.json`, `maintenance_metrics.ndjson`)
so the poller starts from a fresh high-water mark instead of resuming old offsets.

**Gotcha:** `reset_tables.py`'s S3 Tables purge must use `catalog.purge_table(...)`, not
`drop_table(...)` — S3 Tables' REST catalog rejects a plain drop and fails silently if you get
this wrong (the delete never happens, but the script doesn't error loudly).

---

## Layout

```
experiments/
  poll_trades.py          # live poller  -> order_events.ndjson
  write_events.py         # flush ndjson  -> both catalogs (append + MERGE)
  monitor_maintenance.py  # periodic snapshot of both catalogs' maintenance state -> maintenance_metrics.ndjson
  config.py               # reads .env, no hardcoded names
  schemas.py              # shared Iceberg schemas
  setup/                  # one-time provisioning + maintenance triggers
    provision.py, create_tables.py, create_s3tables.py,
    setup_glue_optimizer.py, reset_tables.py, ...
```
