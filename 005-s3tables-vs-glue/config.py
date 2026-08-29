"""
Shared configuration for the 005 "iceberg cost crossover" experiment scripts.

Cloud resource names are read from environment variables (loaded from a local
.env via python-dotenv) instead of being hardcoded, so this repo can be cloned
and run without colliding with the author's own bucket/database names or AWS
account. Copy .env.example to .env and fill in your own values first.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable '{name}'. "
            "Copy .env.example to .env and fill in your own values."
        )
    return value


REGION = os.environ.get("AWS_REGION", "eu-north-1")
# S3 bucket names are globally unique across all of AWS, so these have no defaults.
GENERAL_BUCKET_NAME = _require("GENERAL_BUCKET_NAME")
S3TABLES_BUCKET_NAME = _require("S3TABLES_BUCKET_NAME")
# Ops bucket: holds the ingest high-water state object and the monitor metrics,
# kept separate from warehouse data so the producer's tiny S3 cost stays its own
# cost-allocation line (side=producer) instead of polluting the self-managed side.
OPS_BUCKET_NAME = _require("OPS_BUCKET_NAME")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "iceberg_maint_eval")
WAREHOUSE = f"s3://{GENERAL_BUCKET_NAME}/warehouse"

GLUE_OPTIMIZER_ROLE_NAME = os.environ.get(
    "GLUE_OPTIMIZER_ROLE_NAME", "iceberg-maint-eval-glue-optimizer"
)

# Project slug + cost-allocation tags. Every resource carries project + side so a
# dedicated-account Cost Explorer view can split S3 Tables vs Glue vs producer.
PROJECT = os.environ.get("PROJECT_TAG", "iceberg-maint-eval")


def cost_tags(side: str) -> dict:
    """side is one of: glue, s3tables, producer."""
    return {"project": PROJECT, "side": side}


# Trading pairs the ingest Lambda polls (comma-separated in PAIRS).
PAIRS = [p.strip() for p in os.environ.get("PAIRS", "BTC-USD,ETH-USD").split(",") if p.strip()]
# Ops-bucket keys: the single high-water state object and the metrics prefix.
HIGH_WATER_KEY = os.environ.get("HIGH_WATER_KEY", "state/high_water.json")
METRICS_PREFIX = os.environ.get("METRICS_PREFIX", "metrics/")


def get_s3tables_bucket_arn() -> str:
    """Resolved at call time (not cached in .env) so it stays portable across accounts."""
    import boto3

    account_id = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
    return f"arn:aws:s3tables:{REGION}:{account_id}:bucket/{S3TABLES_BUCKET_NAME}"
