"""
Provision the S3 buckets for the 005 "iceberg cost crossover" experiment:
one general-purpose S3 bucket (self-managed Iceberg: Glue/Spark/Trino/PyIceberg)
and one S3 Tables bucket (managed baseline), same pattern as Part 4.

Usage:
    python setup/provision.py

Reads AWS credentials from the environment and cloud resource names from .env
(see config.py and .env.example). Designed to be idempotent: safe to re-run.
"""
import boto3
from botocore.exceptions import ClientError

# config.py/schemas.py live one level up at the experiments root.
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GENERAL_BUCKET_NAME, OPS_BUCKET_NAME, REGION, S3TABLES_BUCKET_NAME, cost_tags


def get_account_id() -> str:
    sts = boto3.client("sts", region_name=REGION)
    return sts.get_caller_identity()["Account"]


def _secure_bucket(s3, name: str, side: str):
    """Block public access, default SSE, versioning, and cost-allocation tags."""
    s3.put_public_access_block(
        Bucket=name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_encryption(
        Bucket=name,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
        },
    )
    s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_tagging(
        Bucket=name,
        Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in cost_tags(side).items()]},
    )
    print(f"[ok] Security baseline + tags applied to {name} (side={side})")


def _ensure_bucket(name: str, side: str) -> str:
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.head_bucket(Bucket=name)
        print(f"[ok] Bucket already exists: {name}")
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise
        print(f"[create] Creating bucket '{name}' in {REGION}...")
        s3.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print(f"[ok] Created bucket: {name}")
    _secure_bucket(s3, name, side)
    return f"arn:aws:s3:::{name}"


def ensure_general_bucket() -> str:
    """General-purpose S3 bucket for self-managed Iceberg data + Glue Catalog metadata."""
    return _ensure_bucket(GENERAL_BUCKET_NAME, side="glue")


def ensure_ops_bucket() -> str:
    """Ops bucket for the ingest high-water state object + monitor metrics."""
    return _ensure_bucket(OPS_BUCKET_NAME, side="producer")


def ensure_s3tables_bucket() -> str:
    """Managed baseline: S3 Tables bucket."""
    s3tables = boto3.client("s3tables", region_name=REGION)
    account_id = get_account_id()
    expected_arn = f"arn:aws:s3tables:{REGION}:{account_id}:bucket/{S3TABLES_BUCKET_NAME}"

    try:
        resp = s3tables.get_table_bucket(tableBucketARN=expected_arn)
        print(f"[ok] Table bucket already exists: {resp['arn']}")
        return resp["arn"]
    except s3tables.exceptions.NotFoundException:
        pass

    print(f"[create] Creating table bucket '{S3TABLES_BUCKET_NAME}' in {REGION}...")
    resp = s3tables.create_table_bucket(name=S3TABLES_BUCKET_NAME)
    arn = resp["arn"]
    print(f"[ok] Created table bucket: {arn}")
    return arn


def main():
    print(f"Region: {REGION}")
    general_arn = ensure_general_bucket()
    ops_arn = ensure_ops_bucket()
    s3tables_arn = ensure_s3tables_bucket()

    print("\n--- Summary ---")
    print(f"General-purpose bucket (self-managed Iceberg): {general_arn}")
    print(f"Ops bucket (ingest state + monitor metrics):   {ops_arn}")
    print(f"S3 Tables bucket (managed baseline):            {s3tables_arn}")


if __name__ == "__main__":
    main()
