"""
Enable the AWS Glue Data Catalog optimizers (the "semi-managed" maintenance
runtime) on the self-managed Iceberg tables, as a true three-job peer to the
fully-managed S3 Tables maintenance.

As of 2026 the Glue optimizer runs all three maintenance jobs, so we enable all
three on order_events and orders_current: compaction, snapshot retention, and
orphan-file deletion. Retention is matched to the S3 Tables side (keep newest,
expire older than 1 day) so snapshot expiry actually fires within the run and
the comparison is apples-to-apples. dim_products is skipped (slow-changing,
never crosses the compaction threshold).

Usage:
    python setup/setup_glue_optimizer.py

Docs: https://docs.aws.amazon.com/glue/latest/dg/optimization-prerequisites.html
Reads AWS credentials from the environment and cloud resource names from .env
(see config.py). Designed to be idempotent.
"""
import json

import boto3

# config.py/schemas.py live one level up at the experiments root.
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GENERAL_BUCKET_NAME, GLUE_OPTIMIZER_ROLE_NAME, REGION
from config import GLUE_DATABASE as DATABASE
from schemas import SNAPSHOT_RETAIN_MIN

ROLE_NAME = GLUE_OPTIMIZER_ROLE_NAME
TABLES_TO_OPTIMIZE = ["order_events", "order_events_hourly", "orders_current"]

# Match the S3 Tables snapshot config: keep the newest snapshot, expire anything
# older than 1 day, and delete the data files the expired snapshots freed.
RETENTION_DAYS = 1
ORPHAN_RETENTION_DAYS = 3

# The three optimizer types and their per-type configuration blocks.
OPTIMIZER_CONFIGS = {
    "compaction": {
        "compactionConfiguration": {"icebergConfiguration": {"strategy": "binpack"}}
    },
    "retention": {
        "retentionConfiguration": {
            "icebergConfiguration": {
                "snapshotRetentionPeriodInDays": RETENTION_DAYS,
                "numberOfSnapshotsToRetain": SNAPSHOT_RETAIN_MIN,
                "cleanExpiredFiles": True,
            }
        }
    },
    "orphan_file_deletion": {
        "orphanFileDeletionConfiguration": {
            "icebergConfiguration": {"orphanFileRetentionPeriodInDays": ORPHAN_RETENTION_DAYS}
        }
    },
}


def get_account_id() -> str:
    return boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]


def ensure_optimizer_role(iam, account_id: str) -> str:
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        resp = iam.get_role(RoleName=ROLE_NAME)
        role_arn = resp["Role"]["Arn"]
        print(f"[ok] IAM role already exists: {role_arn}")
    except iam.exceptions.NoSuchEntityException:
        print(f"[create] Creating IAM role '{ROLE_NAME}'...")
        resp = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Runs AWS Glue Iceberg compaction for the 005 cost-crossover experiment",
        )
        role_arn = resp["Role"]["Arn"]
        print(f"[ok] Created IAM role: {role_arn}")

    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
                "Resource": [f"arn:aws:s3:::{GENERAL_BUCKET_NAME}/*"],
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{GENERAL_BUCKET_NAME}"],
            },
            {
                "Effect": "Allow",
                "Action": ["glue:UpdateTable", "glue:GetTable"],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{account_id}:table/{DATABASE}/*",
                    f"arn:aws:glue:{REGION}:{account_id}:database/{DATABASE}",
                    f"arn:aws:glue:{REGION}:{account_id}:catalog",
                ],
            },
            {
                "Effect": "Allow",
                "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": [
                    f"arn:aws:logs:{REGION}:{account_id}:log-group:/aws-glue/iceberg-compaction/logs:*",
                    f"arn:aws:logs:{REGION}:{account_id}:log-group:/aws-glue/iceberg-retention/logs:*",
                    f"arn:aws:logs:{REGION}:{account_id}:log-group:/aws-glue/iceberg-orphan-file-deletion/logs:*",
                ],
            },
        ],
    }
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="iceberg-compaction-inline",
        PolicyDocument=json.dumps(inline_policy),
    )
    print("[ok] Inline policy attached/updated")
    return role_arn


def ensure_table_optimizer(glue, account_id: str, role_arn: str, table_name: str, opt_type: str):
    extra_config = OPTIMIZER_CONFIGS[opt_type]
    try:
        glue.get_table_optimizer(
            CatalogId=account_id, DatabaseName=DATABASE, TableName=table_name, Type=opt_type
        )
        print(f"[ok] {opt_type} optimizer already enabled for {DATABASE}.{table_name}")
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    print(f"[create] Enabling {opt_type} optimizer for {DATABASE}.{table_name}...")
    glue.create_table_optimizer(
        CatalogId=account_id,
        DatabaseName=DATABASE,
        TableName=table_name,
        Type=opt_type,
        TableOptimizerConfiguration={"roleArn": role_arn, "enabled": True, **extra_config},
    )
    print(f"[ok] Enabled {opt_type} optimizer for {DATABASE}.{table_name}")


def main():
    account_id = get_account_id()
    print(f"Account: {account_id}, Region: {REGION}")

    iam = boto3.client("iam", region_name=REGION)
    role_arn = ensure_optimizer_role(iam, account_id)

    glue = boto3.client("glue", region_name=REGION)
    for table_name in TABLES_TO_OPTIMIZE:
        for opt_type in OPTIMIZER_CONFIGS:
            ensure_table_optimizer(glue, account_id, role_arn, table_name, opt_type)


if __name__ == "__main__":
    main()
