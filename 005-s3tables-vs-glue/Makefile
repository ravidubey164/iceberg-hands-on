# Task runner for the iceberg-maint-eval experiment — a thin wrapper over the
# idempotent Python scripts in setup/. See README.md for the full walkthrough.
#
# Override the interpreter or AWS settings through your environment, e.g.:
#   make PY="uv run python" AWS_PROFILE=my-profile deploy
#
# Typical first run:  make setup && make deploy
# Clean restart:      make reset CONFIRM=1
# Daily check:        make status

PY ?= uv run python

# AWS settings are intentionally supplied by the caller or their local .env file.
export AWS_PAGER ?=

.DEFAULT_GOAL := help

.PHONY: help check-sso provision tables seed optimizer setup deploy status benchmark model reset

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}'

check-sso:  ## Verify the SSO session is valid, re-login if it has expired
	@aws sts get-caller-identity --profile $(AWS_PROFILE) >/dev/null 2>&1 || { \
		echo "SSO session for profile '$(AWS_PROFILE)' is expired/missing — running aws sso login..."; \
		aws sso login --profile $(AWS_PROFILE); \
	}

provision: check-sso  ## Create + tag the buckets (lakehouse, ops, S3 Tables)
	$(PY) setup/provision.py

tables: check-sso  ## Create the three Iceberg tables on both catalogs
	$(PY) setup/create_tables.py
	$(PY) setup/create_s3tables.py

seed: check-sso  ## Seed dim_products from the live Coinbase product catalog
	$(PY) setup/fetch_products.py
	$(PY) setup/load_dim_products.py

optimizer: check-sso  ## Enable the Glue optimizer (compaction + retention + orphan cleanup)
	$(PY) setup/setup_glue_optimizer.py

setup: provision tables seed optimizer  ## Full stand-up: buckets -> tables -> seed -> optimizer

deploy: check-sso  ## Build+push the producer image, create/update the Lambda + schedule
	$(PY) setup/deploy.py --build

status: check-sso  ## Records/sizes/runs/failures/cost since restart, as Markdown (MYT timestamps)
	@$(PY) setup/check_status.py

benchmark: check-sso  ## One-shot read benchmark: files touched + scan time, Glue vs S3 Tables
	$(PY) setup/read_benchmark.py

model:  ## Project self-managed vs S3 Tables cost across volume/size/reads/tables (no AWS calls)
	$(PY) setup/cost_model.py

reset: check-sso  ## DESTRUCTIVE: purge + recreate all tables, then re-enable the optimizer (needs CONFIRM=1)
ifneq ($(CONFIRM),1)
	@echo "Refusing to reset without CONFIRM=1 (this purges ALL table data on both catalogs)."; exit 1
endif
	$(PY) setup/reset_tables.py --yes
	$(PY) setup/setup_glue_optimizer.py
