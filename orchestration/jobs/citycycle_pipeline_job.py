"""
orchestration/jobs/citycycle_pipeline_job.py
=============================================
Dagster Definitions — entry point for the CityCycle pipeline.

Asset execution order (enforced by dependency graph):

  mock_data_asset
    └── mock_bq_load_asset
          ├── post_ingest_ge_asset     ← quality gate 1
          └── dbt_compile_asset
                └── dbt_test_asset
                      └── post_transform_ge_asset  ← quality gate 2

Dev job: runs full pipeline against mock data (zero BQ cost)
Prod job: swap mock_bq_load_asset → meltano_ingest_asset for live BQ

Schedule: daily at 02:00 UTC (production reference — stopped by default)
"""

import sys
from pathlib import Path

# Ensure repo root is on path so orchestration.assets imports work
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dagster import (  # noqa: E402
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from orchestration.assets import (
    ingestion_assets,
    quality_assets,
    transform_assets,
)  # noqa: E402

# ── Load all assets ───────────────────────────────────────────────
all_assets = load_assets_from_modules(
    [ingestion_assets, transform_assets, quality_assets]
)

# ── Retry policy ─────────────────────────────────────────────────
pipeline_retry_policy = RetryPolicy(max_retries=2, delay=60)

# ── Dev pipeline (mock data, zero BQ cost) ────────────────────────
citycycle_dev_job = define_asset_job(
    name="citycycle_dev_pipeline",
    selection=AssetSelection.groups("ingestion", "transform", "quality"),
    description="Full CityCycle pipeline using mock data — zero BigQuery cost",
    tags={"env": "dev"},
)

# ── Daily schedule (production reference — stopped by default) ────
daily_schedule = ScheduleDefinition(
    name="citycycle_daily_02utc",
    job=citycycle_dev_job,
    cron_schedule="0 2 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Daily CityCycle pipeline — 02:00 UTC (activate in prod)",
)

# ── Dagster Definitions (single entry point) ──────────────────────
defs = Definitions(
    assets=all_assets,
    jobs=[citycycle_dev_job],
    schedules=[daily_schedule],
)
