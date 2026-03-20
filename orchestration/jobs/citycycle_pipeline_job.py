"""
orchestration/jobs/citycycle_pipeline_job.py
=============================================
Defines the full CityCycle pipeline job.

Asset execution order (enforced by dependency graph):
  mock_data_asset
    └── mock_bq_load_asset
          ├── post_ingest_ge_asset          ← quality gate 1
          └── dbt_run_asset
                └── dbt_test_asset
                      └── post_transform_ge_asset  ← quality gate 2

Schedule: daily at 02:00 UTC (after midnight London time).
Retries: up to 3 retries with 5-minute delay between attempts.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

# Import all asset modules
from orchestration.assets import (
    ingestion_assets,
    quality_assets,
    transform_assets,
)

# ── Load all assets ───────────────────────────────────────────────
all_assets = load_assets_from_modules(
    [ingestion_assets, transform_assets, quality_assets]
)

# ── Retry policy: up to 3 retries, 5 min between attempts ────────
pipeline_retry_policy = RetryPolicy(
    max_retries=3,
    delay=300,  # 5 minutes in seconds
)

# ── Full pipeline job (mock/dev path) ─────────────────────────────
citycycle_dev_job = define_asset_job(
    name="citycycle_dev_pipeline",
    selection=AssetSelection.groups("ingestion", "quality", "transform"),
    description="Full CityCycle pipeline using mock data (no BQ scan cost)",
    tags={"env": "dev"},
)

# ── Production job (uses meltano_ingest_asset instead of mock) ───
citycycle_prod_job = define_asset_job(
    name="citycycle_prod_pipeline",
    selection=AssetSelection.assets(
        "meltano_ingest_asset",
        "post_ingest_ge_asset",
        "dbt_run_asset",
        "dbt_test_asset",
        "post_transform_ge_asset",
    ),
    description="Full CityCycle pipeline using live BQ data via Meltano",
    tags={"env": "prod"},
)

# ── Daily schedule: 02:00 UTC ─────────────────────────────────────
daily_schedule = ScheduleDefinition(
    name="citycycle_daily_02utc",
    job=citycycle_prod_job,
    cron_schedule="0 2 * * *",  # 02:00 UTC daily
    default_status=DefaultScheduleStatus.STOPPED,  # manually activate in prod
    description="Daily CityCycle pipeline — runs at 02:00 UTC",
)

# ── Dagster Definitions object (entry point) ──────────────────────
defs = Definitions(
    assets=all_assets,
    jobs=[citycycle_dev_job, citycycle_prod_job],
    schedules=[daily_schedule],
)
