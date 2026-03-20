"""
orchestration/assets/quality_assets.py
========================================
Dagster assets for Great Expectations quality checkpoints.
Two checkpoints:
  1. post_ingest_ge_asset   — after raw BQ load
  2. post_transform_ge_asset — after dbt run

If either checkpoint fails, the pipeline is blocked downstream.
"""

import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetIn,
    Output,
    asset,
    get_dagster_logger,
)

ROOT = Path(__file__).resolve().parents[2]
QUALITY_DIR = ROOT / "quality"


def _run_ge(checkpoint: str) -> tuple[bool, str]:
    """Run a GE checkpoint. Returns (passed: bool, output: str)."""
    result = subprocess.run(
        [
            sys.executable,
            str(QUALITY_DIR / "run_ge_checks.py"),
            "--checkpoint",
            checkpoint,
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    output = result.stdout + result.stderr
    return result.returncode == 0, output


# ══════════════════════════════════════════════════════════════════
# ASSET 1: Post-ingest quality check
# Runs after raw BQ tables are loaded, before dbt
# ══════════════════════════════════════════════════════════════════


@asset(
    group_name="quality",
    ins={"bq_load": AssetIn("mock_bq_load_asset")},
    description="GE checkpoint: validate raw BQ tables after ingest.",
    tags={"layer": "quality", "stage": "post_ingest"},
)
def post_ingest_ge_asset(
    context: AssetExecutionContext,
    bq_load: dict,
) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Running Great Expectations post-ingest checkpoint...")

    passed, output = _run_ge("post_ingest")
    log.info(output)

    if not passed:
        raise RuntimeError(
            "Post-ingest GE checkpoint FAILED. "
            "Raw data does not meet quality requirements. "
            "Pipeline blocked. Check GE data docs for details."
        )

    return Output(
        value={"checkpoint": "post_ingest", "status": "passed"},
        metadata={"ge_output": output[-1500:]},
    )


# ══════════════════════════════════════════════════════════════════
# ASSET 2: Post-transform quality check
# Runs after dbt, before ML + dashboard refresh
# ══════════════════════════════════════════════════════════════════


@asset(
    group_name="quality",
    ins={"dbt_test": AssetIn("dbt_test_asset")},
    description="GE checkpoint: validate marts tables after dbt transform.",
    tags={"layer": "quality", "stage": "post_transform"},
)
def post_transform_ge_asset(
    context: AssetExecutionContext,
    dbt_test: dict,
) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Running Great Expectations post-transform checkpoint...")

    passed, output = _run_ge("post_transform")
    log.info(output)

    if not passed:
        raise RuntimeError(
            "Post-transform GE checkpoint FAILED. "
            "Transformed data does not meet quality requirements. "
            "ML training and dashboard refresh blocked."
        )

    return Output(
        value={"checkpoint": "post_transform", "status": "passed"},
        metadata={"ge_output": output[-1500:]},
    )
