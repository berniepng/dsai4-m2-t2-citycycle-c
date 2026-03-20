"""
orchestration/assets/transform_assets.py
==========================================
Dagster assets for dbt transformation layer.
Runs dbt run + dbt test after ingest completes.
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
DBT_DIR = ROOT / "transform"


def _run_dbt(
    args: list[str], context: AssetExecutionContext
) -> subprocess.CompletedProcess:
    """Helper: run a dbt command and raise on failure."""
    log = get_dagster_logger()
    cmd = [sys.executable, "-m", "dbt"] + args
    log.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(DBT_DIR),
    )

    # Always log stdout (dbt logs are in stdout)
    if result.stdout:
        log.info(result.stdout[-3000:])
    if result.stderr:
        log.warning(result.stderr[-1000:])

    return result


# ══════════════════════════════════════════════════════════════════
# ASSET: dbt run — all models
# ══════════════════════════════════════════════════════════════════


@asset(
    group_name="transform",
    ins={
        "bq_load": AssetIn("mock_bq_load_asset"),  # dev path
        # swap to AssetIn("meltano_ingest_asset") for prod
    },
    description="Run all dbt models: staging → intermediate → marts.",
    tags={"layer": "transform"},
)
def dbt_run_asset(
    context: AssetExecutionContext,
    bq_load: dict,
) -> Output[dict]:

    result = _run_dbt(["run", "--profiles-dir", str(DBT_DIR)], context)

    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stdout[-2000:]}")

    # Parse summary from dbt output
    lines = result.stdout.splitlines()
    summary = next(
        (
            line
            for line in reversed(lines)
            if "Completed" in line or "Done" in line or "models" in line.lower()
        ),
        "dbt run completed",
    )

    return Output(
        value={"status": "success", "summary": summary},
        metadata={"dbt_summary": summary},
    )


# ══════════════════════════════════════════════════════════════════
# ASSET: dbt test — schema + custom tests
# ══════════════════════════════════════════════════════════════════


@asset(
    group_name="transform",
    ins={"dbt_run": AssetIn("dbt_run_asset")},
    description="Run dbt schema tests and custom SQL assertions.",
    tags={"layer": "transform"},
)
def dbt_test_asset(
    context: AssetExecutionContext,
    dbt_run: dict,
) -> Output[dict]:

    result = _run_dbt(["test", "--profiles-dir", str(DBT_DIR)], context)

    if result.returncode != 0:
        # dbt test failures are blocking — extract which tests failed
        failed = [
            line
            for line in result.stdout.splitlines()
            if "FAIL" in line or "ERROR" in line
        ]
        raise RuntimeError(
            "dbt tests failed. Failing tests:\n" + "\n".join(failed[:20])
        )

    return Output(
        value={"status": "all_passed"}, metadata={"result": "All dbt tests passed"}
    )
