"""
orchestration/assets/transform_assets.py
==========================================
Dagster assets for the dbt transformation layer.

In mock/dev mode: runs dbt compile + dbt test (no BQ scan cost).
In prod mode: runs dbt run + dbt test against live BigQuery.
"""

import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, AssetIn, Output, asset, get_dagster_logger

ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = ROOT / "transform"


def _run_dbt(args: list, context: AssetExecutionContext) -> subprocess.CompletedProcess:
    log = get_dagster_logger()
    from pathlib import Path as _Path

    profiles_dir = str(_Path.home() / ".dbt")
    cmd = ["dbt"] + args + ["--profiles-dir", profiles_dir]
    log.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(DBT_DIR),
    )

    if result.stdout:
        log.info(result.stdout[-3000:])
    if result.stderr:
        log.warning(result.stderr[-500:])

    return result


@asset(
    group_name="transform",
    ins={"bq_load": AssetIn("mock_bq_load_asset")},
    description="Compile all dbt models — validates SQL without executing against BQ.",
    tags={"layer": "transform", "cost": "zero"},
)
def dbt_compile_asset(
    context: AssetExecutionContext,
    bq_load: dict,
) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Running dbt compile (validates SQL syntax, no BQ scan cost)...")

    result = _run_dbt(["compile"], context)

    if result.returncode != 0:
        raise RuntimeError(f"dbt compile failed:\n{result.stdout[-2000:]}")

    lines = result.stdout.splitlines()
    summary = next(
        (
            line
            for line in reversed(lines)
            if "Done" in line or "Completed" in line or "models" in line.lower()
        ),
        "dbt compile completed",
    )

    log.info(f"dbt compile succeeded: {summary}")

    return Output(
        value={"status": "compiled", "summary": summary},
        metadata={"dbt_summary": summary, "mode": "compile_only (no BQ scan)"},
    )


@asset(
    group_name="transform",
    ins={"dbt_compile": AssetIn("dbt_compile_asset")},
    description="Run dbt schema tests and custom SQL assertions against mock data.",
    tags={"layer": "transform", "cost": "zero"},
)
def dbt_test_asset(
    context: AssetExecutionContext,
    dbt_compile: dict,
) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Running dbt test (schema + custom assertions)...")

    result = _run_dbt(["test"], context)

    lines = result.stdout.splitlines()

    # Count PASS/WARN/ERROR
    passed = sum(1 for line in lines if "PASS" in line)
    warned = sum(1 for line in lines if "WARN" in line)
    errors = sum(1 for line in lines if "ERROR" in line and "Completed" not in line)

    if result.returncode != 0 and errors > 0:
        failed_tests = [line for line in lines if "FAIL" in line or "ERROR" in line]
        raise RuntimeError(
            f"dbt tests failed ({errors} errors):\n" + "\n".join(failed_tests[:20])
        )

    summary = f"{passed} passed · {warned} warnings · {errors} errors"
    log.info(f"dbt test result: {summary}")

    return Output(
        value={"status": "passed", "passed": passed, "warned": warned},
        metadata={"test_summary": summary},
    )
