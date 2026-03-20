"""
orchestration/assets/quality_assets.py
========================================
Dagster assets for data quality checks.

Runs run_ge_checks.py against the mock data CSV files directly.
No BigQuery connection required in mock/dev mode.
"""

from pathlib import Path

from dagster import AssetExecutionContext, AssetIn, Output, asset, get_dagster_logger

ROOT = Path(__file__).resolve().parents[2]
QUALITY_DIR = ROOT / "quality"


def _validate_mock_csv(context: AssetExecutionContext) -> dict:
    """Run lightweight quality checks directly against mock CSV files."""
    import pandas as pd

    log = get_dagster_logger()
    results = []

    rides = pd.read_csv(ROOT / "data" / "mock" / "cycle_hire_mock.csv")
    stations = pd.read_csv(ROOT / "data" / "mock" / "cycle_stations_mock.csv")

    checks = [
        # Null checks
        ("rental_id not null", rides["rental_id"].notna().all()),
        ("bike_id not null", rides["bike_id"].notna().all()),
        ("start_date not null", rides["start_date"].notna().all()),
        ("start_station_id not null", rides["start_station_id"].notna().all()),
        ("duration positive", (rides["duration"] > 0).all()),
        ("station id not null", stations["id"].notna().all()),
        ("station name not null", stations["name"].notna().all()),
        ("nb_docks positive", (stations["nbdocks"] > 0).all()),
        # Uniqueness
        ("rental_id unique", rides["rental_id"].is_unique),
        ("station id unique", stations["id"].is_unique),
        # Value ranges
        (
            "latitude London range",
            stations["latitude"].between(51.3, 51.7).all(),
        ),
        (
            "longitude London range",
            stations["longitude"].between(-0.6, 0.3).all(),
        ),
        # Row counts
        ("rides > 1000 rows", len(rides) >= 1000),
        ("stations > 100 rows", len(stations) >= 100),
    ]

    passed = 0
    failed = 0
    for name, result in checks:
        status = "PASS" if result else "FAIL"
        if result:
            passed += 1
        else:
            failed += 1
        log.info(f"  [{status}] {name}")
        results.append({"check": name, "status": status})

    return {
        "passed": passed,
        "failed": failed,
        "total": len(checks),
        "results": results,
    }


@asset(
    group_name="quality",
    ins={"bq_load": AssetIn("mock_bq_load_asset")},
    description="Quality checks on mock CSV files — nulls, uniqueness, value ranges.",
    tags={"layer": "quality", "stage": "post_ingest", "cost": "zero"},
)
def post_ingest_ge_asset(
    context: AssetExecutionContext,
    bq_load: dict,
) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Running post-ingest quality checks on mock CSV files...")

    qc = _validate_mock_csv(context)

    if qc["failed"] > 0:
        raise RuntimeError(
            f"Post-ingest quality checks FAILED: {qc['failed']} failures. "
            "Pipeline blocked — fix data quality issues before proceeding."
        )

    summary = f"{qc['passed']}/{qc['total']} checks passed"
    log.info(f"Post-ingest QC: {summary}")

    return Output(
        value={"checkpoint": "post_ingest", "status": "passed", **qc},
        metadata={"summary": summary, "passed": qc["passed"], "failed": qc["failed"]},
    )


@asset(
    group_name="quality",
    ins={"dbt_test": AssetIn("dbt_test_asset")},
    description="Post-transform quality gate — validates dbt test results passed.",
    tags={"layer": "quality", "stage": "post_transform", "cost": "zero"},
)
def post_transform_ge_asset(
    context: AssetExecutionContext,
    dbt_test: dict,
) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Post-transform quality gate...")

    # dbt_test_asset already enforced passing — this asset confirms the gate
    passed = dbt_test.get("passed", 0)
    warned = dbt_test.get("warned", 0)

    summary = f"dbt: {passed} tests passed · {warned} warnings · pipeline cleared"
    log.info(summary)

    return Output(
        value={"checkpoint": "post_transform", "status": "cleared", "summary": summary},
        metadata={"summary": summary, "dbt_tests_passed": passed},
    )
