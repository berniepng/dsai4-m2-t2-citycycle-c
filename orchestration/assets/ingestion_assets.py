"""
orchestration/assets/ingestion_assets.py
=========================================
Dagster software-defined assets for the ingestion layer.

Two modes:
  mock_data_asset   — generates synthetic CSV data (no BQ cost)
  meltano_ingest_asset — runs Meltano tap-bigquery → target-bigquery

In dev/test, only mock_data_asset runs.
In prod, meltano_ingest_asset runs (triggered by Dagster schedule).
"""

import os
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

# ══════════════════════════════════════════════════════════════════
# ASSET 1: Mock data generation
# Produces: data/mock/cycle_hire_mock.csv
#           data/mock/cycle_stations_mock.csv
# ══════════════════════════════════════════════════════════════════


@asset(
    group_name="ingestion",
    description="Generate synthetic mock data matching BQ london_bicycles schema.",
    tags={"layer": "ingestion", "cost": "zero"},
)
def mock_data_asset(context: AssetExecutionContext) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Generating mock data...")

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "dashboard" / "utils" / "mock_data_generator.py"),
            "--rides",
            "10000",
            "--seed",
            "42",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )

    if result.returncode != 0:
        raise RuntimeError(f"Mock data generation failed:\n{result.stderr}")

    log.info(result.stdout)

    stations_path = ROOT / "data" / "mock" / "cycle_stations_mock.csv"
    rides_path = ROOT / "data" / "mock" / "cycle_hire_mock.csv"

    stations_rows = sum(1 for _ in open(stations_path)) - 1  # subtract header
    rides_rows = sum(1 for _ in open(rides_path)) - 1

    return Output(
        value={
            "stations_rows": stations_rows,
            "rides_rows": rides_rows,
            "stations_path": str(stations_path),
            "rides_path": str(rides_path),
        },
        metadata={
            "stations_rows": stations_rows,
            "rides_rows": rides_rows,
        },
    )


# ══════════════════════════════════════════════════════════════════
# ASSET 2: Mock CSV → BigQuery raw loader
# Depends on: mock_data_asset
# Produces: BQ raw.cycle_stations, BQ raw.cycle_hire
# ══════════════════════════════════════════════════════════════════


@asset(
    group_name="ingestion",
    ins={"mock_data": AssetIn("mock_data_asset")},
    description="Load mock CSV files into BigQuery raw dataset.",
    tags={"layer": "ingestion", "cost": "free"},
)
def mock_bq_load_asset(
    context: AssetExecutionContext,
    mock_data: dict,
) -> Output[dict]:
    log = get_dagster_logger()

    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable not set")

    log.info(f"Loading mock data to BigQuery project: {project_id}")

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "ingestion" / "load_mock.py"),
            "--mode=mock",
            f"--project={project_id}",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )

    if result.returncode != 0:
        raise RuntimeError(f"BQ load failed:\n{result.stderr}")

    log.info(result.stdout)

    return Output(
        value={"status": "loaded", "project": project_id},
        metadata={
            "project_id": project_id,
            "dataset": "citycycle_raw",
            "tables_loaded": ["cycle_stations", "cycle_hire"],
            "source_rows": mock_data["rides_rows"],
        },
    )


# ══════════════════════════════════════════════════════════════════
# ASSET 3: Live Meltano ingest (production)
# Runs tap-bigquery → target-bigquery via Meltano CLI
# Only used when MODE=prod
# ══════════════════════════════════════════════════════════════════


@asset(
    group_name="ingestion",
    description="[PROD] Run Meltano tap-bigquery → target-bigquery ingest.",
    tags={"layer": "ingestion", "env": "prod"},
)
def meltano_ingest_asset(context: AssetExecutionContext) -> Output[dict]:
    log = get_dagster_logger()
    meltano_dir = ROOT / "ingestion"

    log.info("Running Meltano tap-bigquery → target-bigquery...")

    result = subprocess.run(
        ["meltano", "--environment=prod", "run", "tap-bigquery", "target-bigquery"],
        capture_output=True,
        text=True,
        cwd=str(meltano_dir),
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Meltano ingest failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    log.info(result.stdout)

    return Output(
        value={"status": "ingested"},
        metadata={"meltano_output": result.stdout[-2000:]},  # last 2000 chars
    )
