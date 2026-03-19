"""
ingestion/bq_cost_guard.py
===========================
Pre-flight cost checker for all BigQuery operations.

BigQuery free tier: 1 TB processed per month.
This script tracks usage and blocks queries that would exceed the budget.

Usage:
    # Check before a query
    from ingestion.bq_cost_guard import guard
    guard.check_query(sql)          # raises if too expensive
    guard.run_query(sql)            # check + execute in one call

    # Standalone report
    python ingestion/bq_cost_guard.py --report
    python ingestion/bq_cost_guard.py --check "SELECT * FROM ..."

Guardrails applied:
    1. Dry-run every query before execution — get byte estimate
    2. Refuse any single query over SINGLE_QUERY_LIMIT_GB
    3. Track monthly cumulative usage in .bq_usage.json
    4. Refuse if monthly total would exceed MONTHLY_BUDGET_GB
    5. Warn at WARNING_THRESHOLD_GB
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# ── Budget thresholds ─────────────────────────────────────────────
FREE_TIER_GB         = 1000.0   # BigQuery free tier: 1 TB/month
MONTHLY_BUDGET_GB    = 800.0    # Stop at 800 GB (80% of free tier)
WARNING_THRESHOLD_GB = 600.0    # Warn at 600 GB (60% of free tier)
SINGLE_QUERY_LIMIT_GB = 50.0    # Refuse any single query over 50 GB

# ── Usage tracking file ───────────────────────────────────────────
USAGE_FILE = Path(__file__).resolve().parent.parent / ".bq_usage.json"

# ── Colours for terminal output ───────────────────────────────────
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


class BQCostGuard:
    """
    Tracks BigQuery query costs and enforces free-tier budget limits.
    All checks happen BEFORE query execution via dry-run.
    """

    def __init__(self, project_id: str = None):
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID")
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable not set.")
        self._usage = self._load_usage()

    # ── Usage persistence ─────────────────────────────────────────

    def _load_usage(self) -> dict:
        month_key = datetime.now().strftime("%Y-%m")
        if USAGE_FILE.exists():
            with open(USAGE_FILE) as f:
                data = json.load(f)
        else:
            data = {}
        if month_key not in data:
            data[month_key] = {"bytes_processed": 0, "query_count": 0, "queries": []}
        return data

    def _save_usage(self):
        with open(USAGE_FILE, "w") as f:
            json.dump(self._usage, f, indent=2)

    @property
    def _month_key(self) -> str:
        return datetime.now().strftime("%Y-%m")

    @property
    def monthly_bytes(self) -> int:
        return self._usage[self._month_key]["bytes_processed"]

    @property
    def monthly_gb(self) -> float:
        return self.monthly_bytes / 1e9

    @property
    def monthly_query_count(self) -> int:
        return self._usage[self._month_key]["query_count"]

    def _record_query(self, sql_preview: str, bytes_processed: int):
        month = self._usage[self._month_key]
        month["bytes_processed"] += bytes_processed
        month["query_count"]     += 1
        month["queries"].append({
            "timestamp":       datetime.now().isoformat(),
            "bytes_processed": bytes_processed,
            "gb_processed":    round(bytes_processed / 1e9, 4),
            "sql_preview":     sql_preview[:120],
        })
        self._save_usage()

    # ── Dry-run estimate ──────────────────────────────────────────

    def estimate_bytes(self, sql: str) -> int:
        """Dry-run a query and return estimated bytes processed. No cost incurred."""
        from google.cloud import bigquery
        client = bigquery.Client(project=self.project_id)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=job_config)
        return job.total_bytes_processed

    # ── Pre-flight check ──────────────────────────────────────────

    def check_query(self, sql: str, label: str = "query") -> int:
        """
        Dry-run the query and enforce all budget limits.
        Returns estimated bytes if approved.
        Raises ValueError if any limit would be exceeded.
        """
        print(f"\n{CYAN}[BQ Cost Guard]{RESET} Checking: {label}")
        print(f"  Monthly usage so far : {self.monthly_gb:.2f} GB / {MONTHLY_BUDGET_GB} GB budget")

        # ── Dry run ───────────────────────────────────────────────
        estimated_bytes = self.estimate_bytes(sql)
        estimated_gb    = estimated_bytes / 1e9

        print(f"  This query estimates : {estimated_gb:.3f} GB")
        print(f"  Would bring total to : {self.monthly_gb + estimated_gb:.2f} GB")

        # ── Check 1: Single query limit ───────────────────────────
        if estimated_gb > SINGLE_QUERY_LIMIT_GB:
            raise ValueError(
                f"\n{RED}{BOLD}[BLOCKED]{RESET}{RED} Single query limit exceeded.\n"
                f"  Estimated: {estimated_gb:.2f} GB\n"
                f"  Limit    : {SINGLE_QUERY_LIMIT_GB} GB\n"
                f"  Fix      : Add WHERE clause, LIMIT, or date partition filter.{RESET}"
            )

        # ── Check 2: Monthly budget ───────────────────────────────
        projected_total = self.monthly_gb + estimated_gb
        if projected_total > MONTHLY_BUDGET_GB:
            raise ValueError(
                f"\n{RED}{BOLD}[BLOCKED]{RESET}{RED} Monthly budget would be exceeded.\n"
                f"  Current usage : {self.monthly_gb:.2f} GB\n"
                f"  This query    : {estimated_gb:.2f} GB\n"
                f"  Would total   : {projected_total:.2f} GB\n"
                f"  Budget        : {MONTHLY_BUDGET_GB} GB\n"
                f"  Resets        : {datetime.now().strftime('%Y-%m')}-01 next month.{RESET}"
            )

        # ── Warning threshold ─────────────────────────────────────
        if projected_total > WARNING_THRESHOLD_GB:
            print(
                f"  {YELLOW}[WARNING]{RESET} Approaching budget limit.\n"
                f"  Projected total: {projected_total:.2f} GB / {MONTHLY_BUDGET_GB} GB"
            )
        else:
            print(f"  {GREEN}[APPROVED]{RESET} Query within budget.")

        return estimated_bytes

    # ── Execute with tracking ─────────────────────────────────────

    def run_query(self, sql: str, label: str = "query") -> "pd.DataFrame":
        """
        Check budget, then execute the query and track usage.
        Returns a pandas DataFrame.
        """
        import pandas as pd
        from google.cloud import bigquery

        estimated_bytes = self.check_query(sql, label)

        print(f"  Executing...")
        client = bigquery.Client(project=self.project_id)
        result = client.query(sql).to_dataframe()

        self._record_query(sql, estimated_bytes)
        print(f"  {GREEN}Done.{RESET} {len(result):,} rows returned. Usage recorded.")

        return result

    # ── Usage report ──────────────────────────────────────────────

    def report(self):
        """Print a full usage report for the current month."""
        month = self._month_key
        used_gb   = self.monthly_gb
        budget_gb = MONTHLY_BUDGET_GB
        pct       = (used_gb / budget_gb * 100) if budget_gb > 0 else 0

        bar_len  = 40
        filled   = int(bar_len * pct / 100)
        bar_char = "█" * filled + "░" * (bar_len - filled)
        colour   = RED if pct > 80 else (YELLOW if pct > 60 else GREEN)

        print(f"\n{BOLD}BigQuery Usage Report — {month}{RESET}")
        print(f"{'─' * 50}")
        print(f"  Used    : {used_gb:.3f} GB")
        print(f"  Budget  : {budget_gb:.0f} GB  (free tier: {FREE_TIER_GB:.0f} GB/month)")
        print(f"  Buffer  : {budget_gb - used_gb:.1f} GB remaining before block")
        print(f"  Queries : {self.monthly_query_count}")
        print(f"\n  {colour}{bar_char}{RESET}  {pct:.1f}%")

        # Recent queries
        queries = self._usage[month].get("queries", [])
        if queries:
            print(f"\n  Last {min(5, len(queries))} queries:")
            for q in queries[-5:]:
                ts = q["timestamp"][:16]
                gb = q["gb_processed"]
                preview = q["sql_preview"][:60]
                print(f"    {ts}  {gb:.4f} GB  {preview}...")

        print(f"\n  {CYAN}Tip:{RESET} Always use WHERE hire_date >= ... to leverage partitioning.")
        print(f"  {CYAN}Tip:{RESET} Add LIMIT to all dev/exploratory queries.")


# ── Safe query templates with built-in guardrails ────────────────

def safe_fact_rides_query(project: str, days: int = 30, limit: int = 500_000) -> str:
    """
    Returns a cost-safe query for fact_rides.
    Always includes a date window and row limit.
    """
    return f"""
        SELECT *
        FROM `{project}.citycycle_marts.fact_rides`
        WHERE hire_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        LIMIT {limit}
    """


def safe_station_imbalance_query(project: str, days: int = 7) -> str:
    """Aggregated station imbalance — small result, safe to run frequently."""
    return f"""
        SELECT
            start_station_id,
            start_station_name,
            start_lat,
            start_lon,
            AVG(start_station_imbalance_score)  AS avg_imbalance_score,
            COUNT(*)                             AS ride_count
        FROM `{project}.citycycle_marts.fact_rides`
        WHERE hire_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY 1, 2, 3, 4
        ORDER BY avg_imbalance_score DESC
        LIMIT 795
    """


def safe_hourly_demand_query(project: str, days: int = 30) -> str:
    """Hourly demand aggregation — always aggregated, never full scan."""
    return f"""
        SELECT
            start_hour,
            day_of_week,
            is_weekend,
            COUNT(*)                    AS ride_count,
            AVG(duration_minutes)       AS avg_duration_mins
        FROM `{project}.citycycle_marts.fact_rides`
        WHERE hire_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY 1, 2, 3
        ORDER BY 1, 2
    """


# ── CLI ───────────────────────────────────────────────────────────

def main():
    import argparse
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="BigQuery cost guard")
    parser.add_argument("--report",  action="store_true", help="Show monthly usage report")
    parser.add_argument("--check",   type=str, metavar="SQL", help="Dry-run a SQL query")
    parser.add_argument("--reset",   action="store_true", help="Reset monthly usage counter")
    args = parser.parse_args()

    guard = BQCostGuard()

    if args.reset:
        month = datetime.now().strftime("%Y-%m")
        guard._usage[month] = {"bytes_processed": 0, "query_count": 0, "queries": []}
        guard._save_usage()
        print(f"Usage counter reset for {month} ✓")

    elif args.check:
        try:
            bytes_est = guard.check_query(args.check, label="CLI check")
            print(f"\nApproved. Estimated: {bytes_est/1e9:.3f} GB")
        except ValueError as e:
            print(e)
            sys.exit(1)

    else:
        guard.report()


if __name__ == "__main__":
    main()
