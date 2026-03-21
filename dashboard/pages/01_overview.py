"""
dashboard/pages/01_overview.py
================================
KPI summary and daily ride trend.
Reads from mock CSV or live BigQuery depending on toggle.

Live BQ mode uses pre-aggregated SQL — never pulls raw rows.
All KPIs computed in BigQuery, returning only summary data.
Full 32M ride dataset used with no row limit.
"""

import os
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
MOCK_DIR = ROOT / "data" / "mock"
PROJECT = os.environ.get("GCP_PROJECT_ID", "citycycle-dsai4")
DATASET = "citycycle_dev_marts"
DATE_FROM = "2020-01-01"
DATE_TO = "2023-01-15"

st.set_page_config(page_title="Overview · CityCycle", page_icon="📊", layout="wide")
st.title("📊 Overview")
st.markdown("Daily KPI summary for CityCycle London operations.")


def get_engine():
    from sqlalchemy import create_engine

    return create_engine(f"bigquery://{PROJECT}/{DATASET}")


# ── Live: single aggregated KPI query (no row limit needed) ───────
@st.cache_data(ttl=604800, show_spinner="Loading KPIs from BigQuery...")
def load_kpis_live():
    from sqlalchemy import text

    sql = f"""
        SELECT
            COUNT(*)                                                AS total_rides,
            ROUND(AVG(duration_minutes), 1)                        AS avg_duration,
            ROUND(AVG(CAST(peak_hour_flag AS INT64)) * 100, 1)     AS peak_pct,
            COUNT(DISTINCT start_station_id)                       AS n_stations,
            ROUND(
                SUM(CASE WHEN start_station_imbalance_score > 0.2
                    THEN 1 ELSE 0 END) / COUNT(*) * 100, 1
            )                                                      AS imbalanced_pct
        FROM `{PROJECT}.{DATASET}.fact_rides`
        WHERE hire_date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn).iloc[0]


# ── Live: daily trend (~1,100 rows grouped by date) ───────────────
@st.cache_data(ttl=604800, show_spinner="Loading daily trend...")
def load_daily_trend_live():
    from sqlalchemy import text

    sql = f"""
        SELECT
            hire_date,
            COUNT(*) AS rides
        FROM `{PROJECT}.{DATASET}.fact_rides`
        WHERE hire_date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
        GROUP BY hire_date
        ORDER BY hire_date
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(text(sql), conn)
    df["hire_date"] = pd.to_datetime(df["hire_date"])
    return df


# ── Live: hourly demand (24 rows) ─────────────────────────────────
@st.cache_data(ttl=604800, show_spinner="Loading hourly pattern...")
def load_hourly_live():
    from sqlalchemy import text

    sql = f"""
        SELECT
            start_hour,
            COUNT(*) AS rides
        FROM `{PROJECT}.{DATASET}.fact_rides`
        WHERE hire_date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
        GROUP BY start_hour
        ORDER BY start_hour
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


# ── Live: top 10 stations (10 rows) ──────────────────────────────
@st.cache_data(ttl=604800, show_spinner="Loading top stations...")
def load_top_stations_live():
    from sqlalchemy import text

    sql = f"""
        SELECT
            start_station_name,
            COUNT(*) AS departures
        FROM `{PROJECT}.{DATASET}.fact_rides`
        WHERE hire_date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
        GROUP BY start_station_name
        ORDER BY departures DESC
        LIMIT 10
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


# ── Mock data loaders ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_mock():
    df = pd.read_csv(
        MOCK_DIR / "cycle_hire_mock.csv", parse_dates=["start_date", "end_date"]
    )
    df["hire_date"] = df["start_date"].dt.date
    df["duration_minutes"] = df["duration"] / 60
    df["start_hour"] = df["start_date"].dt.hour
    df["is_weekend"] = df["start_date"].dt.dayofweek >= 5
    df["peak_hour_flag"] = df["start_hour"].isin([7, 8, 17, 18]).astype(int)
    return df


@st.cache_data(ttl=3600)
def load_stations_mock():
    return pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")


# ── Toggle ────────────────────────────────────────────────────────
use_mock = st.sidebar.toggle("Use mock data", value=True)

# ── KPIs ──────────────────────────────────────────────────────────
if use_mock:
    rides = load_mock()
    stations = load_stations_mock()
    total_rides = len(rides)
    avg_duration = rides["duration_minutes"].mean()
    peak_pct = rides["peak_hour_flag"].mean() * 100
    n_stations = len(stations)
    daily = (
        rides.groupby(["hire_date", "start_station_id"])
        .size()
        .reset_index(name="departures")
    )
    arr = (
        rides.groupby(["hire_date", "end_station_id"])
        .size()
        .reset_index(name="arrivals")
        .rename(columns={"end_station_id": "start_station_id"})
    )
    net = daily.merge(arr, on=["hire_date", "start_station_id"], how="outer").fillna(0)
    net["imb"] = (net["departures"] - net["arrivals"]).abs() / (
        net["departures"] + net["arrivals"]
    ).clip(lower=1)
    imbalanced_pct = (net["imb"] > 0.2).mean() * 100
else:
    kpis = load_kpis_live()
    total_rides = int(kpis["total_rides"])
    avg_duration = float(kpis["avg_duration"])
    peak_pct = float(kpis["peak_pct"])
    n_stations = int(kpis["n_stations"])
    imbalanced_pct = float(kpis["imbalanced_pct"])

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Rides", f"{total_rides:,}")
c2.metric("Avg Duration", f"{avg_duration:.1f} min")
c3.metric("Peak Hour Share", f"{peak_pct:.1f}%")
c4.metric("Active Stations", f"{n_stations}")
c5.metric(
    "Imbalanced Rows",
    f"{imbalanced_pct:.1f}%",
    delta="target <20%",
    delta_color="inverse",
)

st.markdown("---")

# ── Daily trend ───────────────────────────────────────────────────
st.subheader("Daily Ride Volume")
if use_mock:
    dr = (
        rides.groupby("hire_date")
        .size()
        .reset_index(name="rides")
        .sort_values("hire_date")
    )
    dr["hire_date"] = pd.to_datetime(dr["hire_date"])
else:
    dr = load_daily_trend_live()
st.line_chart(dr.set_index("hire_date")["rides"], height=260, color="#0D9488")

# ── Hourly demand ─────────────────────────────────────────────────
st.subheader("Hourly Demand Pattern")
if use_mock:
    hourly = rides.groupby("start_hour").size().reset_index(name="rides")
else:
    hourly = load_hourly_live()
st.bar_chart(hourly.set_index("start_hour")["rides"], height=220, color="#0D9488")
st.caption("Peak hours: 07–09 and 17–19 (commuter double peak)")

# ── Top stations ──────────────────────────────────────────────────
st.subheader("Top 10 Departure Stations")
if use_mock:
    top = (
        rides.groupby("start_station_name")
        .size()
        .reset_index(name="departures")
        .nlargest(10, "departures")
    )
else:
    top = load_top_stations_live()
st.dataframe(top, use_container_width=True, hide_index=True)

if use_mock:
    st.caption("Data source: mock CSV — toggle off to query live BigQuery")
else:
    st.caption(
        f"Data source: {PROJECT}.{DATASET}.fact_rides "
        f"({DATE_FROM} → {DATE_TO})  ·  All 32M rides aggregated in BigQuery"
    )
