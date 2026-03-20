"""
dashboard/pages/01_overview.py
================================
KPI summary and daily ride trend.
Reads from mock CSV or live BigQuery depending on toggle.
"""
import os
from pathlib import Path
import pandas as pd
import streamlit as st

ROOT     = Path(__file__).resolve().parents[2]
MOCK_DIR = ROOT / "data" / "mock"
PROJECT  = os.environ.get("GCP_PROJECT_ID", "citycycle-dsai4")
DATASET  = "citycycle_dev_marts"
# Fixed date range — dataset runs 2020-01-01 to 2023-01-15
DATE_FROM = "2020-01-01"
DATE_TO   = "2023-01-15"

st.set_page_config(page_title="Overview · CityCycle", page_icon="📊", layout="wide")
st.title("📊 Overview")
st.markdown("Daily KPI summary for CityCycle London operations.")


@st.cache_data(ttl=3600, show_spinner="Loading ride data...")
def load_rides(use_mock=True):
    if use_mock:
        df = pd.read_csv(MOCK_DIR / "cycle_hire_mock.csv",
                         parse_dates=["start_date", "end_date"])
        df["hire_date"]        = df["start_date"].dt.date
        df["duration_minutes"] = df["duration"] / 60
        df["start_hour"]       = df["start_date"].dt.hour
        df["is_weekend"]       = df["start_date"].dt.dayofweek >= 5
        df["peak_hour_flag"]   = df["start_hour"].isin([7, 8, 17, 18]).astype(int)
        return df
    else:
        from sqlalchemy import create_engine, text
        engine = create_engine(f"bigquery://{PROJECT}/{DATASET}")
        sql = f"""
            SELECT
                hire_date,
                start_station_id,
                start_station_name,
                duration_minutes,
                start_hour,
                CAST(is_weekend AS INT64)     AS is_weekend,
                CAST(peak_hour_flag AS INT64) AS peak_hour_flag,
                end_station_id
            FROM `{PROJECT}.{DATASET}.fact_rides`
            WHERE hire_date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
            LIMIT 500000
        """
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn)


@st.cache_data(ttl=3600)
def load_stations(use_mock=True):
    if use_mock:
        return pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")
    else:
        from sqlalchemy import create_engine, text
        engine = create_engine(f"bigquery://{PROJECT}/{DATASET}")
        with engine.connect() as conn:
            return pd.read_sql(
                text(f"SELECT * FROM `{PROJECT}.{DATASET}.dim_stations`"), conn)


use_mock = st.sidebar.toggle("Use mock data", value=True)
rides    = load_rides(use_mock)
stations = load_stations(use_mock)

# ── KPIs ──────────────────────────────────────────────────────────
total_rides  = len(rides)
avg_duration = rides["duration_minutes"].mean()
peak_pct     = rides["peak_hour_flag"].mean() * 100
n_stations   = len(stations)

daily = rides.groupby(["hire_date", "start_station_id"]).size().reset_index(name="departures")
arr   = (rides.groupby(["hire_date", "end_station_id"]).size()
         .reset_index(name="arrivals")
         .rename(columns={"end_station_id": "start_station_id"}))
net   = daily.merge(arr, on=["hire_date", "start_station_id"], how="outer").fillna(0)
net["imb"] = ((net["departures"] - net["arrivals"]).abs()
              / (net["departures"] + net["arrivals"]).clip(lower=1))
imbalanced_pct = (net["imb"] > 0.2).mean() * 100

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Rides",     f"{total_rides:,}")
c2.metric("Avg Duration",    f"{avg_duration:.1f} min")
c3.metric("Peak Hour Share", f"{peak_pct:.1f}%")
c4.metric("Active Stations", f"{n_stations}")
c5.metric("Imbalanced Rows", f"{imbalanced_pct:.1f}%",
          delta="target <20%", delta_color="inverse")

st.markdown("---")

# ── Daily trend ───────────────────────────────────────────────────
st.subheader("Daily Ride Volume")
dr = (rides.groupby("hire_date").size()
      .reset_index(name="rides").sort_values("hire_date"))
dr["hire_date"] = pd.to_datetime(dr["hire_date"])
st.line_chart(dr.set_index("hire_date")["rides"], height=260, color="#0D9488")

# ── Hourly demand ─────────────────────────────────────────────────
st.subheader("Hourly Demand Pattern")
hourly = rides.groupby("start_hour").size().reset_index(name="rides")
st.bar_chart(hourly.set_index("start_hour")["rides"], height=220, color="#0D9488")
st.caption("Peak hours: 07–09 and 17–19 (commuter double peak)")

# ── Top stations ──────────────────────────────────────────────────
st.subheader("Top 10 Departure Stations")
top = (rides.groupby("start_station_name").size()
       .reset_index(name="departures").nlargest(10, "departures"))
st.dataframe(top, use_container_width=True, hide_index=True)

if use_mock:
    st.caption("Data source: mock CSV — toggle off to query live BigQuery")
else:
    st.caption(f"Data source: {PROJECT}.{DATASET}.fact_rides ({DATE_FROM} → {DATE_TO})")
