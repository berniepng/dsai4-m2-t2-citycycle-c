"""
dashboard/pages/03_rebalancing.py
===================================
Ranked rebalancing intervention list.
Draining stations need bikes delivered; filling stations need bikes collected.
"""
import os
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

ROOT     = Path(__file__).resolve().parents[2]
MOCK_DIR = ROOT / "data" / "mock"
PROJECT  = os.environ.get("GCP_PROJECT_ID", "citycycle-dsai4")
DATASET  = "citycycle_dev_marts"
DATE_FROM = "2020-01-01"
DATE_TO   = "2023-01-15"

st.set_page_config(page_title="Rebalancing · CityCycle", page_icon="⚖️", layout="wide")
st.title("⚖️ Rebalancing Priority")
st.markdown(
    "Stations ranked by urgency. Draining = bikes needed. Filling = bikes to collect.")


@st.cache_data(ttl=1800)
def build_rebalancing(use_mock=True, date_from=DATE_FROM, date_to=DATE_TO):
    if use_mock:
        stations = pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")
        rides    = pd.read_csv(MOCK_DIR / "cycle_hire_mock.csv",
                               parse_dates=["start_date"])
        dep = rides.groupby("start_station_id").size().reset_index(name="departures")
        arr = (rides.groupby("end_station_id").size()
               .reset_index(name="arrivals")
               .rename(columns={"end_station_id": "start_station_id"}))
        flow = dep.merge(arr, on="start_station_id", how="outer").fillna(0)
        flow["net_flow"]   = flow["departures"] - flow["arrivals"]
        flow["total"]      = flow["departures"] + flow["arrivals"]
        flow["imb_score"]  = flow["net_flow"].abs() / flow["total"].clip(lower=1)
        df = stations.merge(
            flow.rename(columns={"start_station_id": "id"}),
            on="id", how="left").fillna(0)
        df["action"] = np.where(df["net_flow"] > 2, "DELIVER BIKES",
                       np.where(df["net_flow"] < -2, "COLLECT BIKES", "MONITOR"))
        df["priority"] = pd.cut(
            df["imb_score"],
            bins=[-0.01, 0.10, 0.18, 0.25, 1.01],
            labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        ).astype(str)
        df = df.rename(columns={"name": "station", "nbdocks": "nb_docks"})
    else:
        from sqlalchemy import create_engine, text
        engine = create_engine(f"bigquery://{PROJECT}/{DATASET}")
        sql = f"""
            SELECT
                start_station_name                              AS station,
                start_zone                                      AS zone,
                start_lat                                       AS lat,
                start_lon                                       AS lon,
                start_nb_docks                                  AS nb_docks,
                ROUND(AVG(start_station_imbalance_score), 3)   AS imb_score,
                ROUND(AVG(start_station_net_flow), 1)          AS net_flow,
                start_station_imbalance_direction               AS imb_direction,
                COUNT(*)                                        AS total_rides
            FROM `{PROJECT}.{DATASET}.fact_rides`
            WHERE hire_date BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY 1, 2, 3, 4, 5, 8
            HAVING COUNT(*) > 100
            ORDER BY imb_score DESC
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        df["action"] = np.where(df["net_flow"] > 0, "DELIVER BIKES",
                       np.where(df["net_flow"] < 0, "COLLECT BIKES", "MONITOR"))
        df["priority"] = pd.cut(
            df["imb_score"],
            bins=[-0.01, 0.10, 0.18, 0.25, 1.01],
            labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        ).astype(str)
    return df.sort_values("imb_score", ascending=False)


use_mock = st.sidebar.toggle("Use mock data", value=True)

if not use_mock:
    col_a, col_b = st.columns(2)
    date_from = col_a.text_input("From date", DATE_FROM)
    date_to   = col_b.text_input("To date",   DATE_TO)
else:
    date_from, date_to = DATE_FROM, DATE_TO

df = build_rebalancing(use_mock, date_from, date_to)

# ── Filters ───────────────────────────────────────────────────────
col_f1, col_f2 = st.columns(2)
action_filter   = col_f1.multiselect("Action",
    ["DELIVER BIKES", "COLLECT BIKES", "MONITOR"],
    default=["DELIVER BIKES", "COLLECT BIKES"])
priority_filter = col_f2.multiselect("Priority",
    ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
    default=["CRITICAL", "HIGH"])

filtered = df[df["action"].isin(action_filter) &
              df["priority"].isin(priority_filter)]

# ── KPIs ──────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Stations needing action", len(filtered))
c2.metric("Need delivery",  len(filtered[filtered["action"] == "DELIVER BIKES"]))
c3.metric("Need collection", len(filtered[filtered["action"] == "COLLECT BIKES"]))
c4.metric("Crew runs needed (~15/run)",
          max(1, round(len(filtered[filtered["priority"].isin(["CRITICAL","HIGH"])]) / 15)))
st.markdown("---")

# ── Table ─────────────────────────────────────────────────────────
display_cols = ["station", "priority", "action", "imb_score", "net_flow", "nb_docks"]
available = [c for c in display_cols if c in filtered.columns]
st.dataframe(
    filtered[available].rename(columns={
        "station": "Station", "priority": "Priority", "action": "Action",
        "imb_score": "Imbalance Score", "net_flow": "Net Flow", "nb_docks": "Docks"}),
    use_container_width=True, hide_index=True,
    column_config={"Imbalance Score": st.column_config.ProgressColumn(
        "Imbalance Score", min_value=0, max_value=1, format="%.3f")})

# ── Export ────────────────────────────────────────────────────────
csv = filtered[available].to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Export to CSV (ops crew routing)", csv,
                   "citycycle_rebalancing_plan.csv", "text/csv")
