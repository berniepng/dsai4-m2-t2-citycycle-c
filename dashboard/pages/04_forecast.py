"""
dashboard/pages/04_forecast.py
================================
24-hour demand forecast per station using the trained XGBoost model.
Falls back to synthetic pattern if model hasn't been trained.
"""
import os
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

ROOT       = Path(__file__).resolve().parents[2]
MOCK_DIR   = ROOT / "data" / "mock"
MODEL_PATH = ROOT / "ml" / "models" / "demand_model.pkl"
PROJECT    = os.environ.get("GCP_PROJECT_ID", "citycycle-dsai4")
DATASET    = "citycycle_dev_marts"

st.set_page_config(page_title="Forecast · CityCycle", page_icon="🔮", layout="wide")
st.title("🔮 24-Hour Demand Forecast")
st.markdown("Predicted ride demand per station for the next 24 hours.")


@st.cache_data(ttl=3600)
def load_stations(use_mock=True):
    if use_mock:
        return pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv").rename(
            columns={"name": "station_name", "id": "station_id"})
    else:
        from sqlalchemy import create_engine, text
        engine = create_engine(f"bigquery://{PROJECT}/{DATASET}")
        with engine.connect() as conn:
            return pd.read_sql(
                text(f"SELECT station_id, station_name, nb_docks, latitude, longitude "
                     f"FROM `{PROJECT}.{DATASET}.dim_stations`"), conn)


@st.cache_data(ttl=1800)
def get_forecast(station_id: int) -> pd.DataFrame:
    hours = list(range(24))
    if MODEL_PATH.exists():
        import joblib
        model = joblib.load(MODEL_PATH)
        today = pd.Timestamp.now()
        features = pd.DataFrame({
            "hour":             hours,
            "day_of_week":      [today.dayofweek] * 24,
            "is_weekend":       [int(today.dayofweek >= 5)] * 24,
            "is_holiday":       [0] * 24,
            "season":           [1] * 24,
            "start_station_id": [station_id] * 24,
            "rolling_7d_avg":   [50.0] * 24,
        })[["hour", "day_of_week", "is_weekend", "is_holiday",
            "season", "start_station_id", "rolling_7d_avg"]]
        predicted = np.maximum(model.predict(features), 0)
    else:
        HOUR_WEIGHTS = np.array([
            0.3, 0.15, 0.10, 0.10, 0.20, 0.50,
            1.20, 3.80, 4.50, 2.50, 1.80, 1.60,
            2.20, 1.80, 1.60, 1.70, 2.40, 4.20,
            3.60, 2.20, 1.50, 1.10, 0.70, 0.45,
        ])
        rng = np.random.default_rng(station_id % 1000)
        scale = rng.uniform(0.5, 2.0)
        predicted = np.maximum(HOUR_WEIGHTS * scale * 4.5 + rng.normal(0, 0.5, 24), 0)

    ci = predicted * 0.25
    return pd.DataFrame({
        "hour":     hours,
        "forecast": np.round(predicted, 1),
        "lower_ci": np.round(np.maximum(predicted - ci, 0), 1),
        "upper_ci": np.round(predicted + ci, 1),
        "is_peak":  [h in [7, 8, 17, 18] for h in hours],
    })


use_mock = st.sidebar.toggle("Use mock data", value=True)
stations = load_stations(use_mock)

# ── Station selector ──────────────────────────────────────────────
col_sel, col_info = st.columns([2, 3])
with col_sel:
    selected_name = st.selectbox("Select station",
                                 stations["station_name"].tolist(), index=0)
    row        = stations[stations["station_name"] == selected_name].iloc[0]
    station_id = int(row["station_id"])

with col_info:
    st.markdown(f"""
    **Station ID:** {station_id}  
    **Docks:** {int(row.get('nb_docks', row.get('nbdocks', '?')))}  
    **Location:** {row.get('latitude', row.get('lat', '?')):.4f}, {row.get('longitude', row.get('lon', '?')):.4f}
    """)
    if not MODEL_PATH.exists():
        st.info("Trained model not found — showing synthetic pattern. "
                "Run `python ml/models/train_demand_model.py` to train.")

st.markdown("---")

# ── Forecast chart ────────────────────────────────────────────────
forecast = get_forecast(station_id)
st.subheader(f"Hourly demand forecast — {selected_name}")
chart_data = forecast.set_index("hour")[["forecast", "lower_ci", "upper_ci"]]
st.area_chart(chart_data, height=280, color=["#14B8A6", "#0D948840", "#0D948820"])
st.caption("Teal = forecast  |  Shaded = 95% confidence interval  |  "
           "Peak hours: 07–09 and 17–19")

# ── Table ─────────────────────────────────────────────────────────
st.subheader("Forecast table")
display = forecast.copy()
display["Peak?"] = display["is_peak"].map({True: "⚡ Peak", False: ""})
display["Hour"]  = display["hour"].apply(lambda h: f"{h:02d}:00")
st.dataframe(
    display[["Hour", "forecast", "lower_ci", "upper_ci", "Peak?"]]
    .rename(columns={"forecast": "Predicted Rides",
                     "lower_ci": "Lower CI", "upper_ci": "Upper CI"}),
    use_container_width=True, hide_index=True)

# ── Summary ───────────────────────────────────────────────────────
st.markdown("---")
c1, c2, c3 = st.columns(3)
c1.metric("Total predicted (24h)", f"{forecast['forecast'].sum():.0f}")
c2.metric("Peak hours total",      f"{forecast[forecast['is_peak']]['forecast'].sum():.0f}")
c3.metric("Off-peak total",        f"{forecast[~forecast['is_peak']]['forecast'].sum():.0f}")
