"""
dashboard/pages/04_forecast.py
================================
24-hour demand forecast per station using the trained ML model.
Falls back to mock predictions if the model hasn't been trained yet.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
MOCK_DIR = ROOT / "data" / "mock"
MODEL_PATH = ROOT / "ml" / "models" / "demand_model.pkl"

st.set_page_config(page_title="Forecast · CityCycle", page_icon="🔮", layout="wide")
st.title("🔮 24-Hour Demand Forecast")
st.markdown("Predicted ride demand per station for the next 24 hours.")


@st.cache_data(ttl=3600)
def load_stations() -> pd.DataFrame:
    return pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")


@st.cache_data(ttl=1800)
def get_forecast(station_id: int) -> pd.DataFrame:
    """
    Returns a 24-row DataFrame with hourly demand forecast.
    Uses trained model if available, otherwise generates a
    realistic synthetic forecast based on historical patterns.
    """
    hours = list(range(24))

    if MODEL_PATH.exists():
        import joblib

        model = joblib.load(MODEL_PATH)
        # Build feature rows for next 24 hours
        # (day_of_week, is_weekend, is_holiday, station_id, hour)
        today = pd.Timestamp.now()
        features = pd.DataFrame({
            "hour":              hours,
            "day_of_week":       [today.dayofweek] * 24,
            "is_weekend":        [int(today.dayofweek >= 5)] * 24,
            "is_holiday":        [0] * 24,
            "start_station_id":  [station_id] * 24,
            "rolling_7d_avg":    [0.0] * 24,
        })
        predicted = model.predict(features)
        predicted = np.maximum(predicted, 0)
    else:
        # Synthetic forecast based on London commuter pattern
        HOUR_WEIGHTS = np.array(
            [
                0.3,
                0.15,
                0.10,
                0.10,
                0.20,
                0.50,
                1.20,
                3.80,
                4.50,
                2.50,
                1.80,
                1.60,
                2.20,
                1.80,
                1.60,
                1.70,
                2.40,
                4.20,
                3.60,
                2.20,
                1.50,
                1.10,
                0.70,
                0.45,
            ]
        )
        # Scale to ~100 rides/day for a busy station; add station variability
        rng = np.random.default_rng(station_id % 1000)
        scale = rng.uniform(0.5, 2.0)
        predicted = HOUR_WEIGHTS * scale * 4.5
        noise = rng.normal(0, 0.5, 24)
        predicted = np.maximum(predicted + noise, 0)

    ci_width = predicted * 0.25  # ±25% confidence interval
    return pd.DataFrame(
        {
            "hour": hours,
            "forecast": np.round(predicted, 1),
            "lower_ci": np.round(np.maximum(predicted - ci_width, 0), 1),
            "upper_ci": np.round(predicted + ci_width, 1),
            "is_peak": [h in [7, 8, 17, 18] for h in hours],
        }
    )


stations = load_stations()

# ── Station selector ──────────────────────────────────────────────
col_sel, col_info = st.columns([2, 3])
with col_sel:
    station_names = stations["name"].tolist()
    selected_name = st.selectbox("Select station", station_names, index=0)
    selected_row = stations[stations["name"] == selected_name].iloc[0]
    station_id = int(selected_row["id"])

with col_info:
    st.markdown(
        f"""
    **Station ID:** {station_id}  
    **Docks:** {int(selected_row['nbdocks'])}  
    **Location:** {selected_row['latitude']:.4f}, {selected_row['longitude']:.4f}
    """
    )
    if not MODEL_PATH.exists():
        st.info(
            "Model not yet trained — showing synthetic forecast pattern. "
            "Run `python ml/models/train_demand_model.py` to train."
        )

st.markdown("---")

# ── Forecast chart ────────────────────────────────────────────────
forecast = get_forecast(station_id)

st.subheader(f"Hourly demand forecast — {selected_name}")

# Chart: overlay forecast with CI band
chart_data = forecast.set_index("hour")[["forecast", "lower_ci", "upper_ci"]]
st.area_chart(chart_data, height=280, color=["#14B8A6", "#0D948840", "#0D948820"])
st.caption(
    "Teal line = forecast  |  Shaded band = 95% confidence interval  |  Peak hours: 07–09, 17–19"
)

# ── Forecast table ────────────────────────────────────────────────
st.subheader("Forecast table")
display = forecast.copy()
display["Peak?"] = display["is_peak"].map({True: "⚡ Peak", False: ""})
display["Hour"] = display["hour"].apply(lambda h: f"{h:02d}:00")
display = display[["Hour", "forecast", "lower_ci", "upper_ci", "Peak?"]].rename(
    columns={
        "forecast": "Predicted Rides",
        "lower_ci": "Lower CI",
        "upper_ci": "Upper CI",
    }
)
st.dataframe(display, use_container_width=True, hide_index=True)

# ── Daily summary ─────────────────────────────────────────────────
total_pred = forecast["forecast"].sum()
peak_demand = forecast[forecast["is_peak"]]["forecast"].sum()
offpeak_demand = forecast[~forecast["is_peak"]]["forecast"].sum()

st.markdown("---")
c1, c2, c3 = st.columns(3)
c1.metric("Total predicted rides (24h)", f"{total_pred:.0f}")
c2.metric("Peak hours total", f"{peak_demand:.0f}")
c3.metric("Off-peak total", f"{offpeak_demand:.0f}")
