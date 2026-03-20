"""
dashboard/app.py
=================
CityCycle London — Bike Rebalancing Intelligence Dashboard.
Multi-page Streamlit app.

Pages:
  01_overview.py    — KPI summary cards + daily trend chart
  02_station_map.py — Geospatial pydeck map of all 795 stations
  03_rebalancing.py — Ranked rebalancing priority list
  04_forecast.py    — 24-hour demand forecast per station

Run:
    streamlit run dashboard/app.py

Environment:
    GCP_PROJECT_ID   — your GCP project (for live BQ queries)
    USE_MOCK_DATA=1  — use local CSV instead of querying BQ (default in dev)
"""

import streamlit as st

st.set_page_config(
    page_title="CityCycle Rebalancing Intelligence",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🚲")
    st.markdown("## CityCycle London")
    st.markdown("**Bike Rebalancing Intelligence**")
    st.markdown("---")
    st.markdown(
        """
    **Navigation**
    - 📊 Overview
    - 🗺 Station Map
    - ⚖️ Rebalancing
    - 🔮 Forecast
    """
    )
    st.markdown("---")

    # Data source toggle
    use_mock = st.toggle(
        "Use mock data",
        value=True,
        help="ON = local CSV (free). OFF = live BigQuery query.",
    )
    if use_mock:
        st.success("Mock data — zero BQ cost")
    else:
        st.warning("Live BigQuery mode")

    st.markdown("---")
    st.caption("DSAI4 Module 2 · Team C")
    st.caption("bigquery-public-data.london_bicycles")

# ── Home page content ─────────────────────────────────────────────
st.title("🚲 CityCycle London — Rebalancing Intelligence")
st.markdown(
    """
Welcome to the CityCycle operational dashboard. Use the **sidebar** to navigate between views,
or use the **pages** in the left navigation.

| Page | What it shows |
|------|--------------|
| 📊 **Overview** | Daily KPIs: total rides, peak utilisation, imbalance rate |
| 🗺 **Station Map** | All 795 stations colour-coded by rebalancing urgency |
| ⚖️ **Rebalancing** | Ranked intervention list with predicted demand delta |
| 🔮 **Forecast** | 24-hour demand forecast per station from ML model |
"""
)

if use_mock:
    st.info(
        "Running on **mock data** (10,000 synthetic rides, 795 stations). "
        "Toggle 'Use mock data' OFF in the sidebar to query live BigQuery — "
        "ensure GCP_PROJECT_ID is set in your .env first.",
        icon="ℹ️",
    )
