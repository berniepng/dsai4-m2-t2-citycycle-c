"""
dashboard/pages/02_station_map.py
===================================
Geospatial map of all CityCycle docking stations.
- Top: pydeck 3D scatter map (interactive, colour-coded by urgency)
- Bottom: folium map (detailed, with popups, clustering, layer control)

Install: pip install pydeck folium streamlit-folium
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

st.set_page_config(page_title="Station Map · CityCycle", page_icon="🗺", layout="wide")
st.title("🗺 Station Map")
st.markdown("All docking stations, colour-coded by rebalancing priority.")

# ── Dependency checks ─────────────────────────────────────────────
try:
    import pydeck as pdk
    HAS_PYDECK = True
except ImportError:
    HAS_PYDECK = False

try:
    import folium
    from folium.plugins import MarkerCluster, HeatMap
    from streamlit_folium import st_folium
    HAS_FOLIUM = True
except ImportError:
    HAS_FOLIUM = False

# ── Data loader ───────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_station_map_data(use_mock=True) -> pd.DataFrame:
    if use_mock:
        stations = pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")
        rides    = pd.read_csv(MOCK_DIR / "cycle_hire_mock.csv",
                               parse_dates=["start_date"])

        departures = rides.groupby("start_station_id").size().reset_index(name="departures")
        arrivals   = rides.groupby("end_station_id").size().reset_index(name="arrivals")
        arrivals   = arrivals.rename(columns={"end_station_id": "start_station_id"})

        flow = departures.merge(arrivals, on="start_station_id", how="outer").fillna(0)
        flow["net_flow"]     = flow["departures"] - flow["arrivals"]
        flow["total_moves"]  = flow["departures"] + flow["arrivals"]
        flow["imb_score"]    = flow["net_flow"].abs() / flow["total_moves"].clip(lower=1)
        flow["imb_direction"] = np.where(
            flow["net_flow"] > 0, "draining",
            np.where(flow["net_flow"] < 0, "filling", "balanced")
        )

        df = stations.merge(
            flow.rename(columns={"start_station_id": "id"}),
            on="id", how="left"
        ).fillna({"imb_score": 0, "departures": 0, "arrivals": 0,
                   "net_flow": 0, "imb_direction": "balanced"})

        df["priority"] = pd.cut(
            df["imb_score"],
            bins=[-0.01, 0.1, 0.3, 0.5, 1.01],
            labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        ).astype(str)

    else:
        from sqlalchemy import create_engine, text
        engine = create_engine(f"bigquery://{PROJECT}/{DATASET}")
        sql = f"""
            SELECT
                station_id  AS id,
                station_name AS name,
                zone,
                latitude,
                longitude,
                nb_docks    AS nbdocks,
                rebalancing_priority            AS priority,
                avg_imbalance_score_7d          AS imb_score,
                total_departures_all_time       AS departures,
                total_arrivals_all_time         AS arrivals
            FROM `{PROJECT}.{DATASET}.dim_stations`
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        df["net_flow"]      = df["departures"] - df["arrivals"]
        df["imb_direction"] = np.where(df["net_flow"] > 0, "draining",
                              np.where(df["net_flow"] < 0, "filling", "balanced"))

    # ── Shared colour helpers ─────────────────────────────────────
    def score_to_rgb(score):
        if score < 0.1:   return [34, 197, 94]
        elif score < 0.3: return [234, 179, 8]
        elif score < 0.5: return [249, 115, 22]
        else:             return [239, 68, 68]

    def score_to_hex(score):
        if score < 0.1:   return "#22C55E"
        elif score < 0.3: return "#EAB308"
        elif score < 0.5: return "#F97316"
        else:             return "#EF4444"

    df["colour"]     = df["imb_score"].apply(score_to_rgb)
    df["hex_colour"] = df["imb_score"].apply(score_to_hex)
    df["radius"]     = (df["imb_score"] * 200 + 60).clip(upper=300)

    return df


# ── Toggle ────────────────────────────────────────────────────────
use_mock = st.sidebar.toggle("Use mock data", value=True)
df = load_station_map_data(use_mock)

# ── Sidebar filters ───────────────────────────────────────────────
with st.sidebar:
    st.subheader("Filters")
    priority_filter = st.multiselect(
        "Priority",
        options=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        default=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
    )
    direction_filter = st.multiselect(
        "Imbalance direction",
        options=["draining", "filling", "balanced"],
        default=["draining", "filling", "balanced"],
    )

filtered = df[
    df["priority"].isin(priority_filter) &
    df["imb_direction"].isin(direction_filter)
]

# ── Summary stats ─────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Stations",    len(df))
c2.metric("Filtered Stations", len(filtered))
c3.metric("Critical",
          len(df[df["priority"] == "CRITICAL"]),
          delta="need urgent attention", delta_color="inverse")
c4.metric("Draining Now",
          len(df[df["imb_direction"] == "draining"]))

st.markdown("---")

# ════════════════════════════════════════════════════════════════
# MAP 1 — pydeck (3D interactive)
# ════════════════════════════════════════════════════════════════
st.subheader("Interactive 3D View — pydeck")

if HAS_PYDECK and len(filtered) > 0:
    view = pdk.ViewState(
        latitude=filtered["latitude"].mean(),
        longitude=filtered["longitude"].mean(),
        zoom=11,
        pitch=30,
    )
    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=filtered,
        get_position=["longitude", "latitude"],
        get_color="colour",
        get_radius="radius",
        pickable=True,
        opacity=0.85,
        stroked=True,
        line_width_min_pixels=1,
    )
    tooltip = {
        "html": """
            <b>{name}</b><br/>
            Priority: <b>{priority}</b><br/>
            Direction: {imb_direction}<br/>
            Net flow: {net_flow:.0f}<br/>
            Imbalance score: {imb_score:.2f}
        """,
        "style": {
            "backgroundColor": "#1E293B",
            "color": "#F8FAFC",
            "fontSize": "13px",
            "padding": "8px",
        }
    }
    deck = pdk.Deck(
        layers=[scatter_layer],
        initial_view_state=view,
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/light-v11",
    )
    st.pydeck_chart(deck, use_container_width=True)
    st.caption("🟢 LOW  🟡 MEDIUM  🟠 HIGH  🔴 CRITICAL  |  Radius = imbalance severity  |  Click a station for details")
elif not HAS_PYDECK:
    st.info("Install pydeck for the 3D map: `pip install pydeck`")
    st.map(filtered[["latitude","longitude"]].rename(
        columns={"latitude":"lat","longitude":"lon"}))
else:
    st.info("No stations match the current filters.")

st.markdown("---")

# ════════════════════════════════════════════════════════════════
# MAP 2 — folium (detailed, with popups + clustering)
# ════════════════════════════════════════════════════════════════
st.subheader("Detailed Station Map — folium")
st.markdown(
    "Click any marker for full station details. "
    "Toggle layers using the control in the top-right corner. "
    "Markers cluster automatically when zoomed out."
)

if not HAS_FOLIUM:
    st.warning(
        "Install folium and streamlit-folium for this map:\n\n"
        "```\npip install folium streamlit-folium\n```"
    )
else:
    centre_lat = filtered["latitude"].mean() if len(filtered) > 0 else 51.508
    centre_lon = filtered["longitude"].mean() if len(filtered) > 0 else -0.128

    m = folium.Map(
        location=[centre_lat, centre_lon],
        zoom_start=12,
        tiles=None,
    )

    # ── Tile layers ───────────────────────────────────────────────
    folium.TileLayer(tiles="CartoDB positron",
                     name="Light (CartoDB)", control=True).add_to(m)
    folium.TileLayer(tiles="OpenStreetMap",
                     name="OpenStreetMap", control=True).add_to(m)

    # ── Layer: clustered markers ──────────────────────────────────
    cluster_group  = folium.FeatureGroup(name="Station Clusters", show=True)
    marker_cluster = MarkerCluster(
        options={"maxClusterRadius": 40, "disableClusteringAtZoom": 15}
    ).add_to(cluster_group)

    for _, row in filtered.iterrows():
        icon_colour = {"LOW": "green", "MEDIUM": "orange",
                       "HIGH": "red", "CRITICAL": "darkred"}.get(row["priority"], "gray")
        icon_symbol = {"draining": "arrow-up", "filling": "arrow-down",
                       "balanced": "pause"}.get(row["imb_direction"], "info-sign")

        docks = int(row.get("nbdocks", row.get("nb_docks", row.get("docks_count", 0))))

        popup_html = f"""
        <div style="font-family: Arial, sans-serif; min-width: 200px;">
            <h4 style="margin:0 0 8px 0; color:#0F172A;">{row['name']}</h4>
            <table style="width:100%; font-size:12px; border-collapse:collapse;">
                <tr style="background:#F1F5F9;">
                    <td style="padding:4px 6px;"><b>Priority</b></td>
                    <td style="padding:4px 6px; color:{row['hex_colour']};"><b>{row['priority']}</b></td>
                </tr>
                <tr>
                    <td style="padding:4px 6px;"><b>Direction</b></td>
                    <td style="padding:4px 6px;">{row['imb_direction'].title()}</td>
                </tr>
                <tr style="background:#F1F5F9;">
                    <td style="padding:4px 6px;"><b>Imbalance Score</b></td>
                    <td style="padding:4px 6px;">{row['imb_score']:.3f}</td>
                </tr>
                <tr>
                    <td style="padding:4px 6px;"><b>Net Flow</b></td>
                    <td style="padding:4px 6px;">{int(row['net_flow']):+d}</td>
                </tr>
                <tr style="background:#F1F5F9;">
                    <td style="padding:4px 6px;"><b>Departures</b></td>
                    <td style="padding:4px 6px;">{int(row['departures'])}</td>
                </tr>
                <tr>
                    <td style="padding:4px 6px;"><b>Arrivals</b></td>
                    <td style="padding:4px 6px;">{int(row['arrivals'])}</td>
                </tr>
                <tr style="background:#F1F5F9;">
                    <td style="padding:4px 6px;"><b>Docks</b></td>
                    <td style="padding:4px 6px;">{docks}</td>
                </tr>
                <tr>
                    <td style="padding:4px 6px;"><b>Station ID</b></td>
                    <td style="padding:4px 6px;">{int(row['id'])}</td>
                </tr>
            </table>
        </div>
        """
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=f"{row['name']} — {row['priority']}",
            icon=folium.Icon(color=icon_colour, icon=icon_symbol, prefix="glyphicon"),
        ).add_to(marker_cluster)

    cluster_group.add_to(m)

    # ── Layer: heatmap ────────────────────────────────────────────
    heat_group = folium.FeatureGroup(name="Imbalance Heatmap", show=False)
    heat_data  = filtered[filtered["imb_score"] > 0][
        ["latitude", "longitude", "imb_score"]].values.tolist()
    if heat_data:
        HeatMap(heat_data, min_opacity=0.3, max_zoom=18,
                radius=20, blur=15,
                gradient={"0.2": "#22C55E", "0.5": "#EAB308",
                          "0.7": "#F97316", "1.0": "#EF4444"}).add_to(heat_group)
    heat_group.add_to(m)

    # ── Layer: critical only ──────────────────────────────────────
    critical_group = folium.FeatureGroup(name="Critical Stations Only", show=False)
    for _, row in filtered[filtered["priority"] == "CRITICAL"].iterrows():
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=14, color="#EF4444",
            fill=True, fill_color="#EF4444", fill_opacity=0.6,
            popup=row["name"],
            tooltip=f"CRITICAL: {row['name']}",
        ).add_to(critical_group)
    critical_group.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    folium.plugins.MeasureControl(
        position="bottomleft", primary_length_unit="kilometers").add_to(m)

    st_folium(m, use_container_width=True, height=600, returned_objects=[])

    st.caption(
        "Layers: Station Clusters (default) · Imbalance Heatmap · Critical Stations Only  "
        "| Toggle via the layer control (top-right)  "
        "| Measure distances with the ruler (bottom-left)"
    )

st.markdown("---")

# ── Detailed table ────────────────────────────────────────────────
st.subheader(f"Station Details — {len(filtered)} stations")
display_cols = ["name", "priority", "imb_direction", "imb_score",
                "net_flow", "departures", "arrivals"]
if "nbdocks" in filtered.columns:
    display_cols.append("nbdocks")
elif "nb_docks" in filtered.columns:
    display_cols.append("nb_docks")
elif "docks_count" in filtered.columns:
    display_cols.append("docks_count")

st.dataframe(
    filtered[display_cols]
    .sort_values("imb_score", ascending=False)
    .rename(columns={
        "name":          "Station",
        "priority":      "Priority",
        "imb_direction": "Direction",
        "imb_score":     "Imbalance Score",
        "net_flow":      "Net Flow",
        "departures":    "Departures",
        "arrivals":      "Arrivals",
        "nbdocks":       "Docks",
        "nb_docks":      "Docks",
        "docks_count":   "Docks",
    }),
    use_container_width=True,
    hide_index=True,
    column_config={
        "Imbalance Score": st.column_config.ProgressColumn(
            "Imbalance Score", min_value=0, max_value=1, format="%.3f"
        ),
    }
)

if use_mock:
    st.caption("Data source: mock CSV — toggle off to query live BigQuery (dim_stations)")
else:
    st.caption(f"Data source: {PROJECT}.{DATASET}.dim_stations")
