from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import pydeck as pdk
import streamlit as st

from src.build_miso_snapshot import main as build_miso_snapshot_main

# Local imports (so we can refresh snapshot inside the app)
from src.build_miso_snapshot import main as build_miso_snapshot_main

ROOT = Path(__file__).resolve().parents[1]
GEOJSON_PATH = ROOT / "data" / "raw" / "boundaries" / "miso_market.geojson"

LOAD_CSV = ROOT / "data" / "processed" / "miso_rt" / "total_load_latest.csv"
FUEL_CSV = ROOT / "data" / "processed" / "miso_rt" / "fuel_mix_latest.csv"


def load_geojson(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data(show_spinner=False)
def read_csv_cached(path: Path) -> pd.DataFrame:
    try:
        if not path.exists():
            return pd.DataFrame()
        # Avoid pandas EmptyDataError on 0-byte files
        if path.stat().st_size == 0:
            return pd.DataFrame()
        return pd.read_csv(path)
    except Exception:
        # Any parsing issue -> treat as "no data"
        return pd.DataFrame()


def refresh_snapshot(ttl: int = 60) -> None:
    # build_miso_snapshot_main reads argparse; easiest is to call it without args
    # because your cache already enforces TTL.
    build_miso_snapshot_main()


st.set_page_config(page_title="Energy + Weather Vol Forecast", layout="wide")
st.title("Energy × Weather × Volatility Forecast")

# Sidebar
st.sidebar.header("Data Controls")

if st.sidebar.button("Refresh MISO snapshot"):
    with st.spinner("Fetching latest MISO data (cached)…"):
        refresh_snapshot()
    # Clear cached reads so UI updates immediately
    read_csv_cached.clear()
    st.sidebar.success("Updated!")

st.sidebar.divider()
st.sidebar.header("Map Controls")

base_style = st.sidebar.selectbox(
    "Base map style",
    options=["Carto Positron (light)", "Carto Dark Matter (dark)", "No basemap"],
    index=0,
)
MAP_STYLES = {
    "Carto Positron (light)": "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    "Carto Dark Matter (dark)": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    "No basemap": None,
}

center_lat = st.sidebar.number_input("Center latitude", value=41.8, format="%.4f")
center_lon = st.sidebar.number_input("Center longitude", value=-90.3, format="%.4f")
zoom = st.sidebar.slider("Zoom", min_value=2, max_value=10, value=4, step=1)

show_polygons = st.sidebar.checkbox("Show MISO zone polygons", value=True)
geojson = load_geojson(GEOJSON_PATH) if show_polygons else None

# --- Top row: KPIs ---
load_df = read_csv_cached(LOAD_CSV)
fuel_df = read_csv_cached(FUEL_CSV)

k1, k2, k3 = st.columns(3)

with k1:
    st.subheader("Latest Total Load")
    if not load_df.empty:
        # try common columns: "Time" and "Load"
        if "Load" in load_df.columns:
            latest_load = pd.to_numeric(load_df["Load"], errors="coerce").dropna()
            st.metric("MW (latest row)", f"{latest_load.iloc[-1]:,.0f}" if len(latest_load) else "N/A")
        else:
            st.write("Load column not found.")
    else:
        st.caption("Run the snapshot refresh to generate data.")

with k2:
    st.subheader("Fuel Mix Snapshot")
    if not fuel_df.empty:
        if "CATEGORY" in fuel_df.columns and "ACT" in fuel_df.columns:
            total_act = pd.to_numeric(fuel_df["ACT"], errors="coerce").sum()
            st.metric("Total ACT", f"{total_act:,.0f}")
        else:
            st.write("Expected columns not found.")
    else:
        st.caption("Run the snapshot refresh to generate data.")

with k3:
    st.subheader("Data Files")
    st.write("Load CSV exists:", LOAD_CSV.exists())
    st.write("Fuel CSV exists:", FUEL_CSV.exists())

st.divider()

# --- Middle row: Map + Tables/Charts ---
left, right = st.columns([1.25, 1])

with left:
    st.subheader("MISO Footprint Map")

    layers = []
    tooltip = None

    if geojson is not None:
        layers.append(
            pdk.Layer(
                "GeoJsonLayer",
                geojson,
                pickable=True,
                stroked=True,
                filled=True,
                auto_highlight=True,
                opacity=0.25,
                get_fill_color=[80, 160, 255, 80],
                get_line_color=[0, 0, 0, 140],
                line_width_min_pixels=1,
            )
        )
        tooltip = {
            "html": "<b>Zone:</b> {Zone} <br/> <b>Hub:</b> {Hub}",
            "style": {"backgroundColor": "white", "color": "black"},
        }
    else:
        st.info("No MISO GeoJSON found. (Step 3 should create it.)")

    view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom)
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style=MAP_STYLES[base_style],
        tooltip=tooltip,
    )
    st.pydeck_chart(deck, use_container_width=True)

with right:
    st.subheader("Fuel Mix (table + chart)")
    if fuel_df.empty:
        st.caption("No fuel mix data yet. Click “Refresh MISO snapshot”.")
    else:
        st.dataframe(fuel_df, use_container_width=True, height=260)

        if "CATEGORY" in fuel_df.columns and "ACT" in fuel_df.columns:
            chart_df = fuel_df.copy()
            chart_df["ACT"] = pd.to_numeric(chart_df["ACT"], errors="coerce")
            chart_df = chart_df.dropna(subset=["ACT"])
            st.bar_chart(chart_df.set_index("CATEGORY")["ACT"])
        else:
            st.caption("Fuel mix chart requires CATEGORY and ACT columns.")

st.divider()
st.subheader("Total Load (table)")
if load_df.empty:
    st.caption("No load data yet. Click “Refresh MISO snapshot”.")
else:
    st.dataframe(load_df.tail(50), use_container_width=True, height=260)
