from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

LAKEHOUSE_ROOT = Path("/app/lakehouse")
MART_REVENUE_PATH = LAKEHOUSE_ROOT / "mart" / "mart_revenue_hourly"
MART_FLEET_PATH = LAKEHOUSE_ROOT / "mart" / "mart_fleet_health_hourly"


def parquet_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return [p for p in path.glob("*.parquet") if p.is_file()]


def latest_file_time(path: Path) -> str:
    files = parquet_files(path)
    if not files:
        return "No files"
    latest = max(files, key=lambda p: p.stat().st_mtime)
    return datetime.fromtimestamp(latest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def load_parquet_table(path: Path) -> pd.DataFrame:
    files = parquet_files(path)
    if not files:
        return pd.DataFrame()
    return pd.read_parquet([str(p) for p in files])


st.set_page_config(page_title="Mobility Pipeline Dashboard", layout="wide")
st.title("Smart Mobility Pipeline Dashboard")
st.caption("Portfolio monitoring view for Gold/Mart outputs")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Revenue files", len(parquet_files(MART_REVENUE_PATH)))
col2.metric("Fleet files", len(parquet_files(MART_FLEET_PATH)))
col3.metric("Revenue latest file", latest_file_time(MART_REVENUE_PATH))
col4.metric("Fleet latest file", latest_file_time(MART_FLEET_PATH))

revenue_df = load_parquet_table(MART_REVENUE_PATH)
fleet_df = load_parquet_table(MART_FLEET_PATH)

st.subheader("Revenue KPI")
if revenue_df.empty:
    st.warning("No revenue mart data found yet.")
else:
    if "hour_start" in revenue_df.columns:
        revenue_df["hour_start"] = pd.to_datetime(revenue_df["hour_start"], errors="coerce")
        revenue_df = revenue_df.sort_values("hour_start")

    kpi1, kpi2 = st.columns(2)
    total_revenue = float(revenue_df.get("total_revenue", pd.Series(dtype=float)).fillna(0).sum())
    completed_rides = float(revenue_df.get("completed_rides", pd.Series(dtype=float)).fillna(0).sum())
    kpi1.metric("Total Revenue", f"${total_revenue:,.2f}")
    kpi2.metric("Completed Rides", f"{int(completed_rides):,}")

    if "hour_start" in revenue_df.columns and "total_revenue" in revenue_df.columns:
        chart_df = revenue_df[["hour_start", "total_revenue"]].dropna()
        chart_df = chart_df.set_index("hour_start")
        st.line_chart(chart_df)

    st.dataframe(revenue_df.tail(10), use_container_width=True)

st.subheader("Fleet Health KPI")
if fleet_df.empty:
    st.warning("No fleet mart data found yet.")
else:
    if "hour_start" in fleet_df.columns:
        fleet_df["hour_start"] = pd.to_datetime(fleet_df["hour_start"], errors="coerce")
        fleet_df = fleet_df.sort_values("hour_start")

    f1, f2, f3 = st.columns(3)
    avg_battery = float(fleet_df.get("avg_battery_level", pd.Series(dtype=float)).fillna(0).mean())
    low_battery = float(fleet_df.get("low_battery_scooters", pd.Series(dtype=float)).fillna(0).sum())
    in_maint = float(fleet_df.get("scooters_in_maintenance", pd.Series(dtype=float)).fillna(0).sum())
    f1.metric("Avg Battery Level", f"{avg_battery:.1f}%")
    f2.metric("Low Battery Scooters", f"{int(low_battery):,}")
    f3.metric("Maintenance Scooters", f"{int(in_maint):,}")

    if "hour_start" in fleet_df.columns and "avg_battery_level" in fleet_df.columns:
        fleet_chart_df = fleet_df[["hour_start", "avg_battery_level"]].dropna()
        fleet_chart_df = fleet_chart_df.set_index("hour_start")
        st.line_chart(fleet_chart_df)

    st.dataframe(fleet_df.tail(10), use_container_width=True)
