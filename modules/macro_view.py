"""Module 1 - Macro: global inflation / GDP heatmap (World Bank)."""
from __future__ import annotations
import streamlit as st
import pandas as pd
import plotly.express as px
from data.macro import inflation_latest, gdp_growth_latest

TIERS = [("Cool", 0, 2), ("Mild", 2, 5), ("Warm", 5, 10), ("Hot", 10, 20), ("Very hot", 20, 1e9)]
SAMPLE = {"TUR": 44.2, "IRN": 32.4, "EGY": 23.8, "CHN": 0.8, "THA": 1.1,
          "CHE": 1.2, "USA": 3.1, "DEU": 2.2, "IND": 5.6, "JPN": 2.8,
          "GBR": 3.4, "BRA": 4.6, "MEX": 4.7, "CAN": 2.9, "AUS": 3.6,
          "ZAF": 5.3, "RUS": 7.4, "IDN": 2.5, "ARG": 113.0, "KOR": 2.4,
          "ITA": 1.0, "ESP": 3.2, "SAU": 1.9, "NGA": 24.0, "FRA": 2.3}


def _tier(v: float) -> str:
    for name, lo, hi in TIERS:
        if lo <= v < hi:
            return name
    return "Very hot"


def render():
    st.header("01 · Macro")
    st.caption("GDP, inflation, housing rolled into one global heat map. Source: World Bank Open Data.")
    metric = st.radio("Metric", ["Inflation (CPI, annual %)", "GDP growth (annual %)"],
                      horizontal=True)
    if metric.startswith("Inflation"):
        data = inflation_latest() or SAMPLE
        note = "Live (World Bank)" if inflation_latest() else "Sample data"
    else:
        data = gdp_growth_latest() or SAMPLE
        note = "Live (World Bank)" if gdp_growth_latest() else "Sample data"

    if note.startswith("Sample"):
        st.warning("World Bank unreachable from this environment — showing sample data. "
                   "It populates live on your Oracle VM.")

    df = pd.DataFrame([(k, v) for k, v in data.items() if v is not None],
                      columns=["iso3", "value"])
    if df.empty:
        st.info("No macro data available.")
        return

    fig = px.choropleth(
        df, locations="iso3", locationmode="ISO-3", color="value",
        color_continuous_scale="YlGnBu", range_color=(0, max(20, df.value.max())),
        title=f"Global {metric.split('(')[0].strip()} — {note}",
        hover_name="iso3",
    )
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=460)
    st.plotly_chart(fig, width='stretch')

    ranked = df.sort_values("value", ascending=False)
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Hottest")
        for _, r in ranked.head(3).iterrows():
            st.metric(r["iso3"], f"{r['value']:.1f}%", help=_tier(r["value"]))
    with col2:
        st.subheader("Coolest")
        for _, r in ranked.tail(3).iloc[::-1].iterrows():
            st.metric(r["iso3"], f"{r['value']:.1f}%", help=_tier(r["value"]))
    st.caption("Tiers: " + " · ".join(f"{n} ({lo}–{hi if hi<1e9 else '∞'}%)"
                                      for n, lo, hi in TIERS))
