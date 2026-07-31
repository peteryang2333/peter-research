"""Module 3 - Rotation: Relative Rotation Graph (RRG) of SPDR sectors vs SPY."""
from __future__ import annotations
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from data.feed import get_history

SECTORS = {
    "XLF": "Financials", "XLK": "Technology", "XLV": "Healthcare",
    "XLE": "Energy", "XLI": "Industrials", "XLB": "Materials",
    "XLC": "Comm. Services", "XLY": "Cons. Disc.", "XLP": "Cons. Staples",
    "XLU": "Utilities", "XLRE": "Real Estate",
}
TAIL = 8


def _weekly(ticker):
    h = get_history(ticker.lower(), "2Y")
    if not h:
        return None
    s = pd.Series({r["date"]: r["close"] for r in h}).sort_index()
    s.index = pd.to_datetime(s.index)
    s = s.resample("W").last().dropna()
    return s


def _rrg(sector_w, bench_w):
    joined = pd.concat([sector_w, bench_w], axis=1, join="inner")
    if joined.shape[0] < 50:
        return None
    rs = joined.iloc[:, 0] / joined.iloc[:, 1]
    rs_ratio = rs / rs.rolling(40).mean()
    rs_mom = rs_ratio / rs_ratio.shift(10)
    return rs_ratio, rs_mom


def render():
    st.header("03 · Rotation")
    st.caption("Where is money flowing? RS-Ratio vs RS-Momentum of SPDR sectors vs SPY "
               "(JdK Relative Rotation Graph). Clockwise = normal rotation.")

    bench = _weekly("spy")
    if bench is None:
        st.warning("Sector history unreachable — RRG needs live data. Showing sample layout.")
        _sample()
        return

    points = {}
    for sym in SECTORS:
        w = _weekly(sym)
        if w is None:
            continue
        rr = _rrg(w, bench)
        if rr is None:
            continue
        ratio, mom = rr
        x = ratio.iloc[-TAIL:].values * 100
        y = mom.iloc[-TAIL:].values * 100
        points[sym] = (x, y, ratio.iloc[-1] * 100, mom.iloc[-1] * 100)

    if not points:
        st.warning("Could not compute RRG from available data.")
        return

    fig = go.Figure()
    fig.add_vline(x=100, line_dash="dash", line_color="grey")
    fig.add_hline(y=100, line_dash="dash", line_color="grey")
    for sym, (x, y, fx, fy) in points.items():
        q = _quadrant(fx, fy)
        fig.add_trace(go.Scatter(
            x=x, y=y, mode="lines+markers", name=sym,
            line=dict(color=_qcolor(q), width=1.5),
            marker=dict(size=4, color=_qcolor(q)),
            hovertemplate=f"{sym} ({SECTORS[sym]})<extra></extra>"))
        fig.add_trace(go.Scatter(
            x=[fx], y=[fy], mode="markers+text", name=sym,
            marker=dict(size=12, color=_qcolor(q), line=dict(width=1, color="white")),
            text=[sym], textposition="top center",
            hovertemplate=f"{sym} {SECTORS[sym]}<br>RS {fx:.1f} Mom {fy:.1f}<extra></extra>"))
    fig.update_layout(height=560, showlegend=False,
                      xaxis_title="RS-Ratio (100=benchmark)",
                      yaxis_title="RS-Momentum (100=benchmark)",
                      margin=dict(l=0, r=0, t=10, b=0))
    fig.add_annotation(x=100, y=max(y.max() for x, y, _, _ in points.values()) * 1.05 if False else 130,
                       text="Leading", showarrow=False, font=dict(color="green", size=11))
    st.plotly_chart(fig, width='stretch')

    rows = []
    for sym, (_, _, fx, fy) in points.items():
        rows.append({"Sector": f"{sym} {SECTORS[sym]}", "RS-Ratio": round(fx, 1),
                     "RS-Mom": round(fy, 1), "Quadrant": _quadrant(fx, fy)})
    st.dataframe(pd.DataFrame(rows).sort_values("RS-Ratio", ascending=False),
                 width='stretch', hide_index=True)


def _quadrant(x, y):
    if x >= 100 and y >= 100:
        return "Leading"
    if x >= 100 and y < 100:
        return "Weakening"
    if x < 100 and y < 100:
        return "Lagging"
    return "Improving"


def _qcolor(q):
    return {"Leading": "#2ca02c", "Weakening": "#bcbd22",
            "Lagging": "#d62728", "Improving": "#1f77b4"}[q]


def _sample():
    st.info("RRG sample layout: Leading (XLK, XLC), Weakening (XLY), "
            "Improving (XLF, XLI), Lagging (XLU, XLRE). Connect live data for real trails.")
