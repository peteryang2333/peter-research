"""KovaView-OSS — self-hosted trading command center (6 modules).

Run:  streamlit run app.py --server.port 8501
Data: stockanalysis.com + api.nasdaq.com (market), World Bank (macro),
      IBKR Client Portal (optional, broker-verified). No Yahoo, no rate-limit.
"""
from __future__ import annotations
import os
import sys
import datetime as dt

# make project root importable
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st
from modules import macro_view, direction_view, rotation_view, kova_view, discipline_view, proof_view

st.set_page_config(page_title="KovaView-OSS", page_icon="📊", layout="wide")

CATALYST = [
    ("Tue", "CPI"), ("Wed", "FOMC"), ("Thu", "NVDA"), ("Fri", "Jobs"),
]

with st.sidebar:
    st.title("📊 KovaView-OSS")
    st.caption("See the market. Then act.")
    choice = st.radio("Module", [
        "Overview", "01 Macro", "02 Direction", "03 Rotation",
        "04 Kova Score", "05 Discipline", "06 Proof"])
    # allow ?page=macro|direction|rotation|kova|discipline|proof for headless smoke tests
    qp = st.query_params.get("page")
    if qp:
        _map = {"overview": "Overview", "macro": "01 Macro", "direction": "02 Direction",
                "rotation": "03 Rotation", "kova": "04 Kova Score",
                "discipline": "05 Discipline", "proof": "06 Proof"}
        choice = _map.get(str(qp), choice)
    st.divider()
    st.caption(f"Updated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M')} local")
    st.caption("Feeds: stockanalysis · nasdaq · World Bank · IBKR(optional)")

st.markdown("### " + " · ".join(f"**{d}** {e}" for d, e in CATALYST))
st.caption("Weekly catalyst strip (edit in app.py · CATALYST).")

if choice == "Overview":
    st.header("Overview")
    st.markdown(
        "A free, self-hostable take on KovaView's six modules. Pick a module on the left. "
        "All market data is pulled live from **stockanalysis.com** and **api.nasdaq.com** "
        "(no Yahoo rate-limit); macro from **World Bank**; portfolio verification optional via "
        "**Interactive Brokers** Client Portal.\n\n"
        "- **Macro** — global inflation/GDP heatmap\n"
        "- **Direction** — one posture score (trend/breadth/credit/vol/leadership)\n"
        "- **Rotation** — RRG sector-flow map vs SPY\n"
        "- **Kova Score** — transparent composite rank over your watchlist\n"
        "- **Discipline** — percent-risk sizer + trade journal (R multiples)\n"
        "- **Proof** — broker-verified ledger + leaderboard")
    st.info("Tip: set IBKR_BASE (e.g. http://host.docker.internal:5000) and authenticate the "
            "IBKR Client Portal Gateway to enable live broker verification.")
elif choice == "01 Macro":
    macro_view.render()
elif choice == "02 Direction":
    direction_view.render()
elif choice == "03 Rotation":
    rotation_view.render()
elif choice == "04 Kova Score":
    kova_view.render()
elif choice == "05 Discipline":
    discipline_view.render()
elif choice == "06 Proof":
    proof_view.render()
