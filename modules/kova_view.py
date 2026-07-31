"""Module 4 - Kova-style score: transparent composite rank over a watchlist.

Score = 0.45*momentum + 0.35*trend + 0.20*relative-volume (all 0-100).
Not a black box — every input is shown. EPS column is a proxy (12M return %)
because free feeds don't expose fundamentals; swap in a paid source if wanted.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import streamlit as st
from data.feed import get_history, get_quote

WATCHLIST = ["AMAT", "ALAB", "GLW", "TGTX", "CRDO", "FAS", "DDOG",
             "NVDA", "MSFT", "ANET", "SOXX", "XLK"]


def _clamp(x, lo=0, hi=100):
    return max(lo, min(hi, x))


def render():
    st.header("04 · Kova Score (open)")
    st.caption("Transparent composite: momentum + trend + relative volume. "
               "Edit the watchlist in the sidebar.")
    symbols = st.text_input("Watchlist (comma separated)",
                            ", ".join(WATCHLIST)).upper().replace(" ", "").split(",")
    symbols = [s for s in symbols if s]

    rows = []
    for sym in symbols:
        h = get_history(sym.lower(), "1Y")
        q = get_quote(sym)
        if not h:
            rows.append({"Symbol": sym, "Price": "n/a", "%Day": None,
                         "RelVol": None, "Score": None, "Health": "n/a",
                         "EPS≈12M%": None})
            continue
        p = np.array([r["close"] for r in h], dtype=float)
        ma50 = p[-50:].mean() if len(p) >= 50 else p.mean()
        ma200 = p[-200:].mean() if len(p) >= 200 else p.mean()
        ret3m = p[-1] / p[-63] - 1 if len(p) > 63 else 0
        ret12m = p[-1] / p[0] - 1
        mom = _clamp(50 + ret3m * 150)
        trend = 50 + (p[-1] > ma200) * 25 + (p[-1] > ma50) * 15 + (ma50 > ma200) * 10
        # relative volume
        vols = [r.get("volume") for r in h if r.get("volume")]
        if len(vols) > 20:
            relv = float(vols[-1]) / float(np.mean(vols[-21:-1])) if vols[-1] else 1.0
        else:
            relv = 1.0
        volc = _clamp(relv * 50)
        score = 0.45 * mom + 0.35 * trend + 0.20 * volc
        if p[-1] > ma200 and ma50 > ma200:
            health = "Healthy"
        elif p[-1] < ma200:
            health = "REDUCE"
        else:
            health = "Weak"
        rows.append({
            "Symbol": sym,
            "Price": f"{p[-1]:.2f}",
            "%Day": (q["pct"] if q and q["pct"] is not None else None),
            "RelVol": round(relv, 2),
            "Score": round(score, 0),
            "Health": health,
            "EPS≈12M%": f"{ret12m*100:+.0f}%",
        })

    df = pd.DataFrame(rows)
    if "Score" in df:
        df = df.sort_values("Score", ascending=False, na_position="last")
    st.dataframe(df, width='stretch', hide_index=True)
    st.caption("Method: Score = 0.45·momentum(3M) + 0.35·trend(MA50/200) + 0.20·relVol. "
               "EPS≈12M% is a proxy for trailing growth from free price data.")
