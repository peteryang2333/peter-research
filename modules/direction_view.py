"""Module 2 - Direction: one market posture score from free data.

Sub-scores (0-100) like KovaView: Trend, Breadth, Credit, Vol, Leadership.
All computed from stockanalysis/nasdaq history + World Bank/rates; no Yahoo.
"""
from __future__ import annotations
import numpy as np
import streamlit as st
from data.feed import get_history, get_quote, last_close

SECTORS = ["XLF", "XLK", "XLV", "XLE", "XLI", "XLB", "XLC", "XLY", "XLP", "XLU", "XLRE"]


def _closes(hist):
    return np.array([r["close"] for r in hist], dtype=float)


def _ma(prices, n):
    return prices[-n:].mean() if len(prices) >= n else prices.mean()


def _recent_return(prices, days):
    if len(prices) <= days:
        return 0.0
    return prices[-1] / prices[-1 - days] - 1.0


def _clamp(x, lo=0, hi=100):
    return max(lo, min(hi, x))


def render():
    st.header("02 · Direction")
    st.caption("One posture score, read from trend, breadth, credit, volatility, leadership.")

    spy = get_history("spy", "1Y")
    qqq = get_history("qqq", "1Y")
    iwm = get_history("iwm", "1Y")
    btc_q = (get_quote("btc-usd") or get_quote("BTCUSD")
             or ({"price": last_close("btc-usd"), "pct": None}
                 if last_close("btc-usd") else None))

    if not spy or not qqq or not iwm:
        st.warning("Market history unreachable — showing sample posture (demo).")
        _show_sample()
        return

    sp, qp, ip = _closes(spy), _closes(qqq), _closes(iwm)

    # Trend: how many of SPY/QQQ/IWM are above MA200, MA50, and MA50>MA200
    above200 = sum(p[-1] > _ma(p, 200) for p in (sp, qp, ip)) / 3
    above50 = sum(p[-1] > _ma(p, 50) for p in (sp, qp, ip)) / 3
    above_golden = sum(_ma(p, 50) > _ma(p, 200) for p in (sp, qp, ip)) / 3
    trend = _clamp(above200 * 50 + above50 * 30 + above_golden * 20)

    # Breadth: share of SPDR sectors above their MA50
    above = 0
    for s in SECTORS:
        h = get_history(s.lower(), "6M")
        if h and _closes(h)[-1] > _ma(_closes(h), 50):
            above += 1
    breadth = _clamp(above / len(SECTORS) * 100)

    # Leadership: QQQ 3M vs SPY 3M relative strength
    rs = (1 + _recent_return(qp, 63)) / (1 + _recent_return(sp, 63))
    leadership = _clamp(50 + (rs - 1) * 250)

    # Vol: annualized realized vol of SPY
    rets = np.diff(np.log(sp[-30:]))
    vol = float(np.std(rets) * np.sqrt(252)) if len(rets) > 5 else 0.15
    vol_score = _clamp((vol - 0.08) / (0.35 - 0.08) * 100)

    # Credit: 10Y yield proxy (lower yield -> higher risk-on score)
    y10 = _yield_10y()
    if y10:
        credit = _clamp(50 + (4.5 - y10) * 18)
        credit_note = f"10Y {y10:.2f}%"
    else:
        credit = 60.0
        credit_note = "10Y n/a (sample)"

    posture = _clamp(trend * 0.30 + breadth * 0.25 + credit * 0.20
                    + vol_score * 0.10 + leadership * 0.15)
    label, mood = _label(posture)

    st.metric("Posture", f"{posture:.0f}", label)
    st.caption(f"risk-on/risk-off read: **{mood}**")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Trend", f"{trend:.0f}")
    c2.metric("Breadth", f"{breadth:.0f}", f"{above}/{len(SECTORS)} sectors > MA50")
    c3.metric("Credit", f"{credit:.0f}", credit_note)
    c4.metric("Vol", f"{vol_score:.0f}", f"σ≈{vol*100:.0f}%")
    c5.metric("Leadership", f"{leadership:.0f}", "QQQ vs SPY (3M)")

    st.divider()
    cols = st.columns(4)
    for i, (sym, name) in enumerate([("spy", "SPY"), ("qqq", "QQQ"),
                                     ("iwm", "IWM"), ("btc-usd", "BTC")]):
        q = get_quote(sym) or (btc_q if sym == "btc-usd" else None)
        with cols[i]:
            if q and q["price"]:
                st.metric(name, f"{q['price']:.2f}",
                          f"{q['pct']:+.2f}%" if q["pct"] is not None else None)
            else:
                st.metric(name, "n/a")


def _yield_10y():
    h = get_history("tnx", "1M") or get_history("^tnx", "1M")
    if h:
        try:
            return float(h[-1]["close"]) / 10.0  # TNX quoted *10
        except Exception:
            return None
    return None


def _label(p):
    if p >= 80:
        return "Strong uptrend", "risk-on, broad"
    if p >= 60:
        return "Uptrend under pressure", "risk-on, selective"
    if p >= 40:
        return "Mixed / choppy", "neutral"
    return "Risk-off / downtrend", "defensive"


def _show_sample():
    st.metric("Posture", "74", "Uptrend under pressure")
    c1, c2, c3, c4, c5 = st.columns(5)
    for c, (n, v) in zip((c1, c2, c3, c4, c5),
                         [("Trend", 88), ("Breadth", 63), ("Credit", 79),
                          ("Vol", 71), ("Leadership", 82)]):
        c.metric(n, str(v))
    cols = st.columns(4)
    for c, (n, p, d) in zip(cols, [("SPY", "729.1", "+0.4%"), ("QQQ", "706.8", "+0.9%"),
                                   ("IWM", "301.2", "-0.3%"), ("BTC", "112K", "+2.1%")]):
        c.metric(n, p, d)
