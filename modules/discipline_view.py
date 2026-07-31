"""Module 5 - Discipline: percent-risk position sizer + SQLite trade journal."""
from __future__ import annotations
import os
import sqlite3
from datetime import date
import pandas as pd
import streamlit as st
from data.ibkr import get_positions, is_connected

DB = os.getenv("JOURNAL_DB", os.path.join(os.path.dirname(__file__), "..", "data", "journal.db"))


def _con():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    c = sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY, symbol TEXT, side TEXT, qty REAL,
        entry REAL, stop REAL, exit REAL, r REAL, date TEXT, note TEXT)""")
    return c


def add_trade(symbol, side, qty, entry, stop, exit_, d, note):
    risk = (entry - stop) if stop else None
    r = ((exit_ - entry) / risk) if (risk and exit_) else None
    c = _con()
    c.execute("INSERT INTO trades(symbol,side,qty,entry,stop,exit,r,date,note) "
              "VALUES(?,?,?,?,?,?,?,?,?)",
              (symbol, side, qty, entry, stop, exit_, r, d, note))
    c.commit()
    c.close()


def stats():
    c = _con()
    rows = c.execute("SELECT symbol,side,qty,entry,stop,exit,r,date,note FROM trades "
                     "ORDER BY date DESC").fetchall()
    c.close()
    if not rows:
        return None, pd.DataFrame()
    df = pd.DataFrame(rows, columns=["symbol", "side", "qty", "entry", "stop",
                                     "exit", "R", "date", "note"])
    wins = df[df["R"] > 0]["R"]
    losses = df[df["R"] < 0]["R"].abs()
    gross_w = wins.sum()
    gross_l = losses.sum()
    pf = (gross_w / gross_l) if gross_l > 0 else float("inf")
    wr = len(wins) / max(1, len(df[df["R"].notna()]))
    exp = df["R"].mean() if df["R"].notna().any() else 0
    realized = ((df["exit"] - df["entry"]) * df["qty"] * df["side"].map(
        {"L": 1, "S": -1})).fillna(0).sum()
    return {"win_rate": wr, "profit_factor": pf, "expectancy_R": exp,
            "realized": realized, "trades": len(df)}, df


def render():
    st.header("05 · Discipline")
    st.caption("Type a ticker, get the size the rules allow. Every fill lands in one ledger.")

    st.subheader("Position sizer (percent-risk)")
    col1, col2, col3, col4 = st.columns(4)
    acct = col1.number_input("Account $", value=100000.0, step=1000.0)
    risk_pct = col2.number_input("Risk %", value=1.0, step=0.1)
    entry = col3.number_input("Entry", value=100.0, step=1.0)
    stop = col4.number_input("Stop", value=95.0, step=1.0)
    if entry > stop:
        shares = (acct * risk_pct / 100) / (entry - stop)
        st.success(f"Max shares: **{shares:.0f}**  ·  $ risk: **${acct*risk_pct/100:,.0f}**  "
                   f"·  position: **${shares*entry:,.0f}**")
    else:
        st.error("Entry must be above stop.")

    st.divider()
    st.subheader("Trading journal")
    with st.form("add_trade", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        sym = c1.text_input("Symbol", "NVDA")
        side = c2.selectbox("Side", ["L", "S"])
        qty = c3.number_input("Qty", value=98.0, step=1.0)
        c4, c5, c6 = st.columns(3)
        e = c4.number_input("Entry", value=120.0, step=1.0)
        s = c5.number_input("Stop", value=114.0, step=1.0)
        x = c6.number_input("Exit", value=0.0, step=1.0)
        c7, c8 = st.columns(2)
        d = c7.date_input("Date", value=date.today())
        note = c8.text_input("Note", "")
        if st.form_submit_button("Add fill"):
            add_trade(sym, side, qty, e, s, (x or None), str(d), note)
            st.rerun()

    s, df = stats()
    if s:
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Win rate", f"{s['win_rate']*100:.0f}%")
        m2.metric("Profit Factor", f"{s['profit_factor']:.1f}")
        m3.metric("Expectancy", f"{s['expectancy_R']:+.2f}R")
        m4.metric("Realized", f"${s['realized']:,.0f}")
        m5.metric("Trades", s["trades"])
        heat = (s["realized"] / acct) * 100
        st.caption(f"Portfolio heat: {heat:.1f}% (cap 6%)")
        st.dataframe(df, width='stretch', hide_index=True)
    else:
        st.info("No trades yet. Add a fill above, or sync from IBKR.")

    if st.button("Sync positions from IBKR"):
        if is_connected():
            pos = get_positions()
            st.success(f"IBKR connected — {len(pos)} positions pulled.")
            st.dataframe(pd.DataFrame(pos), width='stretch', hide_index=True)
        else:
            st.warning("IBKR gateway not reachable at IBKR_BASE. Set IBKR_BASE and "
                       "authenticate the Client Portal Gateway, then retry. Showing demo:")
            st.dataframe(pd.DataFrame(get_positions()), width='stretch', hide_index=True)
