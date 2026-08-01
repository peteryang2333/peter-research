#!/usr/bin/env python3
"""
KovaView-Lite collector
-----------------------
Fetches market/macro data, computes all 6 dashboard modules, writes snapshot.json,
then exits. Pure stdlib + requests (NO pandas/numpy) so it runs in ~30MB peak on a
1GB Oracle E2.Micro alongside bridge-scheduler / ib-gateway.

Data sources (no Yahoo — yfinance is rate-limited/403 for this user):
  * https://stockanalysis.com/api/...   daily OHLCV history
  * https://api.nasdaq.com/api/...      live-ish quotes
  * https://api.worldbank.org/v2/...    macro (inflation / GDP)
  * local  signal_target.json / equity_state.json  -> real FAS position & equity

Usage:  python3 collect.py [--out /path/snapshot.json] [--cache /path/cachedir]
"""

import argparse
import json
import math
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " \
     "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept": "application/json"})
# connection pool sized for the 6 worker threads
_ADAPTER = HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
SESSION.mount("https://", _ADAPTER)

# ----------------------------------------------------------------------------
# universes
# ----------------------------------------------------------------------------
BENCH = ["SPY", "QQQ", "IWM", "DIA"]

SECTORS = {
    "XLK": "Technology", "XLF": "Financials", "XLV": "Health Care",
    "XLY": "Cons Disc", "XLP": "Cons Staples", "XLE": "Energy",
    "XLI": "Industrials", "XLB": "Materials", "XLRE": "Real Estate",
    "XLU": "Utilities", "XLC": "Comm Svcs",
}

CREDIT = ["HYG", "IEF", "LQD", "TLT"]
VOLPROXY = ["VIXY"]

# user's actual leveraged-ETF rotation universe (from live strategy)
LEVERAGED = ["FAS", "TQQQ", "SOXL", "TECL", "ERX"]

# watchlist for the Kova-style score: user's cloud/software thesis + hardware
# counterweights + the names KovaView itself was surfacing.
WATCHLIST = [
    "MSFT", "AMZN", "GOOGL", "DDOG", "NOW", "CRWD", "SNOW", "PLTR", "MDB",
    "ANET", "NVDA", "AMD", "TSM", "AVGO",
    "AMAT", "ALAB", "GLW", "CRDO",
]

# macro: World Bank inflation, consumer prices (annual %)
WB_COUNTRIES = {
    "USA": "United States", "CHN": "China", "JPN": "Japan", "DEU": "Germany",
    "GBR": "United Kingdom", "FRA": "France", "IND": "India", "BRA": "Brazil",
    "CAN": "Canada", "AUS": "Australia", "KOR": "Korea, Rep.", "MEX": "Mexico",
    "IDN": "Indonesia", "TUR": "Turkiye", "RUS": "Russia", "ZAF": "South Africa",
    "ESP": "Spain", "ITA": "Italy", "NLD": "Netherlands", "CHE": "Switzerland",
    "SGP": "Singapore", "SAU": "Saudi Arabia", "ARG": "Argentina", "EGY": "Egypt",
}

# ----------------------------------------------------------------------------
# tiny disk cache (history changes once a day; don't hammer the API)
# ----------------------------------------------------------------------------
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")


def _cache_path(key):
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in key)
    return os.path.join(CACHE_DIR, safe + ".json")


def cache_get(key, ttl):
    p = _cache_path(key)
    try:
        st = os.stat(p)
        if time.time() - st.st_mtime > ttl:
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def cache_put(key, value):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = _cache_path(key) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(value, f)
        os.replace(tmp, _cache_path(key))
    except OSError:
        pass


def http_json(url, timeout=20, tries=3):
    for i in range(tries):
        try:
            r = SESSION.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):
                time.sleep(1.5 * (i + 1))
                continue
            return None
        except (requests.RequestException, ValueError):
            time.sleep(0.8 * (i + 1))
    return None


# ----------------------------------------------------------------------------
# market data
# ----------------------------------------------------------------------------
def get_history(ticker, rng="2Y", ttl=6 * 3600):
    """Return list of {'d': 'YYYY-MM-DD', 'c': close, 'v': vol} OLDEST-FIRST."""
    key = f"hist_{ticker}_{rng}"
    cached = cache_get(key, ttl)
    if cached is not None:
        return cached
    url = (f"https://stockanalysis.com/api/symbol/s/{ticker.upper()}"
           f"/history?range={rng}&period=Daily")
    j = http_json(url)
    rows = (j or {}).get("data")
    if not isinstance(rows, list) or not rows:
        return None
    out = []
    for r in rows:
        try:
            c = float(r["c"])
            out.append({"d": str(r["t"]), "c": c,
                        "v": float(r.get("v") or 0),
                        "ch": float(r.get("ch") or 0)})
        except (KeyError, TypeError, ValueError):
            continue
    if not out:
        return None
    out.sort(key=lambda x: x["d"])          # oldest first
    cache_put(key, out)
    return out


def _money(s):
    if s is None:
        return None
    t = str(s).replace("$", "").replace(",", "").replace("%", "").strip()
    if t in ("", "N/A", "--"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def get_quote(ticker, ttl=120):
    """Live-ish quote from nasdaq; falls back to last two closes."""
    key = f"q_{ticker}"
    cached = cache_get(key, ttl)
    if cached is not None:
        return cached
    for asset in ("etf", "stocks"):
        j = http_json(f"https://api.nasdaq.com/api/quote/{ticker.upper()}"
                      f"/info?assetclass={asset}", timeout=15, tries=2)
        pd_ = ((j or {}).get("data") or {}).get("primaryData") or {}
        price = _money(pd_.get("lastSalePrice"))
        if price:
            q = {"price": price,
                 "chg": _money(pd_.get("netChange")),
                 "pct": _money(pd_.get("percentageChange")),
                 "vol": _money(pd_.get("volume")),
                 "asof": pd_.get("lastTradeTimestamp"),
                 "src": "nasdaq"}
            cache_put(key, q)
            return q
    return None


def quote_or_close(ticker, hist=None):
    q = get_quote(ticker)
    if q:
        return q
    h = hist if hist is not None else get_history(ticker, "3M")
    if h and len(h) >= 2:
        return {"price": h[-1]["c"], "chg": h[-1]["c"] - h[-2]["c"],
                "pct": h[-1].get("ch"), "vol": h[-1]["v"],
                "asof": h[-1]["d"], "src": "close"}
    return None


# ----------------------------------------------------------------------------
# small math helpers (no numpy)
# ----------------------------------------------------------------------------
def sma(vals, n):
    if not vals or len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def pct_change(vals, n):
    if not vals or len(vals) <= n or vals[-1 - n] == 0:
        return None
    return (vals[-1] / vals[-1 - n] - 1.0) * 100.0


def zscore_series(vals, window):
    """Rolling z-score; returns list aligned to vals (None where insufficient)."""
    out = []
    for i in range(len(vals)):
        if i + 1 < window:
            out.append(None)
            continue
        w = vals[i + 1 - window:i + 1]
        m = sum(w) / window
        try:
            sd = statistics.pstdev(w)
        except statistics.StatisticsError:
            sd = 0.0
        out.append(0.0 if sd == 0 else (vals[i] - m) / sd)
    return out


def realized_vol(closes, n=20):
    if len(closes) < n + 1:
        return None
    rets = []
    for i in range(len(closes) - n, len(closes)):
        p0, p1 = closes[i - 1], closes[i]
        if p0 > 0:
            rets.append(math.log(p1 / p0))
    if len(rets) < 2:
        return None
    return statistics.pstdev(rets) * math.sqrt(252) * 100.0


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def closes_of(hist):
    return [r["c"] for r in hist] if hist else []


# ----------------------------------------------------------------------------
# MODULE 1 — macro heatmap
# ----------------------------------------------------------------------------
def build_macro():
    key = "macro_wb"
    cached = cache_get(key, 12 * 3600)
    if cached is not None:
        return cached
    codes = ";".join(WB_COUNTRIES.keys())
    url = (f"https://api.worldbank.org/v2/country/{codes}"
           f"/indicator/FP.CPI.TOTL.ZG?format=json&per_page=900&mrnev=1")
    j = http_json(url, timeout=25)
    rows = j[1] if isinstance(j, list) and len(j) > 1 and j[1] else []
    items = []
    for r in rows:
        try:
            iso = r["countryiso3code"]
            val = r["value"]
            if val is None or iso not in WB_COUNTRIES:
                continue
            items.append({"iso": iso, "name": WB_COUNTRIES[iso],
                          "infl": round(float(val), 2), "year": r["date"]})
        except (KeyError, TypeError, ValueError):
            continue
    items.sort(key=lambda x: x["infl"], reverse=True)

    def tier(v):
        if v < 0:                return "deflation"
        if v < 2:                return "cool"
        if v < 4:                return "normal"
        if v < 8:                return "warm"
        return "hot"

    for it in items:
        it["tier"] = tier(it["infl"])
    out = {"items": items,
           "hottest": items[:5],
           "coolest": list(reversed(items[-5:])) if len(items) >= 5 else [],
           "source": "World Bank FP.CPI.TOTL.ZG (most recent non-empty year)"}
    if items:
        cache_put(key, out)
    return out


# ----------------------------------------------------------------------------
# MODULE 2 — direction / market posture
# ----------------------------------------------------------------------------
def build_direction(hist_cache):
    spy = hist_cache.get("SPY")
    if not spy:
        return {"error": "no SPY history"}
    spy_c = closes_of(spy)

    # --- trend: price vs MA20/50/200 + slope of MA50
    ma20, ma50, ma200 = sma(spy_c, 20), sma(spy_c, 50), sma(spy_c, 200)
    px = spy_c[-1]
    trend_pts = 0
    for m in (ma20, ma50, ma200):
        if m and px > m:
            trend_pts += 1
    ma50_prev = sma(spy_c[:-10], 50) if len(spy_c) > 60 else None
    if ma50 and ma50_prev and ma50 > ma50_prev:
        trend_pts += 1
    trend = clamp(trend_pts / 4.0 * 100.0)

    # --- breadth: % of sector ETFs + leveraged universe above their 50DMA
    above, total = 0, 0
    for t in list(SECTORS) + BENCH:
        h = hist_cache.get(t)
        if not h:
            continue
        c = closes_of(h)
        m = sma(c, 50)
        if m:
            total += 1
            if c[-1] > m:
                above += 1
    breadth = clamp(above / total * 100.0) if total else 50.0

    # --- credit: HYG vs IEF relative strength (risk appetite proxy)
    hyg, ief = hist_cache.get("HYG"), hist_cache.get("IEF")
    credit = 50.0
    credit_detail = None
    if hyg and ief:
        hc, ic = closes_of(hyg), closes_of(ief)
        n = min(len(hc), len(ic))
        if n > 65:
            ratio = [hc[-n + i] / ic[-n + i] for i in range(n)]
            r_now, r_ma = ratio[-1], sma(ratio, 50)
            roc = pct_change(ratio, 21)
            if r_ma:
                credit = clamp(50 + (r_now / r_ma - 1.0) * 1200)
            credit_detail = {"hyg_ief_roc_1m": round(roc, 2) if roc else None}

    # --- volatility: realized vol of SPY, inverted (low vol = high score)
    rv = realized_vol(spy_c, 20)
    vol = clamp(100 - (rv - 8) / 22 * 100) if rv else 50.0

    # --- leadership: growth (QQQ) + small-cap (IWM) participation vs SPY, 1M.
    # Scaling kept gentle (x6) so a normal -5% divergence does NOT peg the
    # sub-score to an absolute 0 and destroy the signal.
    qqq, iwm = hist_cache.get("QQQ"), hist_cache.get("IWM")
    lead = 50.0
    lead_detail = {}
    b = pct_change(spy_c, 21)
    q_rel = i_rel = None
    if qqq and b is not None:
        a = pct_change(closes_of(qqq), 21)
        if a is not None:
            q_rel = a - b
            lead_detail["qqq_vs_spy_1m"] = round(q_rel, 2)
    if iwm and b is not None:
        a = pct_change(closes_of(iwm), 21)
        if a is not None:
            i_rel = a - b
            lead_detail["iwm_vs_spy_1m"] = round(i_rel, 2)
    if q_rel is not None or i_rel is not None:
        if q_rel is not None and i_rel is not None:
            blended = q_rel * 0.7 + i_rel * 0.3
        else:
            blended = q_rel if q_rel is not None else i_rel
        lead_detail["blended"] = round(blended, 2)
        lead = clamp(50 + blended * 6)

    posture = round(0.30 * trend + 0.25 * breadth + 0.15 * credit +
                    0.10 * vol + 0.20 * lead)

    if posture >= 80:
        label, tone = "Strong uptrend", "risk-on"
    elif posture >= 65:
        label, tone = "Uptrend under pressure", "risk-on, selective"
    elif posture >= 50:
        label, tone = "Choppy / two-way", "neutral, be picky"
    elif posture >= 35:
        label, tone = "Downtrend attempt", "risk-off leaning"
    else:
        label, tone = "Downtrend", "risk-off"

    # --- tape snapshot
    tape = []
    for t in BENCH + ["HYG", "TLT", "VIXY"]:
        h = hist_cache.get(t)
        q = quote_or_close(t, h)
        if not q:
            continue
        c = closes_of(h)
        tape.append({
            "sym": t, "price": round(q["price"], 2),
            "pct": round(q["pct"], 2) if q.get("pct") is not None else None,
            "ma50": round(sma(c, 50), 2) if sma(c, 50) else None,
            "ma200": round(sma(c, 200), 2) if sma(c, 200) else None,
            "r1m": round(pct_change(c, 21), 2) if pct_change(c, 21) else None,
            "r3m": round(pct_change(c, 63), 2) if pct_change(c, 63) else None,
            "asof": q.get("asof"), "src": q.get("src"),
        })

    return {
        "posture": posture, "label": label, "tone": tone,
        "subs": {"Trend": round(trend), "Breadth": round(breadth),
                 "Credit": round(credit), "Vol": round(vol),
                 "Leadership": round(lead)},
        "detail": {"spy_px": round(px, 2),
                   "ma20": round(ma20, 2) if ma20 else None,
                   "ma50": round(ma50, 2) if ma50 else None,
                   "ma200": round(ma200, 2) if ma200 else None,
                   "breadth_above": above, "breadth_total": total,
                   "realized_vol_20d": round(rv, 1) if rv else None,
                   "credit": credit_detail, "leadership": lead_detail},
        "tape": tape,
        "method": ("posture = 0.30*Trend + 0.25*Breadth + 0.15*Credit "
                   "+ 0.10*Vol + 0.20*Leadership  (all 0-100, transparent)"),
    }


# ----------------------------------------------------------------------------
# MODULE 3 — sector rotation (RRG)
# ----------------------------------------------------------------------------
def _weekly(hist):
    """Collapse daily rows to last close of each ISO week."""
    if not hist:
        return []
    buckets = {}
    for r in hist:
        try:
            y, w, _ = datetime.strptime(r["d"], "%Y-%m-%d").isocalendar()
        except ValueError:
            continue
        buckets[(y, w)] = r["c"]
    return [buckets[k] for k in sorted(buckets)]


def build_rotation(hist_cache, bench="SPY", window=12, mom_lag=4, tail=6):
    bh = _weekly(hist_cache.get(bench))
    if len(bh) < window + mom_lag + tail + 2:
        return {"error": "insufficient benchmark history"}
    out = []
    for sym, name in SECTORS.items():
        sh = _weekly(hist_cache.get(sym))
        n = min(len(sh), len(bh))
        if n < window + mom_lag + tail + 2:
            continue
        s, b = sh[-n:], bh[-n:]
        rs = [100.0 * s[i] / b[i] for i in range(n) if b[i]]
        z = zscore_series(rs, window)
        ratio = [None if v is None else 100 + v for v in z]
        roc = [None if (i < mom_lag or rs[i - mom_lag] == 0)
               else rs[i] / rs[i - mom_lag] * 100.0 for i in range(len(rs))]
        roc_clean = [v for v in roc if v is not None]
        zr = zscore_series(roc_clean, window)
        pad = len(roc) - len(zr)
        mom = [None] * pad + [None if v is None else 100 + v for v in zr]

        pts = [(ratio[i], mom[i]) for i in range(len(ratio))
               if ratio[i] is not None and i < len(mom) and mom[i] is not None]
        if len(pts) < 2:
            continue
        pts = pts[-tail:]
        r_now, m_now = pts[-1]

        if r_now >= 100 and m_now >= 100:
            quad = "Leading"
        elif r_now >= 100:
            quad = "Weakening"
        elif m_now < 100:
            quad = "Lagging"
        else:
            quad = "Improving"

        c = closes_of(hist_cache.get(sym))
        out.append({
            "sym": sym, "name": name, "quadrant": quad,
            "rs_ratio": round(r_now, 2), "rs_mom": round(m_now, 2),
            "tail": [[round(a, 2), round(bb, 2)] for a, bb in pts],
            "r1m": round(pct_change(c, 21), 2) if pct_change(c, 21) else None,
            "r3m": round(pct_change(c, 63), 2) if pct_change(c, 63) else None,
        })

    order = {"Leading": 0, "Weakening": 1, "Improving": 2, "Lagging": 3}
    out.sort(key=lambda x: (order[x["quadrant"]], -x["rs_ratio"]))
    counts = {}
    for o in out:
        counts[o["quadrant"]] = counts.get(o["quadrant"], 0) + 1
    return {"bench": bench, "sectors": out, "counts": counts,
            "method": (f"JdK-style: RS=100*sector/{bench}; RS-Ratio=100+z(RS,{window}w); "
                       f"RS-Mom=100+z(RS {mom_lag}w RoC,{window}w); tail={tail}w")}


# ----------------------------------------------------------------------------
# MODULE 4 — Kova-style transparent score
# ----------------------------------------------------------------------------
def score_one(sym, hist, spy_c):
    c = closes_of(hist)
    if len(c) < 210:
        return None
    px = c[-1]
    ma20, ma50, ma200 = sma(c, 20), sma(c, 50), sma(c, 200)

    # 1) trend stack (0-100)
    stack = 0
    if ma20 and px > ma20:
        stack += 1
    if ma50 and px > ma50:
        stack += 1
    if ma200 and px > ma200:
        stack += 1
    if ma20 and ma50 and ma20 > ma50:
        stack += 1
    trend_s = stack / 4.0 * 100

    # 2) momentum: blended 1M/3M/6M return
    r1, r3, r6 = pct_change(c, 21), pct_change(c, 63), pct_change(c, 126)
    blend = sum(v for v in (r1, r3, r6) if v is not None) / \
        max(1, len([v for v in (r1, r3, r6) if v is not None]))
    mom_s = clamp(50 + blend * 1.6)

    # 3) relative strength vs SPY (3M)
    rs_s = 50.0
    rel3 = None
    if spy_c:
        a, b = pct_change(c, 63), pct_change(spy_c, 63)
        if a is not None and b is not None:
            rel3 = a - b
            rs_s = clamp(50 + rel3 * 2.2)

    # 4) proximity to 52w high
    hi52 = max(c[-252:]) if len(c) >= 252 else max(c)
    dd = (px / hi52 - 1.0) * 100 if hi52 else 0
    prox_s = clamp(100 + dd * 3.2)

    # 5) relative volume (today vs 20d avg)
    vols = [r["v"] for r in hist if r["v"]]
    relvol = None
    vol_s = 50.0
    if len(vols) > 21:
        avg20 = sum(vols[-21:-1]) / 20
        if avg20:
            relvol = vols[-1] / avg20 * 100
            vol_s = clamp(30 + (relvol - 60) * 0.55)

    score = round(0.26 * trend_s + 0.24 * mom_s + 0.24 * rs_s +
                  0.16 * prox_s + 0.10 * vol_s)

    # Health is judged on TREND STRUCTURE, not merely distance from the high:
    # a volatile name +60% in 3M should not be flagged REDUCE just because it
    # sits 35% below its peak. REDUCE = actually broken (below MA200) AND
    # lagging the index.
    if ma200 and px < ma200 and (rel3 is not None and rel3 < 0):
        health = "REDUCE"
    elif ma200 and px < ma200:
        health = "Watch"
    elif ma50 and px < ma50:
        health = "Weak"
    elif dd < -20:
        health = "Watch"
    else:
        health = "Healthy"

    q = quote_or_close(sym, hist)
    return {
        "sym": sym,
        "price": round(q["price"], 2) if q else round(px, 2),
        "pct": round(q["pct"], 2) if q and q.get("pct") is not None else None,
        "score": int(score), "health": health,
        "relvol": round(relvol) if relvol else None,
        "r1m": round(r1, 1) if r1 is not None else None,
        "r3m": round(r3, 1) if r3 is not None else None,
        "rel_spy_3m": round(rel3, 1) if rel3 is not None else None,
        "from_52wh": round(dd, 1),
        "parts": {"trend": round(trend_s), "mom": round(mom_s),
                  "rs": round(rs_s), "prox": round(prox_s), "vol": round(vol_s)},
    }


def build_kova(hist_cache, spy_c):
    rows = []
    for sym in WATCHLIST:
        h = hist_cache.get(sym)
        if not h:
            continue
        r = score_one(sym, h, spy_c)
        if r:
            rows.append(r)
    rows.sort(key=lambda x: -x["score"])
    return {"rows": rows,
            "method": ("score = 0.26*TrendStack + 0.24*Momentum(1/3/6M) + "
                       "0.24*RelStrength(vs SPY 3M) + 0.16*Prox52wHigh + "
                       "0.10*RelVolume — fully transparent, no black box")}


def build_leveraged(hist_cache, spy_c):
    rows = []
    for sym in LEVERAGED:
        h = hist_cache.get(sym)
        if not h:
            continue
        r = score_one(sym, h, spy_c)
        if r:
            rows.append(r)
    rows.sort(key=lambda x: -x["score"])
    return rows


# ----------------------------------------------------------------------------
# MODULES 5 & 6 — discipline + proof (real strategy state from the VM)
# ----------------------------------------------------------------------------
def read_state(paths):
    out = {}
    for name, p in paths.items():
        try:
            with open(p, "r", encoding="utf-8") as f:
                out[name] = json.load(f)
        except (OSError, ValueError):
            out[name] = None
    return out


def build_proof(state, hist_cache):
    sig = state.get("signal") or {}
    eq = state.get("equity") or {}
    holding = sig.get("holding")
    peak = eq.get("peak_nlv")

    pos = None
    if holding:
        h = hist_cache.get(holding)
        q = quote_or_close(holding, h)
        c = closes_of(h)
        pos = {
            "sym": holding,
            "weight": sig.get("weight"),
            "price": round(q["price"], 2) if q else None,
            "pct": round(q["pct"], 2) if q and q.get("pct") is not None else None,
            "r1m": round(pct_change(c, 21), 2) if pct_change(c, 21) else None,
            "ma20": round(sma(c, 20), 2) if sma(c, 20) else None,
            "ma50": round(sma(c, 50), 2) if sma(c, 50) else None,
            "below_ma20": (bool(q and sma(c, 20) and q["price"] < sma(c, 20))
                           if q else None),
        }

    recent = sig.get("recent") or []
    switches = sum(1 for i in range(1, len(recent)) if recent[i][1] != recent[i - 1][1])
    return {
        "holding": holding, "weight": sig.get("weight"),
        "cash": sig.get("cash"), "spy_trend": sig.get("spy_trend"),
        "best": sig.get("best"), "best_score": sig.get("best_score"),
        "signal_date": sig.get("date"), "generated_at": sig.get("generated_at"),
        "recent": recent, "switches_in_window": switches,
        "peak_nlv": peak, "position": pos,
        "verified_by": "local daily-signal-bridge state (IBKR-backed)",
    }


def build_discipline(state, hist_cache, direction):
    """Position sizing presets + risk context driven by the live posture."""
    eq = (state.get("equity") or {}).get("peak_nlv") or 100000.0
    posture = direction.get("posture", 50) if isinstance(direction, dict) else 50

    if posture >= 70:
        risk_pct, note = 1.0, "Posture strong — full 1% unit risk allowed."
    elif posture >= 50:
        risk_pct, note = 0.6, "Posture mixed — cut unit risk to 0.6%."
    else:
        risk_pct, note = 0.3, "Posture weak — 0.3% probe size only."

    examples = []
    for sym in ["FAS", "NVDA", "MSFT", "DDOG"]:
        h = hist_cache.get(sym)
        if not h:
            continue
        c = closes_of(h)
        px = c[-1]
        stop = sma(c, 20) or px * 0.94          # structural stop at MA20
        if stop >= px:
            stop = px * 0.94
        per_share = px - stop
        risk_usd = eq * risk_pct / 100.0
        shares = int(risk_usd / per_share) if per_share > 0 else 0
        examples.append({
            "sym": sym, "price": round(px, 2), "stop": round(stop, 2),
            "stop_basis": "MA20" if sma(c, 20) else "-6% fallback",
            "risk_per_share": round(per_share, 2),
            "risk_usd": round(risk_usd),
            "shares": shares,
            "notional": round(shares * px),
            "notional_pct": round(shares * px / eq * 100, 1) if eq else None,
        })

    return {"equity_base": eq, "risk_pct": risk_pct, "note": note,
            "posture_used": posture, "examples": examples,
            "rule": "shares = (equity x risk%) / (entry - structural stop)"}


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "web", "snapshot.json"))
    ap.add_argument("--signal", default="/opt/daily-signal-bridge/signal_target.json")
    ap.add_argument("--equity", default="/opt/daily-signal-bridge/equity_state.json")
    ap.add_argument("--public", action="store_true",
                    help="strip personal broker data (real holdings, NLV, equity) "
                         "so the snapshot is safe to publish on a public site")
    args = ap.parse_args()

    t0 = time.time()
    universe = list(dict.fromkeys(
        BENCH + list(SECTORS) + CREDIT + VOLPROXY + LEVERAGED + WATCHLIST))

    # Parallel fetch: 6 workers keeps wall-time ~20s instead of ~115s while
    # staying polite to the free endpoints and light on a 1GB box.
    hist_cache, failed = {}, []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(get_history, s, "2Y"): s for s in universe}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                h = fut.result()
            except Exception:
                h = None
            if h:
                hist_cache[sym] = h
            else:
                failed.append(sym)

    # Warm the quote cache in parallel too (build_* then reads from cache).
    quote_syms = list(dict.fromkeys(
        BENCH + ["HYG", "TLT", "VIXY"] + LEVERAGED + WATCHLIST))
    with ThreadPoolExecutor(max_workers=6) as ex:
        for fut in [ex.submit(get_quote, s) for s in quote_syms]:
            try:
                fut.result()
            except Exception:
                pass

    spy_c = closes_of(hist_cache.get("SPY"))

    direction = build_direction(hist_cache)
    state = read_state({"signal": args.signal, "equity": args.equity})

    snap = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "generated_at_local": (datetime.now(timezone.utc) +
                               timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S") + " CST",
        "macro": build_macro(),
        "direction": direction,
        "rotation": build_rotation(hist_cache),
        "kova": build_kova(hist_cache, spy_c),
        "leveraged": build_leveraged(hist_cache, spy_c),
        "discipline": build_discipline(state, hist_cache, direction),
        "proof": build_proof(state, hist_cache),
        "meta": {
            "universe": len(universe),
            "fetched": len(hist_cache),
            "failed": failed,
            "elapsed_s": round(time.time() - t0, 1),
            "sources": ["stockanalysis.com", "api.nasdaq.com",
                        "api.worldbank.org", "local strategy state"],
            "note": "No Yahoo/yfinance (rate-limited for this account).",
        },
    }

    # Public mode: never let broker-derived personal figures reach a public
    # site. Keeps the rule/method (useful, non-sensitive), drops the money.
    if args.public:
        d = snap["discipline"]
        snap["discipline"] = {
            "private": True,
            "risk_pct": d.get("risk_pct"),
            "rule": d.get("rule"),
            "posture_used": d.get("posture_used"),
            "note": "Account equity is private; enter it in the browser to size positions.",
        }
        p = snap["proof"]
        snap["proof"] = {
            "private": True,
            "spy_trend": p.get("spy_trend"),
            "note": "Broker-verified ledger is private; served only from the local/VM instance.",
        }
        snap["meta"]["mode"] = "public"

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, args.out)

    print(f"[ok] snapshot -> {args.out} "
          f"({len(hist_cache)}/{len(universe)} symbols, "
          f"{snap['meta']['elapsed_s']}s)")
    if failed:
        print(f"[warn] no history for: {', '.join(failed)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
