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
#
# This is a THEMATIC pool, not a market-wide screen. It is assembled by hand
# from the 2026-H2 thesis ("cloud/apps lead, hardware second") so the score is
# a *ranker for names you already care about*, not a stock picker.
WATCHLIST_GROUPS = [
    {
        "key": "cloud",
        "name": "云厂 / 应用软件",
        "why": "2026H2 核心论点：云收入 recurring、可持续，超配",
        "syms": ["MSFT", "AMZN", "GOOGL", "DDOG", "NOW", "CRWD", "SNOW",
                 "PLTR", "MDB"],
    },
    {
        "key": "hardware",
        "name": "半导体 / 硬件对照组",
        "why": "低配但必须盯：用来验证「硬件退居次席」是否还成立",
        "syms": ["ANET", "NVDA", "AMD", "TSM", "AVGO"],
    },
    {
        "key": "supply",
        "name": "AI 基建供应链延伸",
        "why": "私募 all-in 数据中心的「卖水管」受益端，小仓位观察",
        "syms": ["AMAT", "ALAB", "GLW", "CRDO"],
    },
]

WATCHLIST = [s for g in WATCHLIST_GROUPS for s in g["syms"]]

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


def http_json(url, timeout=25, tries=4):
    # The free World Bank endpoint is flaky through some proxies (intermittent
    # 400/502 even for valid queries), so retry a broader set of status codes
    # before giving up. A genuine 404 still bails immediately.
    for i in range(tries):
        try:
            r = SESSION.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 429, 500, 502, 503):
                time.sleep(1.2 * (i + 1))
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
# macro: lat/lng for the bubble map (equirectangular placement)
COUNTRY_GEO = {
    "USA": (38.0, -97.0), "CHN": (35.0, 104.0), "JPN": (36.0, 138.0),
    "DEU": (51.0, 10.0), "GBR": (54.0, -2.0), "FRA": (46.0, 2.0),
    "IND": (22.0, 78.0), "BRA": (-10.0, -55.0), "CAN": (56.0, -106.0),
    "AUS": (-25.0, 133.0), "KOR": (36.0, 128.0), "MEX": (23.0, -102.0),
    "IDN": (-2.0, 118.0), "TUR": (39.0, 35.0), "RUS": (61.0, 105.0),
    "ZAF": (-29.0, 24.0), "ESP": (40.0, -4.0), "ITA": (42.0, 12.0),
    "NLD": (52.0, 5.0), "CHE": (47.0, 8.0), "SGP": (1.3, 103.0),
    "SAU": (24.0, 45.0), "ARG": (-34.0, -64.0), "EGY": (26.0, 30.0),
}

def _wb_fetch_multi(codes, indicator):
    """Bulk World Bank pull (one request for many countries)."""
    url = (f"https://api.worldbank.org/v2/country/{codes}"
           f"/indicator/{indicator}?format=json&per_page=900&mrnev=1")
    j = http_json(url, timeout=25)
    rows = j[1] if isinstance(j, list) and len(j) > 1 and j[1] else []
    out = {}
    for r in rows:
        try:
            iso = r["countryiso3code"]; val = r["value"]
            if val is None or iso not in WB_COUNTRIES:
                continue
            out[iso] = {"val": round(float(val), 2), "year": r["date"]}
        except (KeyError, TypeError, ValueError):
            continue
    return out

def _wb_fetch_one(code, indicator):
    """Single-country pull (used as a fallback when the bulk call is flaky)."""
    url = (f"https://api.worldbank.org/v2/country/{code}"
           f"/indicator/{indicator}?format=json&mrnev=1")
    j = http_json(url, timeout=25)
    if not isinstance(j, list) or len(j) < 2 or not j[1]:
        return {}
    r = j[1][0]
    try:
        iso = r["countryiso3code"]; val = r["value"]
        if val is None or iso not in WB_COUNTRIES:
            return {}
        return {iso: {"val": round(float(val), 2), "year": r["date"]}}
    except (KeyError, TypeError, ValueError):
        return {}

def _wb_indicator(codes, indicator):
    """One World Bank indicator -> {iso3: {'val': float, 'year': str}}.

    The free endpoint is flaky behind some proxies (intermittent 400 even for
    valid bulk queries). Try the cheap bulk call first; if it comes back thin,
    refill the missing countries one-by-one in a small pool."""
    multi = _wb_fetch_multi(codes, indicator)
    if len(multi) >= int(0.8 * len(WB_COUNTRIES)):
        return multi
    missing = [c for c in WB_COUNTRIES if c not in multi]
    with ThreadPoolExecutor(max_workers=6) as ex:
        for fut in as_completed({ex.submit(_wb_fetch_one, c, indicator): c
                                 for c in missing}):
            try:
                multi.update(fut.result())
            except Exception:
                pass
    return multi

def build_macro():
    key = "macro_wb_v2"   # bump on shape change so stale caches self-invalidate
    cached = cache_get(key, 12 * 3600)
    if cached is not None:
        return cached
    codes = ";".join(WB_COUNTRIES.keys())
    cpi = _wb_indicator(codes, "FP.CPI.TOTL.ZG")
    gdp = _wb_indicator(codes, "NY.GDP.MKTP.KD.ZG")
    items = []
    for iso, name in WB_COUNTRIES.items():
        ci = cpi.get(iso)
        if ci is None:
            continue
        gi = gdp.get(iso)
        lat, lng = COUNTRY_GEO.get(iso, (0.0, 0.0))
        items.append({
            "iso": iso, "name": name,
            "infl": ci["val"], "infl_year": ci["year"],
            "gdp": gi["val"] if gi else None, "gdp_year": gi["year"] if gi else None,
            "lat": lat, "lng": lng,
        })

    def tier(v):
        if v < 0:                return "cold"
        if v < 2:                return "cool"
        if v < 4:                return "normal"
        if v < 8:                return "warm"
        return "hot"

    for it in items:
        it["tier"] = tier(it["infl"])
    items.sort(key=lambda x: x["infl"], reverse=True)
    gdps = [x for x in items if x["gdp"] is not None]
    gdps.sort(key=lambda x: x["gdp"], reverse=True)
    out = {"items": items,
           "hottest": items[:5],
           "coolest": list(reversed(items[-5:])) if len(items) >= 5 else [],
           "gdp_highest": gdps[:5],
           "gdp_lowest": list(reversed(gdps[-5:])) if len(gdps) >= 5 else [],
           "source": "World Bank FP.CPI.TOTL.ZG + NY.GDP.MKTP.KD.ZG (latest non-empty year)"}
    if items:
        cache_put(key, out)
    return out


# ----------------------------------------------------------------------------
# macro economic calendar (computed locally, no API key)
# ----------------------------------------------------------------------------
import calendar as _cal

FOMC_2026 = [
    datetime(2026, 1, 28), datetime(2026, 3, 18), datetime(2026, 4, 29),
    datetime(2026, 6, 10), datetime(2026, 7, 29), datetime(2026, 9, 16),
    datetime(2026, 10, 28), datetime(2026, 12, 9),
]

def _next_dom(y, m, day):
    last = _cal.monthrange(y, m)[1]
    return datetime(y, m, min(day, last))

def _first_friday(y, m):
    for d in range(1, 8):
        if datetime(y, m, d).weekday() == 4:
            return datetime(y, m, d)
    return datetime(y, m, 7)

def _last_friday(y, m):
    last = _cal.monthrange(y, m)[1]
    for d in range(last, last - 7, -1):
        if datetime(y, m, d).weekday() == 4:
            return datetime(y, m, d)
    return datetime(y, m, last)

def _nm(y, m, s=1):
    m += s
    while m > 12: m -= 12; y += 1
    while m < 1: m += 12; y -= 1
    return y, m

def build_events():
    """Upcoming US macro events with day-countdowns. Dates are rule-based;
    FOMC uses the published 2026 schedule (approximate)."""
    today = datetime.now(timezone.utc).date()
    def days(d): return (d.date() - today).days
    evs = []
    y, m = today.year, today.month                       # CPI ~15th monthly
    for _ in range(3):
        d = _next_dom(y, m, 15)
        if d.date() >= today: evs.append(("CPI 通胀", d, "monthly")); break
        y, m = _nm(y, m)
    y, m = today.year, today.month                        # Nonfarm — 1st Fri
    for _ in range(3):
        d = _first_friday(y, m)
        if d.date() >= today: evs.append(("非农就业", d, "monthly")); break
        y, m = _nm(y, m)
    y, m = today.year, today.month                        # PCE — last Fri (approx)
    for _ in range(3):
        d = _last_friday(y, m)
        if d.date() >= today: evs.append(("PCE 物价", d, "monthly")); break
        y, m = _nm(y, m)
    y, m = today.year, today.month                        # GDP advance — end Jan/Apr/Jul/Oct
    for _ in range(6):
        if m in (1, 4, 7, 10):
            d = _next_dom(y, m, 28)
            if d.date() >= today: evs.append(("GDP 初值", d, "quarterly")); break
        y, m = _nm(y, m)
    for d in FOMC_2026:                                    # FOMC — 2026 schedule
        if d.date() >= today:
            evs.append(("FOMC", d, "scheduled")); break
    evs.sort(key=lambda e: e[1])
    return [{"name": e[0], "date": e[1].isoformat()[:10],
             "in_days": days(e[1]), "cadence": e[2]} for e in evs[:7]]


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
                       "0.10*RelVolume — fully transparent, no black box"),
            "spec": SCORE_SPEC}


# Self-documenting spec so the dashboard can explain itself instead of relying
# on a README nobody opens. Keep in sync with score_one() above.
SCORE_SPEC = {
    "kind": "ranker",           # NOT a screener
    "min_bars": 210,
    "factors": [
        {"w": 26, "name": "趋势结构 TrendStack", "en": "trend",
         "how": "价 > MA20 / MA50 / MA200 各记 1 分，MA20 > MA50 再记 1 分；"
                "满分 4 分折算百分制"},
        {"w": 24, "name": "动量 Momentum", "en": "mom",
         "how": "1M / 3M / 6M 收益率取均值 blend，得分 = 50 + blend×1.6（截断 0–100）"},
        {"w": 24, "name": "相对强弱 RS vs SPY", "en": "rs",
         "how": "近 3 个月自身涨幅 − SPY 同期涨幅 = rel3，得分 = 50 + rel3×2.2"},
        {"w": 16, "name": "距 52 周高点 Prox", "en": "prox",
         "how": "dd = 现价/52周最高 − 1（负数），得分 = 100 + dd×100×3.2"},
        {"w": 10, "name": "相对量能 RelVol", "en": "vol",
         "how": "今日成交量 ÷ 前 20 日均量 ×100 = relvol，得分 = 30 + (relvol−60)×0.55"},
    ],
    "health": [
        {"tag": "REDUCE", "rule": "跌破 MA200 **且** 近 3 个月跑输 SPY —— 真的坏了"},
        {"tag": "Watch", "rule": "跌破 MA200（但没跑输指数），或距 52 周高点回撤 > 20%"},
        {"tag": "Weak", "rule": "跌破 MA50"},
        {"tag": "Healthy", "rule": "以上都不满足，均线结构完好"},
    ],
    "caveat": ("健康度只看趋势结构、不单看距高点：一只 3 个月涨 60% 的高波动股"
               "即便离峰值 −35% 也不该被判 REDUCE。"),
}


def build_universe_doc():
    """Pool composition, surfaced in the UI so the strategy is self-explaining."""
    return {
        "watchlist_groups": WATCHLIST_GROUPS,
        "watchlist_n": len(WATCHLIST),
        "leveraged": LEVERAGED,
        "bench": BENCH,
        "sectors_n": len(SECTORS),
        "note": ("观察池是按 2026H2 主题手工拼的三块，不是全市场筛选；"
                 "打分只回答「我池子里现在谁最强 / 谁在坏掉」，"
                 "不回答「全市场哪只该买」。"),
        "edit_where": "vm/collect.py 的 WATCHLIST_GROUPS / LEVERAGED",
    }


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

    # Equity curve straight from the bridge's daily NLV log (IBKR-sourced).
    curve, nlv, nlv_prev, dd, day_chg = [], None, None, None, None
    raw_nlv = state.get("nlv") or {}
    if isinstance(raw_nlv, dict) and raw_nlv:
        try:
            curve = sorted(((d, float(v)) for d, v in raw_nlv.items()),
                           key=lambda kv: kv[0])
            curve = [{"date": d, "nlv": round(v, 2)} for d, v in curve]
            nlv = curve[-1]["nlv"]
            if len(curve) >= 2:
                nlv_prev = curve[-2]["nlv"]
                day_chg = round((nlv / nlv_prev - 1) * 100, 2) if nlv_prev else None
            if peak:
                dd = round((nlv / peak - 1) * 100, 2)
        except (TypeError, ValueError):
            curve = []

    # ---- verification / consistency (private instance) ----
    consistent = None
    if isinstance(raw_nlv, dict) and raw_nlv and nlv is not None:
        try:
            consistent = abs(nlv - float(raw_nlv[max(raw_nlv)])) < 1.0
        except (TypeError, ValueError, KeyError):
            consistent = None
    verified = {
        "broker_linked": True,
        "snapshot_checked": True,
        "consistency": "pass" if consistent else ("n/a" if consistent is None else "fail"),
        "last_update": sig.get("generated_at"),
    }

    return {
        "holding": holding, "weight": sig.get("weight"),
        "cash": sig.get("cash"), "spy_trend": sig.get("spy_trend"),
        "best": sig.get("best"), "best_score": sig.get("best_score"),
        "signal_date": sig.get("date"), "generated_at": sig.get("generated_at"),
        "recent": recent, "switches_in_window": switches,
        "peak_nlv": peak, "position": pos,
        "nlv": nlv, "nlv_prev": nlv_prev, "nlv_day_pct": day_chg,
        "drawdown_pct": dd, "nlv_curve": curve[-120:],
        "nlv_asof": curve[-1]["date"] if curve else None,
        "verified_by": "local daily-signal-bridge state (IBKR-backed)",
        "verified": verified,
    }


def build_journal(state, hist_cache, trades_path, risk_pct):
    """Derive round-trip trades from the bridge's switch history + price
    history, accumulate them in a VM-side trades.json, and compute journal
    stats (win rate, profit factor, expectancy in R, ...). Private only."""
    sig = state.get("signal") or {}
    recent = sig.get("recent") or []
    holding = sig.get("holding")
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        with open(trades_path) as f:
            trades = json.load(f)
    except (OSError, ValueError):
        trades = []

    if not trades and recent:                       # seed from bridge history
        runs = []
        for date, sym in recent:
            if runs and runs[-1]["sym"] == sym:
                runs[-1]["exit"] = date
            else:
                runs.append({"sym": sym, "entry": date, "exit": date})
        for i, r in enumerate(runs):
            nxt = runs[i + 1]["entry"] if i + 1 < len(runs) else None
            r["exit"] = nxt or today
            r["closed"] = nxt is not None
        trades = runs
    elif trades:                                     # close open run on switch
        last = trades[-1]
        if last.get("closed") is False and holding:
            if last["sym"] != holding:
                last["exit"] = today; last["closed"] = True
                trades.append({"sym": holding, "entry": today,
                               "exit": today, "closed": False})
            else:
                last["exit"] = today

    for r in trades:                                 # per-trade return from prices
        if "ret" in r:
            continue
        h = hist_cache.get(r["sym"])
        if h:
            # history is a list of {'d': date, 'c': close, ...} oldest-first
            ent = [x["c"] for x in h if x["d"] >= r["entry"]]
            ex = [x["c"] for x in h if x["d"] <= (r["exit"] or today)]
            if ent and ex and ent[0]:
                r["ret"] = round((ex[-1] / ent[0] - 1) * 100, 2)
            else:
                r["ret"] = None
        else:
            r["ret"] = None

    try:                                            # persist (best-effort)
        os.makedirs(os.path.dirname(trades_path), exist_ok=True)
        with open(trades_path, "w") as f:
            json.dump(trades, f)
    except OSError:
        pass

    closed = [t for t in trades if t.get("closed") and t.get("ret") is not None]
    wins = [t for t in closed if t["ret"] > 0]
    losses = [t for t in closed if t["ret"] <= 0]
    n = len(closed)
    win_rate = round(100 * len(wins) / n, 1) if n else None
    avg_win = round(statistics.mean([t["ret"] / risk_pct for t in wins]), 2) if wins else None
    avg_loss = round(statistics.mean([t["ret"] / risk_pct for t in losses]), 2) if losses else None
    pf = None
    if wins and losses:
        gw = sum(t["ret"] for t in wins); gl = abs(sum(t["ret"] for t in losses))
        pf = round(gw / gl, 2) if gl else None
    expectancy = round(statistics.mean([t["ret"] / risk_pct for t in closed]), 2) if closed else None
    total_R = round(sum(t["ret"] / risk_pct for t in closed), 2) if closed else None
    open_t = next((t for t in reversed(trades) if not t.get("closed")), None)
    return {"trades": trades, "closed_n": n, "wins": len(wins),
            "win_rate": win_rate, "avg_win_R": avg_win, "avg_loss_R": avg_loss,
            "profit_factor": pf, "expectancy_R": expectancy, "total_R": total_R,
            "open": open_t}

def build_discipline(state, hist_cache, direction, journal=None):
    """Position sizing presets + risk context + trade journal (private)."""
    # Size off the *current* NLV when the bridge has logged it; peak equity
    # would over-size after a drawdown.
    eq = None
    raw_nlv = state.get("nlv") or {}
    if isinstance(raw_nlv, dict) and raw_nlv:
        try:
            eq = float(raw_nlv[max(raw_nlv)])
        except (TypeError, ValueError, KeyError):
            eq = None
    if not eq:
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

    # 10-EMA exit discipline for the currently held name (KovaView rule)
    sig = state.get("signal") or {}
    held = sig.get("holding")
    exit_sig = None
    if held:
        hh = hist_cache.get(held)
        if hh:
            c = closes_of(hh)
            if len(c) >= 10:
                ema = c[-1]; k = 2 / 11
                for v in c[-10:]:
                    ema = v * k + ema * (1 - k)
                exit_sig = {"sym": held, "price": round(c[-1], 2),
                            "ema10": round(ema, 2),
                            "status": "HOLD" if c[-1] >= ema else "EXIT (broke 10-EMA)",
                            "above": c[-1] >= ema}

    # daily P&L W/L from the NLV series
    daily = {"up": 0, "down": 0, "flat": 0}
    if isinstance(raw_nlv, dict) and raw_nlv:
        ser = sorted(raw_nlv.items())
        for i in range(1, len(ser)):
            d = ser[i][1] - ser[i - 1][1]
            daily["up" if d > 0 else ("down" if d < 0 else "flat")] += 1

    out = {"equity_base": eq, "risk_pct": risk_pct, "note": note,
           "posture_used": posture, "examples": examples,
           "rule": "shares = (equity x risk%) / (entry - structural stop)",
           "exit_signal": exit_sig,
           "daily_pnl": daily,
           "heat_pct": risk_pct, "heat_cap": 2.0}
    if journal:
        out["journal"] = {
            "closed_n": journal["closed_n"], "wins": journal["wins"],
            "win_rate": journal["win_rate"], "avg_win_R": journal["avg_win_R"],
            "avg_loss_R": journal["avg_loss_R"], "profit_factor": journal["profit_factor"],
            "expectancy_R": journal["expectancy_R"], "total_R": journal["total_R"],
            "open": journal["open"], "recent": journal["trades"][-6:],
        }
    return out


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "web", "snapshot.json"))
    ap.add_argument("--signal", default="/opt/daily-signal-bridge/signal_target.json")
    ap.add_argument("--equity", default="/opt/daily-signal-bridge/equity_state.json")
    ap.add_argument("--nlv", default="/opt/daily-signal-bridge/daily_nlv.json",
                    help="daily NLV series written by the bridge (private only)")
    ap.add_argument("--trades", default="/opt/peter-review/trades.json",
                    help="VM-side accumulated trade journal (private only)")
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
    state = read_state({"signal": args.signal, "equity": args.equity,
                        "nlv": args.nlv})

    # posture-scaled unit risk (shared by sizer + journal R-multiple)
    posture = direction.get("posture", 50)
    risk_pct = 1.0 if posture >= 70 else (0.6 if posture >= 50 else 0.3)

    journal = None
    if not args.public:
        try:
            journal = build_journal(state, hist_cache, args.trades, risk_pct)
        except Exception:
            journal = None

    snap = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "generated_at_local": (datetime.now(timezone.utc) +
                               timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S") + " CST",
        "macro": build_macro(),
        "events": build_events(),
        "direction": direction,
        "rotation": build_rotation(hist_cache),
        "kova": build_kova(hist_cache, spy_c),
        "universe": build_universe_doc(),
        "leveraged": build_leveraged(hist_cache, spy_c),
        "discipline": build_discipline(state, hist_cache, direction, journal),
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
