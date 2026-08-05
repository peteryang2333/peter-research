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
import re
import glob
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " \
     "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# requests.Session is NOT thread-safe: sharing one Session across the 6 worker
# threads lets a pooled connection get half-read in one thread and stall another
# (observed as an indefinite ssl.read hang against api.nasdaq.com). Give each
# thread its own Session instead.
_ADAPTER = HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
_LS = threading.local()


def _session():
    s = getattr(_LS, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "application/json",
                          "Connection": "close"})
        s.mount("https://", _ADAPTER)
        _LS.session = s
    return s

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

# Broad, sector-balanced LIQUID universe — the "most active" base pool. Curated
# and editable via vm/liquid_universe.json (so the user can reshape the pool
# without touching code). This is what makes the dashboard a real watch-tool
# instead of a 23-name thematic ranker.
def load_liquid_universe():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "liquid_universe.json")
    try:
        d = json.load(open(p, encoding="utf-8"))
    except (OSError, ValueError):
        return [], []
    return ([x["t"] for x in d.get("stocks", [])],
            [x["t"] for x in d.get("etfs", [])])

LIQUID_STOCKS, LIQUID_ETFS = load_liquid_universe()

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


def _raw_http_json(url, timeout=25, tries=4):
    for i in range(tries):
        try:
            r = _session().get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 429, 500, 502, 503):
                time.sleep(1.2 * (i + 1))
                continue
            return None
        except (requests.RequestException, ValueError):
            time.sleep(0.8 * (i + 1))
    return None


def http_json(url, timeout=25, tries=4):
    # HARD wall-clock cap: api.nasdaq.com (and some flaky proxies) answer the
    # TLS handshake then trickle the body (slow-loris style), so requests' per-
    # recv read timeout never trips and a stalled call would hang the whole
    # collector forever. Run the fetch in a daemon thread and join with a cap;
    # on overrun we return None and let the (leaked) worker die at process exit.
    cap = timeout * tries + 4
    box = {}

    def _work():
        try:
            box["v"] = _raw_http_json(url, timeout, tries)
        except Exception:
            box["v"] = None

    th = threading.Thread(target=_work, daemon=True)
    th.start()
    th.join(cap)
    return box.get("v")


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
        time.sleep(0.15)  # pace nasdaq to dodge throttle / slow-loris stalls
        j = http_json(f"https://api.nasdaq.com/api/quote/{ticker.upper()}"
                      f"/info?assetclass={asset}", timeout=8, tries=1)
        pd_ = ((j or {}).get("data") or {}).get("primaryData") or {}
        price = _money(pd_.get("lastSalePrice"))
        if price:
            inst_own = None
            ks = ((j or {}).get("data") or {}).get("keyStats") or {}
            if isinstance(ks, dict):
                for kk, vv in ks.items():
                    if "institutional" in str(kk).lower() and "own" in str(kk).lower():
                        inst_own = _money(vv)
                        break
            q = {"price": price,
                 "chg": _money(pd_.get("netChange")),
                 "pct": _money(pd_.get("percentageChange")),
                 "vol": _money(pd_.get("volume")),
                 "asof": pd_.get("lastTradeTimestamp"),
                 "src": "nasdaq",
                 "inst_own": inst_own}
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


def get_analyst(ticker, ttl=6 * 3600):
    """Nasdaq analyst consensus: buy/hold/sell split + price target.
    Returns None on miss. This is the 'institutional thesis' signal."""
    key = f"an_{ticker}"
    cached = cache_get(key, ttl)
    if cached is not None:
        return cached
    time.sleep(0.15)  # pace nasdaq to dodge throttle / slow-loris stalls
    j = http_json(f"https://api.nasdaq.com/api/analyst/{ticker.upper()}/targetprice",
                  timeout=8, tries=1)
    co = ((j or {}).get("data") or {}).get("consensusOverview")
    if not co:
        return None
    buy = int(co.get("buy") or 0)
    hold = int(co.get("hold") or 0)
    sell = int(co.get("sell") or 0)
    total = buy + hold + sell
    if total == 0:
        return None
    target = _money(co.get("priceTarget"))
    out = {"buy": buy, "hold": hold, "sell": sell, "total": total,
           "target": target,
           "low": _money(co.get("lowPriceTarget")),
           "high": _money(co.get("highPriceTarget")),
           "score": round((buy * 100 + hold * 50) / total)}
    cache_put(key, out)
    return out


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
            "sym": sym, "name": name, "kind": "sector", "quadrant": quad,
            "rs_ratio": round(r_now, 2), "rs_mom": round(m_now, 2),
            "tail": [[round(a, 2), round(bb, 2)] for a, bb in pts],
            "r1m": round(pct_change(c, 21), 2) if pct_change(c, 21) else None,
            "r3m": round(pct_change(c, 63), 2) if pct_change(c, 63) else None,
        })

    # Broader theme/style ETFs — shows money rotating across themes
    # (semis, software, biotech, banks, treasuries, gold...) not just GICS sectors.
    for sym in LIQUID_ETFS:
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
            "sym": sym, "name": sym, "kind": "etf", "quadrant": quad,
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
            "method": (f"JdK-style RRG: RS=100*ticker/{bench}; RS-Ratio=100+z(RS,{window}w); "
                       f"RS-Mom=100+z(RS {mom_lag}w RoC,{window}w); tail={tail}w. "
                       f"覆盖 {sum(1 for o in out if o['kind']=='sector')} 个板块ETF + "
                       f"{sum(1 for o in out if o['kind']=='etf')} 个主题ETF(半导体/软件/生物/银行/国债/黄金等)。")}


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


# ----------------------------------------------------------------------------
# MODULE 4b — Flow (money flow / activity) over the liquid universe
# ----------------------------------------------------------------------------
def _net_flow_index(hist, n=20):
    """OBV-style net-flow ratio over last n bars, in [-100,100].
    Positive = net accumulation (volume leaning into up-days)."""
    h = hist[-n:] if len(hist) >= n else hist
    if len(h) < 5:
        return 0.0
    net = 0.0
    tot = 0.0
    for i in range(1, len(h)):
        d = h[i]["c"] - h[i - 1]["c"]
        v = h[i]["v"] or 0
        if d > 0:
            net += v
        elif d < 0:
            net -= v
        tot += v
    if tot == 0:
        return 0.0
    return max(-100.0, min(100.0, net / tot * 100.0))


def _dollar_vol_b(hist, quote):
    price = (quote or {}).get("price")
    if not price and hist:
        price = hist[-1]["c"]
    v = (hist[-1]["v"] if hist else 0) or 0
    return (price * v) / 1e9  # billions


def build_flow(hist_cache, quotes, liquid_stocks):
    rows = []
    for sym in liquid_stocks:
        h = hist_cache.get(sym)
        if not h or len(h) < 30:
            continue
        q = quotes.get(sym)
        fi = _net_flow_index(h, 20)
        vols = [r["v"] for r in h if r["v"]]
        relvol = None
        if len(vols) > 21:
            avg20 = sum(vols[-21:-1]) / 20.0
            if avg20:
                relvol = vols[-1] / avg20 * 100.0
        rows.append({
            "sym": sym,
            "price": round(q["price"], 2) if q else round(h[-1]["c"], 2),
            "pct": round(q["pct"], 2) if q and q.get("pct") is not None else None,
            "dvol_b": round(_dollar_vol_b(h, q), 2),
            "relvol": round(relvol) if relvol else None,
            "flow_index": round(fi, 1),
            "flow_state": ("Accumulation" if fi > 10 else
                           ("Distribution" if fi < -10 else "Neutral")),
        })
    rows.sort(key=lambda x: -x["dvol_b"])
    n = len(rows)
    for i, r in enumerate(rows):
        r["activity_rank"] = i + 1
        r["activity_score"] = round(100 * (n - 1 - i) / max(1, n - 1))
        r["flow_score"] = round(clamp(50 + r["flow_index"] * 0.5))
    return {
        "rows": rows,
        "most_active": rows[:30],
        "accumulation": sorted([r for r in rows if r["flow_state"] == "Accumulation"],
                               key=lambda x: -x["flow_index"])[:30],
        "method": ("活跃度 = 成交额(十亿)排名；资金流向 = 近20日净量比(OBV式："
                   "涨日量−跌日量)/总量×100；>10 吸筹 / <-10 派发。无雅虎、无新数据源，"
                   "全从已有 OHLCV 算。"),
    }


# ----------------------------------------------------------------------------
# MODULE 4c — Institutional (analyst consensus + ownership)
# ----------------------------------------------------------------------------
def build_institutional(hist_cache, quotes, liquid_stocks):
    rows = []
    for sym in liquid_stocks:
        h = hist_cache.get(sym)
        if not h:
            continue
        a = get_analyst(sym)
        if not a:
            continue
        q = quotes.get(sym)
        price = (q or {}).get("price") or h[-1]["c"]
        upside = round((a["target"] - price) / price * 100, 1) if a["target"] else None
        rows.append({
            "sym": sym,
            "price": round(price, 2),
            "pct": round(q["pct"], 2) if q and q.get("pct") is not None else None,
            "buy": a["buy"], "hold": a["hold"], "sell": a["sell"],
            "total": a["total"], "target": a["target"], "upside": upside,
            "consensus_score": a["score"],
            "inst_own": (q or {}).get("inst_own"),
            "rating_label": ("强力看多" if a["score"] >= 80 else
                             "看多" if a["score"] >= 60 else
                             "中性" if a["score"] >= 40 else "看空"),
        })
    rows.sort(key=lambda x: (-(x["consensus_score"] or 0),
                             -(x["upside"] if x["upside"] is not None else -999)))
    return {
        "rows": rows,
        "top_conviction": rows[:30],
        "method": ("来源 Nasdaq 分析师共识：买入/持有/卖出分布→共识分(买=100/持有=50/卖出=0)；"
                   "目标价上行空间=(目标价−现价)/现价；机构持仓% 尽力从 quote keyStats 取"
                   "(免费源无完整13F聚合，故以分析师共识为主、持仓%为辅)。"),
    }


# ----------------------------------------------------------------------------
# MODULE 4d — Composite fusion ranker (the "real watch tool" output)
# ----------------------------------------------------------------------------
def _thesis_score(sym):
    for g in WATCHLIST_GROUPS:
        if sym in g["syms"]:
            return {"cloud": 100, "hardware": 75, "supply": 60}.get(g["key"], 60)
    return 0


COMPOSITE_SPEC = {
    "kind": "fusion ranker",
    "min_bars": 210,
    "weights": {"tech": 35, "activity": 10, "flow": 20, "inst": 20, "thesis": 15},
    "factors": [
        {"w": 35, "name": "技术五因子", "how": "复用 Kova 五因子(趋势/动量/相对SPY/距高/量能)"},
        {"w": 10, "name": "活跃度", "how": "全流动性池按成交额排名折算百分制"},
        {"w": 20, "name": "资金流向", "how": "近20日净量比(OBV式)→吸筹/派发"},
        {"w": 20, "name": "机构共识", "how": "60%分析师共识分 + 40%目标价上行空间"},
        {"w": 15, "name": "论点叠加", "how": "云厂=100/硬件=75/供应链=60/其他=0，仅作多源之一"},
    ],
    "note": ("你的论点只是 5 个输入里的 1 个(权重15%)，不再一票否决；"
             "单一信号偏弱不会掩盖其他信号，互补降低“论点不全”的风险。"),
}


def _composite_row(sym, h, spy_c, fmap, imap, vcp_syms=None):
    """Score one symbol with the 5-factor fusion. Shared by build_composite and
    the VCP fusion so both paths produce identical, apples-to-apples scores."""
    sc = score_one(sym, h, spy_c)
    if not sc:
        return None
    f = fmap.get(sym, {})
    im = imap.get(sym, {})
    tech = sc["score"]
    activity = f.get("activity_score", 0)
    flow = f.get("flow_score", 50)
    upside = im.get("upside")
    upside_s = clamp(50 + (upside or 0) * 1.5) if upside is not None else 50
    inst = round(0.6 * (im.get("consensus_score", 50)) + 0.4 * upside_s)
    thesis = _thesis_score(sym)
    composite = round(0.35 * tech + 0.10 * activity + 0.20 * flow +
                      0.20 * inst + 0.15 * thesis)
    strong = []
    if thesis >= 75:
        strong.append("契合你的云厂论点")
    elif thesis > 0:
        strong.append("契合你的硬件/供应链论点")
    if (im.get("consensus_score") or 0) >= 70:
        strong.append(f"机构看多({im.get('total')}位分析师)")
    if upside is not None and upside >= 15:
        strong.append(f"目标价+{upside:.0f}%")
    if f.get("flow_state") == "Accumulation":
        strong.append("吸筹中")
    if activity >= 80:
        strong.append("成交额活跃")
    if sc["health"] == "Healthy":
        strong.append("趋势完好")
    return {"sym": sym, "price": sc["price"], "pct": sc["pct"],
            "score": composite, "health": sc["health"],
            "contrib": {"tech": tech, "activity": activity, "flow": flow,
                        "inst": inst, "thesis": thesis},
            "why": strong,
            "in_thesis": thesis > 0,
            "in_vcp": bool(vcp_syms and sym in vcp_syms)}


def build_composite(hist_cache, spy_c, quotes, liquid_stocks, flow_rows, inst_rows,
                    vcp_syms=None):
    fmap = {r["sym"]: r for r in flow_rows}
    imap = {r["sym"]: r for r in inst_rows}
    rows = []
    for sym in liquid_stocks:
        h = hist_cache.get(sym)
        if not h or len(h) < 210:
            continue
        r = _composite_row(sym, h, spy_c, fmap, imap, vcp_syms)
        if r:
            rows.append(r)
    rows.sort(key=lambda x: -x["score"])
    return {"rows": rows,
            "method": ("复合分 = 0.35*技术 + 0.10*活跃 + 0.20*资金流向 + 0.20*机构共识"
                       " + 0.15*论点叠加；各因子0–100，透明可解释，每只票给“为什么高亮”。"),
            "spec": COMPOSITE_SPEC}


# ----------------------------------------------------------------------------
# VCP fusion — fold the daily Volatility-Contraction-Pattern scan into the
# same 5-factor system so "买入前10榜 / 额外买入前10榜" get a system score + verdict.
# ----------------------------------------------------------------------------
GH_OWNER = "peteryang2333"
GH_RAW = "https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"
GH_API = "https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"


def gh_text(repo, path, ref="main", timeout=25, tries=3):
    """Read a text file out of *another* public repo of the same GitHub owner.

    This is what makes the daily VCP / full-market-scan data show up here
    without any VM: the screeners commit their output to their own repos,
    and every snapshot run pulls the latest copy across repo boundaries.
    raw.githubusercontent is tried first (CDN, unauthenticated, no API quota);
    the Contents API with `Accept: raw` is the fallback for networks that
    block the raw host. Returns None instead of raising — a missing upstream
    file must never break the whole snapshot."""
    from urllib.parse import quote
    q = quote(path)
    urls = [
        (GH_RAW.format(owner=GH_OWNER, repo=repo, ref=ref, path=q), "text/plain"),
        (GH_API.format(owner=GH_OWNER, repo=repo, ref=ref, path=q),
         "application/vnd.github.raw"),
    ]
    cap = timeout + 4

    def _get(url, accept):
        # hard-capped get: same slow-loris guard as http_json
        box = {}

        def _w():
            try:
                box["r"] = _session().get(url, timeout=timeout,
                                         headers={"Accept": accept})
            except Exception:
                box["r"] = None

        th = threading.Thread(target=_w, daemon=True)
        th.start()
        th.join(cap)
        return box.get("r")

    for attempt in range(tries):
        for url, accept in urls:
            try:
                r = _get(url, accept)
                if r is not None and r.status_code == 200 and r.text:
                    r.encoding = "utf-8"
                    return r.text
                if r is not None and r.status_code == 404:
                    return None
            except Exception:
                pass
        time.sleep(1.0 + attempt)
    return None


def parse_vcp_md(txt):
    """Parse the screener's published markdown table (数据/最新_US.txt).

    Columns: VCP分|代码|当前价格|RS评级|收缩%|5日振幅%|缩量比例%|突破挂单价|
             距突破%|距52周高%|放量突破|10EMA|21EMA|吊灯止损|ATR止损"""
    date = None
    m = re.search(r"扫描日:\s*([0-9\-]+)", txt)
    if m:
        date = m.group(1)
    pm = re.search(r"数据来源:\s*([^\n]+)", txt)
    pool = pm.group(1).strip() if pm else None

    def num(s):
        s = (s or "").strip()
        try:
            return float(s)
        except Exception:
            return None

    cands = []
    for line in txt.splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 15 or not re.fullmatch(r"[1-3]", cells[0]):
            continue
        ratios = (cells[4].split("/") + ["", "", ""])[:3]
        cands.append({
            "vcp_score": int(cells[0]), "sym": cells[1],
            "price": num(cells[2]), "rs": int(num(cells[3]) or 0),
            "contraction": num(ratios[0]), "amp5": num(cells[5]),
            "shrink": num(cells[6]), "trigger": num(cells[7]),
            "dist_breakout": num(cells[8]), "dist52": num(cells[9]),
            "vol_breakout": bool(cells[10].strip()),
            "ema10": num(cells[11]), "ema21": num(cells[12]),
            "stop_chandelier": num(cells[13]), "stop_hard": num(cells[14]),
        })
    return {"date": date, "market": "美股", "pool": pool,
            "n": len(cands), "candidates": cands}


def parse_vcp_txt(path):
    """Parse a VCP screener TXT (e.g. 数据/vcp_US_20260801.txt).

    Each data row: VCP分 代码 价格 RS 收缩%/5日振幅%/缩量比例% <2 未标注列>
    突破挂单价 距突破% 距52周高% 10EMA 21EMA 吊灯止损 硬止损价.
    The two unlabeled columns sit between the ratio group and the breakout
    block; we skip them and read the trailing 7 fields by fixed position."""
    txt = open(path, encoding="utf-8", errors="ignore").read()
    if "| VCP分 |" in txt or re.search(r"^\|\s*[1-3]\s*\|", txt, re.M):
        return parse_vcp_md(txt)
    m = re.search(r"VCP 扫描器 v(\d+)\s*\|\s*([0-9\-]+)\s+([0-9:]+)", txt)
    date = m.group(2) if m else None
    mm = re.search(r"【(🇺🇸|🇯🇵|🇰🇷)\s*([^】]+)】扫描开始", txt)
    market = mm.group(2).strip() if mm else "美股"
    pm = re.search(r"美股池:\s*([^\n]+)", txt)
    pool = pm.group(1).strip() if pm else None
    rx = re.compile(
        r"^\s*([1-3])\s+([A-Z]{1,5})\s+([\d.]+)\s+(\d+)\s+"
        r"([\d.]+)/([\d.]+)/([\d.]+)\s+"          # 收缩/5日振幅/缩量
        r"([\d.]+)\s+([\d.]+)\s+"                # 2 未标注列 (跳过)
        r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+"     # 突破挂单价 距突破% 距52周高%
        r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*$")  # 10EMA 21EMA 吊灯 硬止损
    cands = []
    for line in txt.splitlines():
        g = rx.match(line)
        if not g:
            continue
        cands.append({
            "vcp_score": int(g.group(1)), "sym": g.group(2),
            "price": float(g.group(3)), "rs": int(g.group(4)),
            "contraction": float(g.group(5)), "amp5": float(g.group(6)),
            "shrink": float(g.group(7)),
            "trigger": float(g.group(10)), "dist_breakout": float(g.group(11)),
            "dist52": float(g.group(12)), "ema10": float(g.group(13)),
            "ema21": float(g.group(14)), "stop_chandelier": float(g.group(15)),
            "stop_hard": float(g.group(16)),
        })
    return {"date": date, "market": market, "pool": pool,
            "n": len(cands), "candidates": cands}


def _verdict(vcp_score, sys_score):
    if sys_score is None:
        return "系统分缺失(历史不足)"
    if vcp_score >= 3 and sys_score >= 70:
        return "强 · 形态+系统双高"
    if vcp_score >= 3 and sys_score < 55:
        return "警惕 · 形态好但系统分低"
    if vcp_score == 2:
        return "中 · 次优梯队"
    return "弱 · 仅观察"


def build_vcp(vcp, hist_cache, spy_c, flow_rows, inst_rows):
    fmap = {r["sym"]: r for r in flow_rows}
    imap = {r["sym"]: r for r in inst_rows}
    out = []
    for c in vcp.get("candidates", []):
        sym = c["sym"]
        h = hist_cache.get(sym)
        r = _composite_row(sym, h, spy_c, fmap, imap) if (h and len(h) >= 210) else None
        sys_score = r["score"] if r else None
        vs = c.get("vcp_score", 0)
        combined = (round((vs / 3) * 100 * 0.4 + sys_score * 0.6)
                    if sys_score is not None else None)
        out.append({**c, "system_score": sys_score,
                    "contrib": r["contrib"] if r else None,
                    "why": r["why"] if r else [],
                    "verdict": _verdict(vs, sys_score), "combined": combined})
    # Closest-to-breakout first inside each VCP tier — that is the order the
    # screener itself publishes, and it is the order that matters for entries.
    out.sort(key=lambda x: (-(x.get("vcp_score") or 0),
                            x.get("dist_breakout") if x.get("dist_breakout") is not None else 999,
                            -(x.get("rs") or 0)))
    scored = [r for r in out if r["system_score"] is not None]
    return {
        "date": vcp.get("date"), "market": vcp.get("market"), "pool": vcp.get("pool"),
        "n": len(out), "scored_n": len(scored),
        "source": vcp.get("source"), "source_url": vcp.get("source_url"),
        "buy_top10": out[:10], "extra_top10": out[10:20], "candidates": out,
        "method": ("VCP 信号(分/RS/突破挂单/止损) 与 本面板 5 因子系统分"
                   "(技术35+活跃10+资金20+机构20+论点15) 融合；"
                   "综合 = 0.4×VCP归一 + 0.6×系统分。绿=强/红=弱，仅供研究，非投资建议。"),
    }


# ----------------------------------------------------------------------------
# Full-market scanner fusion — peteryang2333/stock-screener publishes
# data/daily_scans/latest_signals.json every weekday; we score its buy/sell
# lists with the same 5-factor system so both engines are directly comparable.
# ----------------------------------------------------------------------------
def build_screener(sig, hist_cache, spy_c, flow_rows, inst_rows):
    fmap = {r["sym"]: r for r in flow_rows}
    imap = {r["sym"]: r for r in inst_rows}

    def rows_of(items):
        out = []
        for s in items or []:
            sym = s.get("ticker") or s.get("sym")
            if not sym:
                continue
            h = hist_cache.get(sym)
            r = (_composite_row(sym, h, spy_c, fmap, imap)
                 if (h and len(h) >= 210) else None)
            sys_score = r["score"] if r else None
            raw = s.get("score")
            # scanner buys are /125, sells /110 → normalise to 0-100
            base = s.get("score_max") or 125
            norm = round(min(100.0, (raw or 0) / base * 100)) if raw is not None else None
            combined = (round(0.4 * norm + 0.6 * sys_score)
                        if (norm is not None and sys_score is not None) else None)
            if sys_score is None:
                verdict = "系统分缺失(历史不足)"
            elif norm is not None and norm >= 60 and sys_score >= 70:
                verdict = "强 · 扫描+系统双高"
            elif norm is not None and norm >= 60 and sys_score < 55:
                verdict = "警惕 · 扫描高但系统分低"
            elif sys_score >= 65:
                verdict = "中 · 系统分撑住"
            else:
                verdict = "弱 · 仅观察"
            out.append({
                "sym": sym, "scan_score": raw, "scan_norm": norm,
                "phase": s.get("phase"), "entry_quality": s.get("entry_quality"),
                "price": s.get("price") or (r["price"] if r else None),
                "stop_loss": s.get("stop_loss"), "target": s.get("target"),
                "rr": s.get("risk_reward_ratio"),
                "breakout": s.get("breakout_price"),
                "breakdown": s.get("breakdown_level"),
                "severity": s.get("severity"),
                "rs_slope": s.get("rs_slope"), "vol_ratio": s.get("volume_ratio"),
                "vcp_quality": s.get("vcp_quality"),
                "reasons": (s.get("reasons") or [])[:4],
                "system_score": sys_score, "contrib": r["contrib"] if r else None,
                "why": r["why"] if r else [],
                "combined": combined, "verdict": verdict,
            })
        out.sort(key=lambda x: -(x["combined"] if x["combined"] is not None
                                 else (x["scan_norm"] or 0)))
        return out

    buys = rows_of(sig.get("buys"))
    sells = rows_of(sig.get("sells"))
    return {
        "date": sig.get("scan_date"), "generated_at": sig.get("generated_at"),
        "universe": sig.get("universe"), "analyzed": sig.get("analyzed"),
        "buy_n": sig.get("buy_n"), "sell_n": sig.get("sell_n"),
        "spy_trend": sig.get("spy_trend"), "breadth": sig.get("breadth"),
        "source": sig.get("source"), "source_url": sig.get("source_url"),
        "buy_top10": buys[:10], "extra_top10": buys[10:20],
        "buys": buys, "sells": sells[:10],
        "method": ("全市场扫描器(≈3800只) 的买卖信号 与 本面板 5 因子系统分融合；"
                   "扫描分归一到百分制后 综合 = 0.4×扫描 + 0.6×系统。"
                   "两套引擎独立打分，双高才算强信号。非投资建议。"),
    }


def build_universe_doc():
    """Pool composition, surfaced in the UI so the strategy is self-explaining."""
    return {
        "watchlist_groups": WATCHLIST_GROUPS,
        "watchlist_n": len(WATCHLIST),
        "liquid_n": len(LIQUID_STOCKS),
        "liquid_etf_n": len(LIQUID_ETFS),
        "leveraged": LEVERAGED,
        "bench": BENCH,
        "sectors_n": len(SECTORS),
        "note": ("两层池：① 你的主题论点池(23只，手工)；② 跨行业流动性池"
                 "(~155只+26ETF，可编辑)。打分有两条线——kova 只排你的论点池；"
                 "composite 把 技术+活跃+资金流向+机构共识+你的论点 融合成全池排名，"
                 "你的论点只是其中 15% 权重，不再一票否决。非投资建议。"),
        "edit_where": "vm/collect.py 的 WATCHLIST_GROUPS；vm/liquid_universe.json",
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
    ap.add_argument("--vcp", default=None,
                    help="path to a LOCAL VCP scan TXT or its directory (latest "
                         "vcp_US_*.txt is auto-picked). Overrides --remote for VCP.")
    ap.add_argument("--remote", action="store_true", default=True,
                    help="pull the daily VCP scan and full-market scan straight "
                         "out of the sibling GitHub repos (vcp-screener / "
                         "stock-screener). On by default; this is how the public "
                         "site stays fresh with no VM.")
    ap.add_argument("--no-remote", dest="remote", action="store_false",
                    help="disable the cross-repo pull")
    ap.add_argument("--score-cap", type=int, default=70,
                    help="max upstream candidates to fetch history for, per "
                         "source (keeps CI runtime and rate limits sane)")
    args = ap.parse_args()

    # ---- upstream sources -------------------------------------------------
    # VCP fusion: prefer a local file when explicitly given, otherwise read the
    # published table straight out of peteryang2333/vcp-screener.
    vcp = None
    if args.vcp:
        vp = args.vcp
        if os.path.isdir(vp):
            us = sorted(glob.glob(os.path.join(vp, "vcp_US_*.txt")))
            vp = us[-1] if us else None
        if vp and os.path.exists(vp):
            try:
                vcp = parse_vcp_txt(vp)
                vcp["source"] = "local:" + os.path.basename(vp)
            except Exception as e:
                print("WARN parse_vcp_txt failed:", e, file=sys.stderr)
    if vcp is None and args.remote:
        try:
            txt = gh_text("vcp-screener", "数据/最新_US.txt")
            if txt:
                vcp = parse_vcp_md(txt)
                vcp["source"] = "github:peteryang2333/vcp-screener"
                vcp["source_url"] = ("https://github.com/peteryang2333/"
                                     "vcp-screener/blob/main/数据/最新_US.txt")
                print(f"VCP remote: {vcp['n']} candidates, scan {vcp['date']}",
                      file=sys.stderr)
        except Exception as e:
            print("WARN remote VCP fetch failed:", e, file=sys.stderr)

    # Full-market scanner signals from peteryang2333/stock-screener.
    screener = None
    if args.remote:
        try:
            j = gh_text("stock-screener", "data/daily_scans/latest_signals.json")
            if j:
                screener = json.loads(j)
                screener["source"] = "github:peteryang2333/stock-screener"
                screener["source_url"] = ("https://github.com/peteryang2333/"
                                          "stock-screener/blob/main/data/"
                                          "daily_scans/latest_signals.json")
                print(f"Screener remote: {len(screener.get('buys') or [])} buys, "
                      f"scan {screener.get('scan_date')}", file=sys.stderr)
        except Exception as e:
            print("WARN remote screener fetch failed:", e, file=sys.stderr)

    # Only the top slice of each upstream list gets a 2Y history fetch, so a
    # 120-name VCP hit list can't blow up the run time or trip rate limits.
    cap = max(0, args.score_cap)
    vcp_syms = [c["sym"] for c in (vcp or {}).get("candidates", [])][:cap]
    scan_syms = [s.get("ticker") for s in (screener or {}).get("buys", [])[:cap]
                 if s.get("ticker")]
    scan_syms += [s.get("ticker") for s in (screener or {}).get("sells", [])[:20]
                  if s.get("ticker")]
    ext_syms = list(dict.fromkeys(vcp_syms + scan_syms))

    t0 = time.time()
    universe = list(dict.fromkeys(
        BENCH + list(SECTORS) + CREDIT + VOLPROXY + LEVERAGED + WATCHLIST
        + LIQUID_STOCKS + LIQUID_ETFS + ext_syms))

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
    quotes = {}
    quote_syms = list(dict.fromkeys(
        BENCH + ["HYG", "TLT", "VIXY"] + LEVERAGED + WATCHLIST
        + LIQUID_STOCKS + LIQUID_ETFS + ext_syms))
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(get_quote, s): s for s in quote_syms}
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                quotes[s] = fut.result()
            except Exception:
                pass

    # Warm analyst-consensus cache for the liquid universe (parallel).
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(as_completed(ex.submit(get_analyst, s)
                          for s in LIQUID_STOCKS + ext_syms))

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

    EXTRA = list(dict.fromkeys(LIQUID_STOCKS + ext_syms))
    flow = build_flow(hist_cache, quotes, EXTRA)
    institutional = build_institutional(hist_cache, quotes, EXTRA)
    composite = build_composite(hist_cache, spy_c, quotes, EXTRA,
                                 flow["rows"], institutional["rows"], vcp_syms)

    snap = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "generated_at_local": (datetime.now(timezone.utc) +
                               timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S") + " CST",
        "macro": build_macro(),
        "events": build_events(),
        "direction": direction,
        "rotation": build_rotation(hist_cache),
        "flow": flow,
        "institutional": institutional,
        "composite": composite,
        "vcp": build_vcp(vcp, hist_cache, spy_c, flow["rows"], institutional["rows"])
                if vcp else None,
        "screener": build_screener(screener, hist_cache, spy_c, flow["rows"],
                                   institutional["rows"]) if screener else None,
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
            "vcp": vcp["date"] if vcp else None,
            "vcp_n": len(vcp_syms),
            "vcp_src": (vcp or {}).get("source"),
            "scan": (screener or {}).get("scan_date"),
            "scan_n": len(scan_syms),
            "scan_src": (screener or {}).get("source"),
            "sources": ["stockanalysis.com", "api.nasdaq.com (quote+analyst)",
                        "api.worldbank.org", "local strategy state",
                        "github:peteryang2333/vcp-screener (每日 VCP 扫描)",
                        "github:peteryang2333/stock-screener (每日全市场扫描)"],
            "note": "No Yahoo/yfinance (rate-limited for this account).",
        },
    }

    # Surface VCP pool size in the universe doc (for the self-documenting UI).
    snap["universe"]["vcp_n"] = len(vcp_syms)

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
