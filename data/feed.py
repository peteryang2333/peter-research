"""Unified market-data feed.

Primary sources (no Yahoo, no rate-limit):
  - stockanalysis.com  -> daily history (OHLCV)
  - api.nasdaq.com     -> real-time quote (stocks/etf/crypto)

All functions degrade to None on failure so the UI can fall back to sample
data instead of crashing. A tiny TTL cache avoids hammering the endpoints.
"""
from __future__ import annotations
import time
import json
import threading
import requests

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
TIMEOUT = 20
_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_TTL = 300  # seconds
_LOCK = threading.Lock()


def _get(url: str, headers: dict | None = None, params: dict | None = None) -> dict | None:
    try:
        r = requests.get(url, headers=headers or {"User-Agent": UA}, params=params,
                         timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def _cached(key: str, ttl: int, fn):
    now = time.time()
    with _LOCK:
        if key in _CACHE and now - _CACHE[key][0] < ttl:
            return _CACHE[key][1]
    val = fn()
    with _LOCK:
        _CACHE[key] = (now, val)
    return val


def get_history(ticker: str, rng: str = "1Y", period: str = "Daily") -> list[dict] | None:
    """Return list of {date:str, close:float} oldest->newest, or None."""
    def fn():
        url = f"https://stockanalysis.com/api/symbol/s/{ticker.lower()}/history"
        j = _get(url, params={"range": rng, "period": period})
        if not j:
            return None
        rows = j.get("data") or j.get("history") or (j.get("status") or {}).get("data")
        if not rows:
            return None
        out = []
        for row in rows:
            try:
                out.append({"date": str(row["t"]), "close": float(row["c"]),
                            "volume": float(row["v"]) if row.get("v") is not None else None})
            except (KeyError, TypeError, ValueError):
                continue
        return out if out else None
    return _cached(f"hist:{ticker}:{rng}:{period}", _CACHE_TTL, fn)


def get_quote(ticker: str) -> dict | None:
    """Return {price, change, pct} or None. Tries stock then etf then crypto."""
    def fn():
        sym = ticker.upper()
        for asset in ("etf", "stocks", "crypto"):
            url = f"https://api.nasdaq.com/api/quote/{sym}/info?assetclass={asset}"
            j = _get(url, headers={"User-Agent": UA, "Accept": "application/json"})
            d = (j or {}).get("data")
            if isinstance(d, dict):
                pd = d.get("primaryData") or {}
                price = _num(pd.get("lastSalePrice") or d.get("lastSalePrice"))
                chg = _num(pd.get("netChange") or d.get("netChange"))
                pct = _num(pd.get("percentageChange") or d.get("percentageChange"))
                if price is not None:
                    return {"price": price, "change": chg, "pct": pct}
        # fallback: stockanalysis quote
        j = _get(f"https://stockanalysis.com/api/symbol/{ticker.lower()}/info")
        if j and isinstance(j.get("data"), dict):
            d = j["data"]
            price = _num(d.get("close") or d.get("price"))
            pct = _num(d.get("change_p") or d.get("pct"))
            if price is not None:
                return {"price": price, "change": None, "pct": pct}
        return None
    return _cached(f"quote:{ticker}", 60, fn)


def _num(x) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace("$", "").replace("%", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def last_close(ticker: str) -> float | None:
    h = get_history(ticker, "1M", "Daily")
    if h:
        return h[-1]["close"]
    return None
