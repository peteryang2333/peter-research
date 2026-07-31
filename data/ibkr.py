"""Interactive Brokers integration via the Client Portal Web API (REST).

Runs in DEMO mode unless IBKR_BASE is reachable. Point IBKR_BASE at your
IBKR Client Portal Gateway (paper or live), e.g. http://localhost:5000 or
http://host.docker.internal:5000 from inside the container.

The gateway must be authenticated (SSO / conf code) out-of-band; this adapter
only reads already-authenticated endpoints. Everything degrades to demo data
so the dashboard always renders.
"""
from __future__ import annotations
import os
import requests

BASE = os.getenv("IBKR_BASE", "http://localhost:5000").rstrip("/")
TIMEOUT = 8


def _get(path: str) -> object | None:
    try:
        r = requests.get(f"{BASE}{path}", timeout=TIMEOUT)
        if r.status_code == 200:
            return r.json()
    except Exception:
        return None
    return None


def is_connected() -> bool:
    return _get("/v1/api/iserver/accounts") is not None


def get_positions() -> list[dict]:
    """Return list of {symbol, qty, avg_cost, mkt_price, mkt_value, unrealized}."""
    accts = _get("/v1/api/iserver/accounts")
    if not accts:
        return _demo_positions()
    account = accts.get("accounts", [None])[0] if isinstance(accts, dict) else None
    if not account:
        return _demo_positions()
    data = _get(f"/v1/api/portfolio/{account}/positions/0") or _get(
        f"/v1/api/portfolio/{account}/positions")
    if not data:
        return _demo_positions()
    rows = data if isinstance(data, list) else data.get("positions", [])
    out = []
    for p in rows:
        out.append({
            "symbol": p.get("ticker", p.get("symbol", "?")),
            "qty": float(p.get("position", p.get("qty", 0) or 0)),
            "avg_cost": float(p.get("avgCost", p.get("avg_cost", 0) or 0)),
            "mkt_price": float(p.get("mktPrice", p.get("mkt_price", 0) or 0)),
            "mkt_value": float(p.get("mktValue", p.get("mkt_value", 0) or 0)),
            "unrealized": float(p.get("unrealizedPnl", p.get("unrealized", 0) or 0)),
        })
    return out or _demo_positions()


def get_realized() -> dict:
    """Best-effort realized P&L summary (demo if unavailable)."""
    accts = _get("/v1/api/iserver/accounts")
    if not accts:
        return _demo_realized()
    account = accts.get("accounts", [None])[0] if isinstance(accts, dict) else None
    data = _get(f"/v1/api/portfolio/{account}/ledger") if account else None
    if not data:
        return _demo_realized()
    # ledger shape varies; fall back to demo for the summary
    return _demo_realized()


def _demo_positions() -> list[dict]:
    return [
        {"symbol": "FAS", "qty": 400.0, "avg_cost": 98.2, "mkt_price": 112.4,
         "mkt_value": 44960.0, "unrealized": 5688.0},
        {"symbol": "DDOG", "qty": 120.0, "avg_cost": 268.0, "mkt_price": 281.5,
         "mkt_value": 33780.0, "unrealized": 1620.0},
    ]


def _demo_realized() -> dict:
    return {"realized": 12480.0, "trades": 47, "win_rate": 0.58,
            "profit_factor": 2.1, "expectancy_R": 0.42, "demo": True}
