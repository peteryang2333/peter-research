"""Global macro heatmap data via World Bank Open Data API (no key required)."""
from __future__ import annotations
import requests
from data.feed import _cached

UA = "Mozilla/5.0"
WB = "https://api.worldbank.org/v2/country"

# indicator -> human label
INFLATION = "FP.CPI.TOTL.ZG"   # inflation, consumer prices (annual %)
GDP_GROWTH = "NY.GDP.MKTP.KD.ZG"
# A curated set of economies for the heatmap
COUNTRIES = [
    "USA", "CHN", "IND", "JPN", "DEU", "GBR", "FRA", "BRA", "MEX", "TUR",
    "IRN", "EGY", "THA", "CHE", "CAN", "AUS", "ZAF", "RUS", "IDN", "ARG",
    "KOR", "ITA", "ESP", "SAU", "NGA",
]


def _wb(indicator: str, year: int | None = None) -> dict[str, float]:
    """Return {iso3: latest_value} for the indicator."""
    url = f"{WB}/{'%3B'.join(COUNTRIES)}/indicator/{indicator}"
    params = {"format": "json", "per_page": 500}
    if year:
        params["date"] = year
    try:
        r = requests.get(url, headers={"User-Agent": UA}, params=params, timeout=25)
        if r.status_code != 200:
            return {}
        payload = r.json()
        if not isinstance(payload, list) or len(payload) < 2:
            return {}
        rows = payload[1]
        out: dict[str, float] = {}
        for row in rows:
            if row.get("value") is None:
                continue
            try:
                out[row["countryiso3code"]] = float(row["value"])
            except (TypeError, ValueError, KeyError):
                continue
        return out
    except Exception:
        return {}


def inflation_latest() -> dict[str, float]:
    """Latest available year per country (World Bank returns most recent first)."""
    return _cached("wb_inflation", 3600, lambda: _wb(INFLATION))


def inflation_year(year: int) -> dict[str, float]:
    return _cached(f"wb_inflation_{year}", 3600, lambda: _wb(INFLATION, year))


def gdp_growth_latest() -> dict[str, float]:
    return _cached("wb_gdp", 3600, lambda: _wb(GDP_GROWTH))
