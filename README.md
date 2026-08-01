# Kova-OSS

A self-hosted, six-module market dashboard — an open-source take on the KovaView layout.
Static front end, no build step, no chart library, **no Yahoo / yfinance**, no API keys.

**Live site:** https://peteryang2333.github.io/kova-oss/

## Modules

| # | Module | What it shows | Source |
|---|--------|---------------|--------|
| 01 | Macro | Global CPI heat tiers, hottest / coolest economies | World Bank |
| 02 | Direction | Posture score (Trend / Breadth / Credit / Vol / Leadership) + index, rate, credit and crypto tape | stockanalysis + nasdaq |
| 03 | Rotation | RRG of the 11 SPDR sectors vs SPY, with 6-week tails | derived |
| 04 | Score | Transparent composite screener (trend, momentum, relative strength, distance from high, volume) | derived |
| 05 | Discipline | Percent-risk position sizer, computed in the browser | local |
| 06 | Proof | Broker-verified ledger — **private, never published** | local strategy state |

Every score is a mechanical, reproducible formula. No black box, no proprietary rating.

## How it runs

```
GitHub Actions (cron)  ->  vm/collect.py --public  ->  docs/snapshot.json  ->  GitHub Pages
```

- `vm/collect.py` fetches ~43 symbols in parallel (~20 s, ~30 MB RSS, stdlib + `requests` only)
  and writes one flat `snapshot.json`.
- `docs/index.html` is a single dependency-free file that reads that JSON. The RRG is hand-drawn
  SVG, so the page loads instantly and works offline.
- The workflow refreshes every 30 min during US market hours and refuses to publish a snapshot
  that fails its privacy or completeness checks.

## Privacy model

The public site is built with `--public`, which strips every broker-derived figure
(real holdings, account equity, peak NLV). The CI job asserts this before each commit and
fails the build if anything personal appears.

Personal data stays on the private instance: run the collector without `--public`, write the
result next to the page as `private.json`, and the front end merges it in automatically.
That file is git-ignored and only ever exists on the machine that generated it.

## Run it yourself

```bash
pip install requests
python vm/collect.py --public --out docs/snapshot.json
python -m http.server -d docs 8080     # http://localhost:8080
```

Full instance with your own strategy state:

```bash
python vm/collect.py \
  --signal /path/to/signal_target.json \
  --equity /path/to/equity_state.json \
  --out docs/private.json
```

## Notes

- Yahoo Finance is deliberately unused — it rate-limits and returns 403 for many hosts.
  `stockanalysis.com` and `api.nasdaq.com` need no key and have been stable.
- GitHub disables scheduled workflows in a repo with no commits for 60 days; the snapshot
  commits themselves keep it alive.
- Not investment advice. Mechanical output from public data — every decision is yours.

MIT

---

Also in this repo: `app.py` + `modules/` are a Streamlit build of the same six modules,
useful locally but too memory-hungry for a 1 GB always-free VM — which is why the
static snapshot pipeline above is the deployed path.
