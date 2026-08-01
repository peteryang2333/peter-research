# Peter Review

A self-hosted, six-module market dashboard. Static front end, no build step, no chart
library, **no Yahoo / yfinance**, no API keys.

**Public demo:** https://peteryang2333.github.io/peter-review/

It runs in two flavours from the same code:

| | Public instance | Private instance |
|---|---|---|
| Where | GitHub Pages | Oracle VM, behind HTTPS + basic auth |
| Refreshed by | GitHub Actions cron | root cron on the VM |
| Modules 01–04 | live market data | live market data |
| Modules 05–06 | redacted placeholders | real holdings, NLV, equity curve |

## Modules

| # | Module | What it shows | Source |
|---|--------|---------------|--------|
| 01 | Macro | Global CPI heat tiers, hottest / coolest economies | World Bank |
| 02 | Direction | Posture score (Trend / Breadth / Credit / Vol / Leadership) + index, rate, credit and crypto tape | stockanalysis + nasdaq |
| 03 | Rotation | RRG of the 11 SPDR sectors vs SPY, with 6-week tails | derived |
| 04 | Score | Transparent composite screener (trend, momentum, relative strength, distance from high, volume) | derived |
| 05 | Discipline | Percent-risk sizer, risk budget scaled by the live posture | local state |
| 06 | Proof | Current holding, NLV, drawdown, equity curve, rotation log — **private only** | local strategy state |

Every score is a mechanical, reproducible formula. No black box, no proprietary rating.

## How it runs

```
public :  Actions cron -> collect.py --public -> docs/snapshot.json -> Pages
private:  root cron    -> collect.py           -> /opt/peter-review/web/snapshot.json -> Caddy
```

- `vm/collect.py` fetches ~43 symbols in parallel (~20 s, ~30 MB RSS, stdlib + `requests`
  only) and writes one flat `snapshot.json`.
- `docs/index.html` is a single dependency-free file that reads that JSON. The RRG and the
  equity curve are hand-drawn SVG, so the page loads instantly.
- The public workflow refuses to publish a snapshot that fails its privacy or completeness
  checks.

## Privacy model

The public build runs with `--public`, which strips every broker-derived figure (real
holdings, account equity, peak NLV, NLV curve). CI asserts this before each commit — it
scans for the sensitive keys *and* for any number above 500 000 — and fails closed.

The private instance runs the same collector without that flag, reading the live strategy
state, and is only reachable over TLS behind basic auth.

## Deploy

Public: push to `main`; Actions and Pages do the rest.

Private (Oracle VM):

```bash
bash vm/deploy.sh          # ships collect.py + index.html, refreshes, reloads Caddy
```

First-time setup on the VM:

- `vm/oci_open_port.py --ip <public-ip> --port 8443` opens the OCI security-list ingress
  (also `--port 80`, needed once for the Let's Encrypt HTTP-01 challenge).
- `vm/Caddyfile` serves `/srv` on 8443 with a real Let's Encrypt certificate via
  `sslip.io`, since port 443 is already taken on that box. Replace `PLACEHOLDER_USER` /
  `PLACEHOLDER_HASH` with the output of `caddy hash-password`.
- `vm/refresh.sh` regenerates the private snapshot from the strategy state and is driven
  by root cron.

## Run it locally

```bash
pip install requests
python vm/collect.py --public --out docs/snapshot.json
python -m http.server -d docs 8080     # http://localhost:8080
```

## Notes

- Yahoo Finance is deliberately unused — it rate-limits and returns 403 for many hosts.
  `stockanalysis.com` and `api.nasdaq.com` need no key and have been stable, including
  from GitHub-hosted runners.
- Caddy holds ~13 MB RSS, which is why this replaced the Streamlit build on a 1 GB
  always-free box. `app.py` + `modules/` still contain that Streamlit variant.
- GitHub disables scheduled workflows in a repo with no commits for 60 days; the snapshot
  commits themselves keep it alive.
- Not investment advice. Mechanical output from public data — every decision is yours.

MIT
