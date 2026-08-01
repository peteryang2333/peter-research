# Peter Research

A self-hosted, multi-source signal dashboard. Static front end, no build step, no chart
library, **no Yahoo / yfinance**, no API keys. Several independent signal layers are fused
into one watch tool: your thesis is only one of five inputs, so no single opinion dominates.

**Public demo:** https://peteryang2333.github.io/peter-research/

It runs in two flavours from the same code:

| | Public instance | Private instance |
|---|---|---|
| Where | GitHub Pages | Oracle VM, behind HTTPS + basic auth |
| Refreshed by | GitHub Actions cron | root cron on the VM |
| Modules 01–07 | live market data | live market data |
| Modules 08–09 | redacted placeholders | real holdings, NLV, equity curve |

## Modules

| Tab | View | What it shows | Source |
|-----|------|---------------|--------|
| 01 宏观 | Macro | Global CPI / GDP heat tiers, hottest / coolest economies | World Bank |
| 02 方向 | Direction | Posture score (Trend / Breadth / Credit / Vol / Leadership) + index, rate, credit and crypto tape | stockanalysis + nasdaq |
| 03 轮动 | Rotation | RRG of the 11 SPDR sectors **and** 26 theme/style ETFs (semis, software, biotech, banks, treasuries, gold…) vs SPY | derived |
| 04 最活跃 | Movers | Most-active US stocks by dollar volume + per-stock accumulation / distribution (net-flow index) | derived from OHLCV |
| 05 资金·机构 | Flow & Institutional | Analyst consensus (buy/hold/sell, target upside) + money-flow accumulation list | nasdaq consensus |
| 06 复合排名 | Composite Ranker | **5-factor fusion** = 0.35·technical + 0.10·activity + 0.20·flow + 0.20·institutional + 0.15·thesis, with a per-stock "why highlighted" list | all of the above |
| 07 VCP融合 | VCP Fusion | Daily volatility-contraction scan folded into the same 5-factor system: **买入前10榜 + 额外买入前10榜** with system score, 综合 verdict, breakout trigger & stops | local VCP scan (`--vcp`) |
| 08 打分 | Score | Transparent screener on your thematic watchlist (trend, momentum, relative strength, distance from high, volume) | derived |
| 09 纪律 | Discipline | Percent-risk position sizer; risk budget scaled by the live posture (private: real holdings/NLV) | local state |
| 10 验证 | Proof | Current holding, NLV, drawdown, equity curve, rotation log — **private only** | local strategy state |

**Why five signals, not just your thesis:** a single-thesis ranker is fragile. The composite
ranker blends the technical screener, trading activity, money flow, institutional consensus and
your own thesis — each weighted, none with veto — so a gap in any one view is covered by the
others. The "why highlighted" column shows exactly which signals fired for each stock.

Every score is a mechanical, reproducible formula. No black box, no proprietary rating.

## How it runs

```
public :  Actions cron -> collect.py --public -> docs/snapshot.json -> Pages
private:  root cron    -> collect.py           -> /opt/peter-research/web/snapshot.json -> Caddy
```

- `vm/collect.py` fetches ~190 symbols (155 liquid cross-sector stocks + 26 ETFs + benchmarks
  and sectors) in parallel (~4 min, stdlib + `requests` only) and writes one flat `snapshot.json`.
  Edit `vm/liquid_universe.json` to change the liquid pool.
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
