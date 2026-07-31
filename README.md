# KovaView-OSS

A free, self-hostable trading command center inspired by KovaView's six modules.
No Yahoo Finance (so no rate-limit), no paywall.

## Modules
1. **Macro** — global inflation/GDP heatmap (World Bank Open Data)
2. **Direction** — one posture score from trend / breadth / credit / vol / leadership
3. **Rotation** — RRG sector-flow map (JdK RS-Ratio vs RS-Momentum, sectors vs SPY)
4. **Kova Score** — transparent composite rank over your watchlist (momentum+trend+relVol)
5. **Discipline** — percent-risk position sizer + SQLite trade journal (R multiples, PF, win rate)
6. **Proof** — broker-verified ledger via Interactive Brokers + honest leaderboard

## Data sources
- Market data: `stockanalysis.com` + `api.nasdaq.com` (primary, no key)
- Macro: `api.worldbank.org` (no key)
- Broker verification: IBKR Client Portal Web API (optional)

## Run locally
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py --server.port 8501
# open http://localhost:8501
```

## Deploy to Oracle Cloud (Always Free)
```bash
git clone <repo> kovaview-oss && cd kovaview-oss
sudo bash deploy_oracle.sh
# open http://<vm-public-ip>:8501
```
⚠️ On Oracle the #1 reason a port won't open is the **VCN Security List**, not the
firewall. Add an ingress rule: TCP 8501, source 0.0.0.0/0.

## Enable IBKR (broker verification)
1. Run IBKR Client Portal Gateway (paper or live) on the VM/host, authenticate (SSO/conf code).
2. Set `IBKR_BASE` to its URL (e.g. `http://127.0.0.1:5000` or `http://host.docker.internal:5000`).
3. Restart the container. Modules 5/6 will reconcile live; otherwise they show demo data.

## Notes / honesty
- The Kova Score here is a transparent heuristic, **not** KovaView's proprietary algorithm.
- "Proof" leaderboard is demo-only unless you wire your own IBKR account (multi-trader
  verification would need a backend + auth — out of scope for a personal dashboard).
- Macro shows sample data if World Bank is unreachable from the runtime.
