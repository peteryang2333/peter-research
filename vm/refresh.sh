#!/usr/bin/env bash
# Peter Research - regenerate the private snapshot from live market data plus the
# real daily-signal-bridge state. Runs as root from cron so it can read the
# bridge's docker volume.
set -euo pipefail

BASE=/opt/peter-research
STATE=/var/lib/docker/volumes/vps_bridge_data/_data
LOG=$BASE/refresh.log

# Cron and the dashboard's "re-collect now" button can both land here. Only one
# collector at a time - this box has 1 GB of RAM. Wait up to 4 min, then bail.
exec 9>"$BASE/.refresh.lock"
flock -w 240 9 || { echo "$(date -Is) another refresh holds the lock, giving up" >>"$LOG"; exit 0; }

# Keep the log bounded - this box only has 1 GB and a shared disk.
if [ -f "$LOG" ]; then tail -n 400 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"; fi
exec >>"$LOG" 2>&1

echo "=== $(date -Is) refresh start ==="

# Prefer the persisted volume copy; fall back to the container's working copy
# if the bridge has not flushed yet.
SIGNAL="$STATE/last_good_signal.json"
[ -s "$SIGNAL" ] || SIGNAL="/opt/daily-signal-bridge/signal_target.json"

/usr/bin/python3 "$BASE/collect.py" \
  --out    "$BASE/web/snapshot.json" \
  --signal "$SIGNAL" \
  --equity "$STATE/equity_state.json" \
  --nlv    "$STATE/daily_nlv.json" \
  --trades "$BASE/trades.json"

chmod 644 "$BASE/web/snapshot.json"
echo "=== $(date -Is) refresh done ==="
