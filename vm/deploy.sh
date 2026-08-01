#!/usr/bin/env bash
# Sync the private "Peter Review" instance to the Oracle VM and restart it.
#
#   bash vm/deploy.sh                 # push code + refresh snapshot + reload
#   HOST=1.2.3.4 bash vm/deploy.sh    # different box
#
# First-time setup (credentials + TLS) is done by vm/bootstrap_vm.sh.
set -euo pipefail

HOST=${HOST:-152.70.194.225}
USER_=${USER_:-ubuntu}
KEY=${KEY:-$HOME/.ssh/id_ed25519}
BASE=/opt/peter-review
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SSHOPT=(-i "$KEY" -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new)

echo "==> shipping code to $USER_@$HOST:$BASE"
scp "${SSHOPT[@]}" -q "$ROOT/vm/collect.py" "$ROOT/vm/refresh.sh" "$USER_@$HOST:$BASE/"
scp "${SSHOPT[@]}" -q "$ROOT/docs/index.html" "$USER_@$HOST:$BASE/web/"

echo "==> refreshing private snapshot + reloading Caddy"
ssh "${SSHOPT[@]}" "$USER_@$HOST" 'bash -s' <<'REMOTE'
set -e
chmod +x /opt/peter-review/refresh.sh
sudo /opt/peter-review/refresh.sh
docker exec peter-review caddy reload --config /etc/caddy/Caddyfile 2>/dev/null || \
  docker restart peter-review >/dev/null
echo "--- status ---"
docker ps --filter name=peter-review --format "{{.Names}} {{.Status}}"
sudo tail -n 3 /opt/peter-review/refresh.log
REMOTE

echo "==> done: https://152-70-194-225.sslip.io:8443/"
