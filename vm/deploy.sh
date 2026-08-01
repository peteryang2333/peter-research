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
scp "${SSHOPT[@]}" -q "$ROOT/vm/collect.py" "$ROOT/vm/refresh.sh" \
    "$ROOT/vm/refreshd.py" "$ROOT/vm/peter-refreshd.service" "$USER_@$HOST:$BASE/"
scp "${SSHOPT[@]}" -q "$ROOT/docs/index.html" "$USER_@$HOST:$BASE/web/"

echo "==> refreshing private snapshot + reloading Caddy"
ssh "${SSHOPT[@]}" "$USER_@$HOST" 'bash -s' <<'REMOTE'
set -e
chmod +x /opt/peter-review/refresh.sh

# --- on-demand refresh trigger (systemd) -----------------------------------
sudo install -m 644 /opt/peter-review/peter-refreshd.service \
     /etc/systemd/system/peter-refreshd.service
sudo systemctl daemon-reload
sudo systemctl enable --now peter-refreshd >/dev/null 2>&1 || true
sudo systemctl restart peter-refreshd

# --- Caddy: route /api/* to the trigger (idempotent, preserves auth hash) ---
sudo /usr/bin/python3 - <<'PATCH'
p = "/opt/peter-review/Caddyfile"
s = orig = open(p).read()
UPSTREAM = "reverse_proxy unix//data/refreshd.sock"

if "/api/*" not in s:
    old = "\troot * /srv\n\tencode gzip\n\tfile_server\n"
    new = ("\t@api path /api/*\n"
           "\thandle @api {\n"
           f"\t\t{UPSTREAM}\n"
           "\t}\n\n"
           "\thandle {\n"
           "\t\troot * /srv\n"
           "\t\tencode gzip\n"
           "\t\tfile_server\n"
           "\t}\n")
    if old not in s:
        raise SystemExit("caddyfile: unexpected layout, refusing to patch")
    s = s.replace(old, new, 1)
    print("caddyfile: /api route added")

# Upgrade an older TCP-gateway upstream (blocked by the host firewall) to the
# unix socket.
if "reverse_proxy 172.17.0.1:8790" in s:
    s = s.replace("reverse_proxy 172.17.0.1:8790", UPSTREAM)
    print("caddyfile: upstream switched to unix socket")

if s != orig:
    open(p, "w").write(s)
else:
    print("caddyfile: already up to date")
PATCH

sudo /opt/peter-review/refresh.sh
docker exec peter-review caddy reload --config /etc/caddy/Caddyfile 2>/dev/null || \
  docker restart peter-review >/dev/null
echo "--- status ---"
docker ps --filter name=peter-review --format "{{.Names}} {{.Status}}"
systemctl is-active peter-refreshd | sed 's/^/peter-refreshd: /'
sudo tail -n 3 /opt/peter-review/refresh.log
REMOTE

echo "==> done: https://152-70-194-225.sslip.io:8443/"
