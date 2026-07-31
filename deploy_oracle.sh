#!/usr/bin/env bash
# deploy_oracle.sh — run this ON your Oracle Cloud VM (Always Free tier).
# It installs Docker if needed, opens port 8501 (Oracle Security List + ufw),
# and brings KovaView-OSS up with docker compose.
#
# Usage:
#   git clone <your-repo> kovaview-oss && cd kovaview-oss
#   # optional: export IBKR_BASE=http://127.0.0.1:5000
#   sudo bash deploy_oracle.sh
set -euo pipefail

PORT=8501
echo "==> Installing Docker (if missing) =="
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker "$(whoami)" || true
fi
if ! command -v docker-compose >/dev/null 2>&1; then
  sudo apt-get update -y && sudo apt-get install -y docker-compose-plugin
fi

echo "==> Opening port $PORT =="
# Oracle uses Security Lists, not just the OS firewall. If the OCI CLI is
# configured you can open the ingress rule; otherwise do it in the OCI console
# (Networking > VCN > Security Lists > Ingress: TCP $PORT, source 0.0.0.0/0).
if command -v oci >/dev/null 2>&1; then
  echo "OCI CLI found — you can add an ingress rule via 'oci network security-list ...'."
fi
sudo ufw allow "$PORT"/tcp 2>/dev/null || true

echo "==> Starting KovaView-OSS =="
docker compose up -d --build

echo
echo "Done. Open:  http://<your-vm-public-ip>:$PORT"
echo "If it doesn't load, the #1 cause is the OCI Security List — open TCP $PORT there."
echo "IBKR: set IBKR_BASE and authenticate the Client Portal Gateway for live verification."
