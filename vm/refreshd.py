#!/usr/bin/env python3
"""Peter Research - tiny "re-collect now" trigger for the private instance.

The dashboard is a static snapshot; the browser's refresh button could only
re-fetch the same JSON. This daemon lets the button actually re-run the
collector, so a click means "go get fresh market data", not "reload the file".

Security model
--------------
It listens on a UNIX SOCKET inside the Caddy container's own volume, so there
is no TCP port on the host at all - nothing on the network can reach it, only
the Caddy container, which already enforces basic auth on every path. It also
takes no user input: the POST body is ignored entirely, there is nothing to
inject. (A TCP fallback exists for local testing via REFRESHD_PORT.)

Endpoints (Caddy reverse-proxies /api/* here):
  POST /api/refresh -> start a run (rate limited); 202 accepted / 429 too soon
  GET  /api/refresh -> current state, for the front-end to poll
"""
import json
import os
import socket
import socketserver
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Caddy sees this same file as /data/refreshd.sock (peter_review_data volume).
SOCK = os.environ.get(
    "REFRESHD_SOCK",
    "/var/lib/docker/volumes/peter_review_data/_data/refreshd.sock")
BIND = os.environ.get("REFRESHD_BIND", "127.0.0.1")   # only used when PORT set
PORT = int(os.environ.get("REFRESHD_PORT", "0"))      # 0 = unix socket mode
SCRIPT = os.environ.get("REFRESHD_SCRIPT", "/opt/peter-research/refresh.sh")
MIN_INTERVAL = int(os.environ.get("REFRESHD_MIN_INTERVAL", "45"))
RUN_TIMEOUT = int(os.environ.get("REFRESHD_TIMEOUT", "300"))

_lock = threading.Lock()
STATE = {
    "state": "idle",          # idle | running
    "started_at": None,
    "finished_at": None,
    "duration": None,
    "rc": None,
    "error": None,
}


def _run():
    t0 = time.time()
    rc, err = -1, None
    try:
        p = subprocess.run(["/bin/bash", SCRIPT],
                           capture_output=True, timeout=RUN_TIMEOUT)
        rc = p.returncode
        if rc != 0:
            err = (p.stderr.decode("utf-8", "replace").strip()[-400:]
                   or f"refresh.sh exited {rc}")
    except subprocess.TimeoutExpired:
        err = f"refresh timed out after {RUN_TIMEOUT}s"
    except Exception as e:                                # noqa: BLE001
        err = f"{type(e).__name__}: {e}"[:400]
    with _lock:
        STATE.update(state="idle", finished_at=time.time(),
                     duration=round(time.time() - t0, 1), rc=rc, error=err)


def trigger():
    """Returns (http_status, payload)."""
    with _lock:
        if STATE["state"] == "running":
            return 202, dict(STATE, message="已经在采集中，请稍候")
        since = time.time() - (STATE["finished_at"] or 0)
        if since < MIN_INTERVAL:
            wait = int(MIN_INTERVAL - since) + 1
            return 429, dict(STATE, retry_after=wait,
                             message=f"刚刚才采集过，{wait} 秒后再试")
        STATE.update(state="running", started_at=time.time(),
                     finished_at=None, duration=None, rc=None, error=None)
        payload = dict(STATE)
    threading.Thread(target=_run, daemon=True).start()
    return 202, dict(payload, message="开始重新采集")


class Handler(BaseHTTPRequestHandler):
    server_version = "peter-refreshd"
    sys_version = ""

    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):                                     # noqa: N802
        if self.path.split("?")[0] != "/api/refresh":
            return self._send(404, {"error": "not found"})
        with _lock:
            payload = dict(STATE)
        payload["min_interval"] = MIN_INTERVAL
        self._send(200, payload)

    def do_POST(self):                                    # noqa: N802
        if self.path.split("?")[0] != "/api/refresh":
            return self._send(404, {"error": "not found"})
        # Body is deliberately ignored - drain it so the client isn't blocked.
        try:
            n = int(self.headers.get("Content-Length") or 0)
            if 0 < n < 65536:
                self.rfile.read(n)
        except (TypeError, ValueError):
            pass
        status, payload = trigger()
        self._send(status, payload)

    def log_message(self, fmt, *args):                    # quieter journal
        pass


class UnixHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer over AF_UNIX (http.server assumes AF_INET)."""

    address_family = socket.AF_UNIX
    allow_reuse_address = False

    def server_bind(self):
        path = self.server_address
        if os.path.exists(path):
            os.unlink(path)
        socketserver.TCPServer.server_bind(self)
        os.chmod(path, 0o660)
        self.server_name = "peter-refreshd"
        self.server_port = 0

    def get_request(self):
        # AF_UNIX accept() gives an empty peer address; BaseHTTPRequestHandler
        # wants an indexable one.
        conn, _ = self.socket.accept()
        return conn, ("local", 0)

    def server_close(self):
        super().server_close()
        try:
            os.unlink(SOCK)
        except OSError:
            pass


def main():
    if PORT:
        srv = ThreadingHTTPServer((BIND, PORT), Handler)
        where = f"{BIND}:{PORT}"
    else:
        os.makedirs(os.path.dirname(SOCK), exist_ok=True)
        srv = UnixHTTPServer(SOCK, Handler)
        where = f"unix:{SOCK}"
    srv.daemon_threads = True
    print(f"peter-refreshd listening on {where} -> {SCRIPT}", flush=True)
    try:
        srv.serve_forever()
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
