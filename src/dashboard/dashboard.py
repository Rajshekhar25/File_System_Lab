"""Web dashboard: the "visualization of availability and status" requirement.

It is intentionally a read-only viewer. It never talks to the nodes over the
network; it just reads the JSON status files that the monitors and the balancer
already write into runtime/. If every server process dies, the dashboard still
runs and simply shows everything as stale.

    python -m src.dashboard.dashboard      ->  http://127.0.0.1:8080
"""

import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from ..common import config
from ..server.policy import PolicyStore
from ..server.storage import Storage

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE = os.path.join(HERE, "index.html")

# A node whose status file has not been touched for this long is called stale.
STALE_AFTER = 4.0


def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def collect_status():
    """Merge everything the components have written into one picture."""
    now = time.time()

    nodes = []
    if os.path.isdir(config.RUNTIME_DIR):
        for entry in sorted(os.listdir(config.RUNTIME_DIR)):
            if not entry.startswith("node_") or not entry.endswith(".json"):
                continue
            data = _read_json(os.path.join(config.RUNTIME_DIR, entry))
            if data:
                data["stale"] = (now - data.get("updated", 0)) > STALE_AFTER
                nodes.append(data)
    nodes.sort(key=lambda n: n["node"])

    balancer = _read_json(os.path.join(config.RUNTIME_DIR, "balancer.json")) or {}
    health = balancer.get("nodes", {})

    # A node counts as available only if the balancer's last ping succeeded.
    for node in nodes:
        node["reachable"] = bool(health.get(node["node"], {}).get("alive"))

    # Nodes configured but never started still deserve a row in the table.
    seen = {n["node"] for n in nodes}
    for name, entry in sorted(health.items()):
        if name not in seen:
            nodes.append({
                "node": name, "address": "%s:%d" % (entry["host"], entry["port"]),
                "reachable": entry["alive"], "stale": True, "uptime": 0,
                "active_connections": 0, "counters": {}, "transfers": [], "recent": [],
            })

    storage = Storage()
    policy = PolicyStore()
    rules = policy.all_rules()
    files = []
    for item in storage.list_files():
        rule = rules.get(item["name"], {})
        files.append({
            "name": item["name"], "size": item["size"], "state": item["state"],
            "owner": rule.get("owner", "-"),
            "readers": rule.get("readers", []),
            "writers": rule.get("writers", []),
            "public": rule.get("public", False),
        })

    totals = {}
    events = []
    for node in nodes:
        for key, value in node.get("counters", {}).items():
            totals[key] = totals.get(key, 0) + value
        for event in node.get("recent", []):
            event = dict(event)
            event["node"] = node["node"]
            events.append(event)
    events.sort(key=lambda e: e["ts"], reverse=True)

    return {
        "now": now,
        "balancer_alive": bool(balancer) and (now - balancer.get("updated", 0)) < STALE_AFTER,
        "nodes": nodes,
        "files": files,
        "totals": totals,
        "events": events[:30],
    }


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api/status"):
            body = json.dumps(collect_status()).encode("utf-8")
            self._respond(200, "application/json", body)
        elif self.path in ("/", "/index.html"):
            try:
                with open(PAGE, "rb") as fh:
                    self._respond(200, "text/html; charset=utf-8", fh.read())
            except OSError:
                self._respond(500, "text/plain", b"index.html is missing")
        else:
            self._respond(404, "text/plain", b"not found")

    def _respond(self, code, content_type, body):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # keep the console clean; the nodes are the interesting log


def main():
    config.ensure_dirs()
    server = HTTPServer(("127.0.0.1", config.DASHBOARD_PORT), DashboardHandler)
    print("dashboard: http://127.0.0.1:%d" % config.DASHBOARD_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\ndashboard shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
