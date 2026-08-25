"""Load balancer + health checker: the one address every client knows.

It does NOT relay file data (that would make it the bottleneck for GB-sized
transfers). It only answers one question:

    client: {"op": "WHERE"}
    balancer: {"ok": true, "host": "127.0.0.1", "port": 9102, "node": "node2"}

and the client then talks to that node directly. This is "redirect" style
balancing, the same idea used by many real storage systems.

Choice rule: among the nodes that answered the last health ping, pick the one
with the fewest active connections (least-connections). A dead node simply
stops being offered, which is how a server failure is survived.

Run it with:
    python -m src.balancer.balancer
"""

import json
import os
import socket
import socketserver
import threading
import time

from ..common import config
from ..common.protocol import send_json, recv_json


class NodeRegistry:
    """Live view of every storage node, refreshed by a background thread."""

    def __init__(self, nodes):
        self._lock = threading.Lock()
        self._state = {}
        for index, (host, port) in enumerate(nodes):
            node_id = "node%d" % (index + 1)
            self._state[node_id] = {
                "node": node_id, "host": host, "port": port,
                "alive": False, "active": 0, "served": 0,
                "last_seen": 0.0, "failures": 0,
            }

    def snapshot(self):
        with self._lock:
            return json.loads(json.dumps(self._state))

    def _ping(self, entry):
        """One health check. Returns (alive, active, served)."""
        try:
            sock = socket.create_connection((entry["host"], entry["port"]), timeout=2.0)
        except OSError:
            return False, 0, 0
        try:
            sock.settimeout(2.0)
            send_json(sock, {"op": "PING"})
            reply = recv_json(sock.makefile("rb"))
            if reply and reply.get("ok"):
                return True, reply.get("active", 0), reply.get("served", 0)
            return False, 0, 0
        except (OSError, ValueError):
            return False, 0, 0
        finally:
            sock.close()

    def health_loop(self):
        while True:
            for node_id in list(self._state):
                with self._lock:
                    entry = dict(self._state[node_id])
                alive, active, served = self._ping(entry)
                with self._lock:
                    current = self._state[node_id]
                    was_alive = current["alive"]
                    current["alive"] = alive
                    current["active"] = active
                    current["served"] = served
                    if alive:
                        current["last_seen"] = time.time()
                        current["failures"] = 0
                        if not was_alive:
                            print("[balancer] %s is UP" % node_id)
                    else:
                        current["failures"] += 1
                        if was_alive:
                            print("[balancer] %s is DOWN" % node_id)
            self._write_status()
            time.sleep(config.HEALTH_INTERVAL)

    def pick(self):
        """Least-connections choice among the healthy nodes."""
        with self._lock:
            healthy = [e for e in self._state.values() if e["alive"]]
        if not healthy:
            return None
        healthy.sort(key=lambda e: (e["active"], e["served"]))
        return healthy[0]

    def _write_status(self):
        path = os.path.join(config.RUNTIME_DIR, "balancer.json")
        payload = {"updated": time.time(), "nodes": self.snapshot()}
        try:
            with open(path + ".tmp", "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(path + ".tmp", path)
        except OSError:
            pass


class BalancerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        registry = self.server.registry
        stream = self.request.makefile("rb")
        try:
            request = recv_json(stream)
            if request is None:
                return
            operation = str(request.get("op", "")).upper()

            if operation == "WHERE":
                chosen = registry.pick()
                if chosen is None:
                    send_json(self.request, {"ok": False, "error": "no healthy node available"})
                else:
                    send_json(self.request, {"ok": True, "node": chosen["node"],
                                             "host": chosen["host"], "port": chosen["port"]})
            elif operation == "STATUS":
                send_json(self.request, {"ok": True, "nodes": registry.snapshot()})
            else:
                send_json(self.request, {"ok": False, "error": "unknown operation"})
        except (OSError, ValueError):
            pass
        finally:
            try:
                stream.close()
            except OSError:
                pass


class BalancerServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, registry):
        socketserver.ThreadingTCPServer.__init__(self, address, BalancerHandler)
        self.registry = registry


def main():
    config.ensure_dirs()
    registry = NodeRegistry(config.NODES)
    threading.Thread(target=registry.health_loop, daemon=True).start()

    address = (config.BALANCER_HOST, config.BALANCER_PORT)
    server = BalancerServer(address, registry)
    print("balancer listening on %s:%d, watching %d nodes"
          % (address[0], address[1], len(config.NODES)))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbalancer shutting down")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
