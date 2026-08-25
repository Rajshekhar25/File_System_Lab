"""A storage node: the multithreaded server that actually moves the bytes.

Threading model
---------------
One acceptor thread waits for connections. Every accepted connection is given
its own worker thread (socketserver.ThreadingTCPServer), so a 5 GB upload from
one user never blocks a small LIST from another.

Run three of them for the demo:
    python -m src.server.node 1
    python -m src.server.node 2
    python -m src.server.node 3
"""

import socketserver
import sys
import threading
import time

from ..common import config
from ..common.events import (bus, WILDCARD, CLIENT_CONNECTED, CLIENT_DISCONNECTED,
                             NODE_STARTED)
from ..common.protocol import recv_json, send_json
from .handlers import OPS
from .monitor import Monitor
from .policy import PolicyStore
from .storage import Storage

# Events that are too chatty to print on the console.
QUIET_EVENTS = ("upload.progress", "download.progress")


class NodeContext:
    """Everything a handler needs, plus the live connection counters."""

    def __init__(self, node_id, storage, policy):
        self.node_id = node_id
        self.storage = storage
        self.policy = policy
        self._active = 0
        self._served = 0
        self._lock = threading.Lock()

    def begin_request(self):
        with self._lock:
            self._active += 1
            self._served += 1

    def end_request(self):
        with self._lock:
            self._active -= 1

    def active_count(self):
        with self._lock:
            return self._active

    def served_count(self):
        with self._lock:
            return self._served


class ClientHandler(socketserver.BaseRequestHandler):
    """Runs in its own thread. Reads one request and dispatches it."""

    def handle(self):
        ctx = self.server.ctx
        peer = "%s:%d" % self.client_address
        counted = False

        stream = self.request.makefile("rb")
        try:
            request = recv_json(stream)
            if request is None:
                return
            operation = str(request.get("op", "")).upper()

            # Health pings are bookkeeping, not user load: they must not show
            # up in the number the balancer uses to compare nodes.
            if operation != "PING":
                ctx.begin_request()
                counted = True
                bus.publish(CLIENT_CONNECTED, peer=peer, node=ctx.node_id)

            handler = OPS.get(operation)
            if handler is None:
                send_json(self.request, {"ok": False,
                                         "error": "unknown operation: %s" % operation})
            else:
                handler(ctx, request, self.request, stream)
        except (ConnectionError, OSError, ValueError):
            # Abrupt client death. Handlers already published the abort event.
            pass
        finally:
            try:
                stream.close()
            except OSError:
                pass
            if counted:
                ctx.end_request()
                bus.publish(CLIENT_DISCONNECTED, peer=peer, node=ctx.node_id)


class StorageNode(socketserver.ThreadingTCPServer):
    daemon_threads = True      # worker threads never block shutdown
    allow_reuse_address = True

    def __init__(self, address, ctx):
        socketserver.ThreadingTCPServer.__init__(self, address, ClientHandler)
        self.ctx = ctx


def console_logger(event):
    """A second bus listener, kept separate from the monitor on purpose."""
    if event["type"] in QUIET_EVENTS:
        return
    stamp = time.strftime("%H:%M:%S", time.localtime(event["ts"]))
    details = " ".join("%s=%s" % (k, v) for k, v in event.items()
                       if k not in ("type", "ts") and v is not None)
    print("[%s] %-20s %s" % (stamp, event["type"], details))


def main():
    if len(sys.argv) < 2:
        print("usage: python -m src.server.node <node-number 1..%d>" % len(config.NODES))
        return
    index = int(sys.argv[1]) - 1
    if not 0 <= index < len(config.NODES):
        print("no such node number")
        return

    config.ensure_dirs()
    address = config.NODES[index]
    node_id = "node%d" % (index + 1)

    storage = Storage()
    policy = PolicyStore()
    ctx = NodeContext(node_id, storage, policy)

    monitor = Monitor(node_id, storage, address)
    monitor.attach_active_source(ctx.active_count)
    bus.subscribe(WILDCARD, console_logger)

    server = StorageNode(address, ctx)
    bus.publish(NODE_STARTED, node=node_id, address="%s:%d" % address)
    print("%s listening on %s:%d  (storage: %s)" % (node_id, address[0], address[1],
                                                    storage.root))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n%s shutting down" % node_id)
    finally:
        server.shutdown()
        server.server_close()
        monitor.remove_status_file()


if __name__ == "__main__":
    main()
