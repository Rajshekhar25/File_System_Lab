"""The listener that turns raw events into the numbers you see on screen.

The monitor subscribes to '*' on the event bus, so it sees every single thing
that happens inside a node without any handler knowing it exists. It keeps the
picture in memory and dumps it to runtime/node_<id>.json once a second; the
dashboard just reads those files.
"""

import json
import os
import threading
import time
from collections import deque

from ..common import config
from ..common.events import bus, WILDCARD

MAX_RECENT_EVENTS = 40


class Monitor:
    def __init__(self, node_id, storage, listen_addr):
        self.node_id = node_id
        self.storage = storage
        self.listen_addr = listen_addr
        self.started_at = time.time()

        self._lock = threading.Lock()
        self._recent = deque(maxlen=MAX_RECENT_EVENTS)
        self._transfers = {}          # key -> live transfer description
        self._counters = {
            "uploads_started": 0, "uploads_completed": 0, "uploads_aborted": 0,
            "downloads_started": 0, "downloads_completed": 0, "downloads_aborted": 0,
            "access_denied": 0, "connections": 0,
            "bytes_in": 0, "bytes_out": 0,
        }

        bus.subscribe(WILDCARD, self.on_event)
        self._status_path = os.path.join(config.RUNTIME_DIR, "node_%s.json" % node_id)
        os.makedirs(config.RUNTIME_DIR, exist_ok=True)

        writer = threading.Thread(target=self._write_loop, daemon=True)
        writer.start()

    # --------------------------------------------------------- event listener
    def on_event(self, event):
        kind = event["type"]
        key = "%s:%s:%s" % (event.get("user"), event.get("file"), kind.split(".")[0])

        with self._lock:
            self._recent.append({
                "ts": event["ts"], "type": kind, "user": event.get("user"),
                "file": event.get("file"),
            })

            if kind == "client.connected":
                self._counters["connections"] += 1
            elif kind == "access.denied":
                self._counters["access_denied"] += 1

            elif kind == "upload.started":
                self._counters["uploads_started"] += 1
                self._transfers[key] = {"kind": "upload", "user": event.get("user"),
                                        "file": event.get("file"),
                                        "done": event.get("offset", 0),
                                        "total": event.get("total", 0)}
            elif kind == "upload.progress" and key in self._transfers:
                self._transfers[key]["done"] = event.get("done", 0)
            elif kind == "upload.completed":
                self._counters["uploads_completed"] += 1
                self._counters["bytes_in"] += event.get("size", 0)
                self._transfers.pop(key, None)
            elif kind == "upload.aborted":
                self._counters["uploads_aborted"] += 1
                self._transfers.pop(key, None)

            elif kind == "download.started":
                self._counters["downloads_started"] += 1
                self._transfers[key] = {"kind": "download", "user": event.get("user"),
                                        "file": event.get("file"),
                                        "done": event.get("offset", 0),
                                        "total": event.get("total", 0)}
            elif kind == "download.progress" and key in self._transfers:
                self._transfers[key]["done"] = event.get("done", 0)
            elif kind == "download.completed":
                self._counters["downloads_completed"] += 1
                self._counters["bytes_out"] += event.get("size", 0)
                self._transfers.pop(key, None)
            elif kind == "download.aborted":
                self._counters["downloads_aborted"] += 1
                self._transfers.pop(key, None)

    # ------------------------------------------------------------- publishing
    def snapshot(self, active=0):
        with self._lock:
            return {
                "node": self.node_id,
                "address": "%s:%d" % self.listen_addr,
                "alive": True,
                "uptime": round(time.time() - self.started_at, 1),
                "updated": time.time(),
                "active_connections": active,
                "counters": dict(self._counters),
                "transfers": list(self._transfers.values()),
                "recent": list(self._recent)[-MAX_RECENT_EVENTS:],
            }

    def attach_active_source(self, callable_):
        """Let the node tell us how many client threads are busy right now."""
        self._active_source = callable_

    def _write_loop(self):
        while True:
            try:
                active = getattr(self, "_active_source", lambda: 0)()
                tmp = self._status_path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(self.snapshot(active), fh)
                os.replace(tmp, self._status_path)  # atomic: readers never see half a file
            except OSError:
                pass
            time.sleep(1.0)

    def remove_status_file(self):
        """Called on a clean shutdown so the dashboard stops showing this node."""
        try:
            os.remove(self._status_path)
        except OSError:
            pass
