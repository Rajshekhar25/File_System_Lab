"""A tiny publish/subscribe event bus.

This is the heart of the "event-driven architecture" requirement. Nothing in
the server calls the monitor or the logger directly. A component only shouts
"this happened" (publish) and whoever cares has registered to listen
(subscribe). Adding a new listener never means changing the code that emits.
"""

import threading
import time
from collections import defaultdict

WILDCARD = "*"  # subscribe to this to receive every event


class EventBus:
    def __init__(self):
        self._subscribers = defaultdict(list)
        self._lock = threading.Lock()

    def subscribe(self, event_type, handler):
        """Register handler(event_dict) for one event type, or '*' for all."""
        with self._lock:
            self._subscribers[event_type].append(handler)

    def publish(self, event_type, **data):
        """Announce that something happened. Returns the event that was sent."""
        event = {"type": event_type, "ts": time.time()}
        event.update(data)

        with self._lock:
            handlers = list(self._subscribers[event_type])
            handlers += list(self._subscribers[WILDCARD])

        # One broken listener must never take the server down.
        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:  # pragma: no cover - defensive only
                print("[event-bus] listener failed:", exc)
        return event


# Every component inside one process shares this single bus.
bus = EventBus()


# --- Event names used across the project (kept in one place for reference) ---
CLIENT_CONNECTED = "client.connected"
CLIENT_DISCONNECTED = "client.disconnected"
ACCESS_DENIED = "access.denied"

UPLOAD_STARTED = "upload.started"
UPLOAD_PROGRESS = "upload.progress"
UPLOAD_COMPLETED = "upload.completed"
UPLOAD_ABORTED = "upload.aborted"

DOWNLOAD_STARTED = "download.started"
DOWNLOAD_PROGRESS = "download.progress"
DOWNLOAD_COMPLETED = "download.completed"
DOWNLOAD_ABORTED = "download.aborted"

POLICY_CHANGED = "policy.changed"
NODE_STARTED = "node.started"
NODE_UP = "node.up"
NODE_DOWN = "node.down"
