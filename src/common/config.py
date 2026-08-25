"""Single place for every setting used by the platform.

Change a port or a folder here and every component picks it up, so a demo
never needs edits in more than one file.
"""

import os

# Project root = two levels above this file (src/common/config.py)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# The shared "local cloud" disk. Every storage node reads/writes here, which is
# what makes a node replaceable when it dies.
STORAGE_DIR = os.path.join(BASE_DIR, "storage")

# Live status files written by the nodes and the balancer, read by the dashboard.
RUNTIME_DIR = os.path.join(BASE_DIR, "runtime")

# Access-control rules (who owns what, who may read/write what).
POLICY_FILE = os.path.join(STORAGE_DIR, "policies.json")

# Files are moved 1 MB at a time so a 10 GB file never sits in RAM.
CHUNK_SIZE = 1024 * 1024

# Load balancer: the single address a client needs to know.
BALANCER_HOST = "127.0.0.1"
BALANCER_PORT = 9000

# The pool of multithreaded storage nodes.
NODES = [
    ("127.0.0.1", 9101),
    ("127.0.0.1", 9102),
    ("127.0.0.1", 9103),
]

# How often the balancer pings each node to see if it is still alive.
HEALTH_INTERVAL = 2.0

# Web dashboard.
DASHBOARD_PORT = 8080

# Socket timeout for control messages (seconds).
SOCKET_TIMEOUT = 10.0


def ensure_dirs():
    """Create the storage and runtime folders if they do not exist yet."""
    os.makedirs(STORAGE_DIR, exist_ok=True)
    os.makedirs(RUNTIME_DIR, exist_ok=True)
