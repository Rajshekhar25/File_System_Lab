"""The wire format spoken between client, balancer and storage nodes.

Deliberately minimal so it can be explained in one line:

    one JSON header terminated by '\n', optionally followed by raw file bytes.

Example upload request:
    {"op": "UPLOAD", "user": "alice", "file": "movie.iso", "offset": 0,
     "total": 2147483648}\n
    <2147483648 raw bytes>

Keeping the payload raw (instead of base64 or JSON) is what allows multi-GB
files to stream at disk speed.
"""

import json


def send_json(sock, obj):
    """Send one JSON message as a single newline-terminated line."""
    sock.sendall((json.dumps(obj) + "\n").encode("utf-8"))


def recv_json(stream):
    """Read one JSON line from a buffered binary file object.

    Returns None if the peer closed the connection (an abrupt client exit
    looks exactly like this, and callers treat it as 'aborted').
    """
    line = stream.readline()
    if not line:
        return None
    return json.loads(line.decode("utf-8"))


def recv_exactly(stream, n):
    """Read exactly n bytes, or fewer if the peer disappeared mid-transfer."""
    data = stream.read(n)
    return data if data else b""


def human_size(num_bytes):
    """Format a byte count for logs and the dashboard."""
    step = 1024.0
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < step:
            return "%.1f %s" % (num_bytes, unit)
        num_bytes /= step
    return "%.1f PB" % num_bytes
