"""One handler function per operation, plus the table that maps them.

This is where the event-driven style shows up in day to day code: a handler
never prints, never updates a counter and never writes a log file. It only
publishes events ("upload.started", "upload.aborted", ...) and the monitor,
which subscribed to the bus, does the rest.

Adding a new operation = write a function and add one line to OPS.
"""

import socket

from ..common import config
from ..common.events import (bus, ACCESS_DENIED, UPLOAD_STARTED, UPLOAD_PROGRESS,
                             UPLOAD_COMPLETED, UPLOAD_ABORTED, DOWNLOAD_STARTED,
                             DOWNLOAD_PROGRESS, DOWNLOAD_COMPLETED, DOWNLOAD_ABORTED)
from ..common.protocol import send_json

# Publish a progress event at most this often (every 8 MB) so a 10 GB upload
# does not flood the bus with thousands of tiny updates.
PROGRESS_EVERY = 8 * config.CHUNK_SIZE


def _deny(ctx, sock, user, filename, operation, reason):
    bus.publish(ACCESS_DENIED, user=user, file=filename,
                operation=operation, reason=reason, node=ctx.node_id)
    send_json(sock, {"ok": False, "error": reason})


# --------------------------------------------------------------------- upload
def handle_upload(ctx, req, sock, stream):
    user = req.get("user", "anonymous")
    name = ctx.storage.safe_name(req.get("file", ""))
    total = int(req.get("total", 0))
    offset = int(req.get("offset", 0))

    ctx.policy.reload()
    if not ctx.policy.can_write(user, name):
        return _deny(ctx, sock, user, name, "upload",
                     "no write permission on %s" % name)

    # The client must resume exactly where the .part file ends.
    on_disk = ctx.storage.uploaded_bytes(name)
    if offset > on_disk:
        send_json(sock, {"ok": False, "error": "bad offset", "resume_at": on_disk})
        return

    send_json(sock, {"ok": True, "offset": offset, "node": ctx.node_id})
    bus.publish(UPLOAD_STARTED, user=user, file=name, offset=offset,
                total=total, node=ctx.node_id)

    written = offset
    remaining = total - offset
    next_report = written + PROGRESS_EVERY

    with ctx.storage.file_lock(name):
        handle = ctx.storage.open_for_append(name, offset)
        try:
            while remaining > 0:
                chunk = stream.read(min(config.CHUNK_SIZE, remaining))
                if not chunk:
                    # Client vanished (Ctrl+C, network drop, killed process).
                    # The .part file stays on disk, so the next attempt resumes.
                    bus.publish(UPLOAD_ABORTED, user=user, file=name,
                                received=written, total=total,
                                resume_at=written, node=ctx.node_id)
                    return
                handle.write(chunk)
                written += len(chunk)
                remaining -= len(chunk)
                if written >= next_report:
                    bus.publish(UPLOAD_PROGRESS, user=user, file=name,
                                done=written, total=total, node=ctx.node_id)
                    next_report = written + PROGRESS_EVERY
        finally:
            handle.close()

    size = ctx.storage.finalize(name)
    ctx.policy.claim(user, name)
    bus.publish(UPLOAD_COMPLETED, user=user, file=name, size=size, node=ctx.node_id)
    send_json(sock, {"ok": True, "status": "complete", "size": size})


# ------------------------------------------------------------------- download
def handle_download(ctx, req, sock, stream):
    user = req.get("user", "anonymous")
    name = ctx.storage.safe_name(req.get("file", ""))
    offset = int(req.get("offset", 0))

    ctx.policy.reload()
    if not ctx.storage.exists(name):
        send_json(sock, {"ok": False, "error": "file not found"})
        return
    if not ctx.policy.can_read(user, name):
        return _deny(ctx, sock, user, name, "download",
                     "no read permission on %s" % name)

    size = ctx.storage.size(name)
    send_json(sock, {"ok": True, "size": size, "offset": offset, "node": ctx.node_id})
    bus.publish(DOWNLOAD_STARTED, user=user, file=name, offset=offset,
                total=size, node=ctx.node_id)

    sent = offset
    next_report = sent + PROGRESS_EVERY
    handle = ctx.storage.open_for_read(name, offset)
    try:
        while True:
            chunk = handle.read(config.CHUNK_SIZE)
            if not chunk:
                break
            sock.sendall(chunk)
            sent += len(chunk)
            if sent >= next_report:
                bus.publish(DOWNLOAD_PROGRESS, user=user, file=name,
                            done=sent, total=size, node=ctx.node_id)
                next_report = sent + PROGRESS_EVERY
    except (socket.error, OSError):
        # Receiver disappeared half way through; it can resume from `sent`.
        bus.publish(DOWNLOAD_ABORTED, user=user, file=name, sent=sent,
                    total=size, node=ctx.node_id)
        return
    finally:
        handle.close()

    bus.publish(DOWNLOAD_COMPLETED, user=user, file=name, size=size, node=ctx.node_id)


# ------------------------------------------------------------ small metadata ops
def handle_list(ctx, req, sock, stream):
    user = req.get("user", "anonymous")
    ctx.policy.reload()
    allowed = set(ctx.policy.visible_to(user))
    rules = ctx.policy.all_rules()

    files = []
    for entry in ctx.storage.list_files():
        if entry["name"] not in allowed:
            continue
        rule = rules.get(entry["name"], {})
        entry = dict(entry)
        entry["owner"] = rule.get("owner", "-")
        entry["public"] = rule.get("public", False)
        files.append(entry)
    send_json(sock, {"ok": True, "files": files, "node": ctx.node_id})


def handle_stat(ctx, req, sock, stream):
    """Tell the client how much of an interrupted upload already landed."""
    name = ctx.storage.safe_name(req.get("file", ""))
    send_json(sock, {
        "ok": True,
        "file": name,
        "complete": ctx.storage.exists(name),
        "size": ctx.storage.size(name),
        "uploaded": ctx.storage.uploaded_bytes(name),
        "node": ctx.node_id,
    })


def handle_share(ctx, req, sock, stream):
    ctx.policy.reload()
    ok, message = ctx.policy.share(req.get("user"), ctx.storage.safe_name(req.get("file", "")),
                                   req.get("target"), req.get("permission", "read"))
    send_json(sock, {"ok": ok, "message" if ok else "error": message})


def handle_public(ctx, req, sock, stream):
    ctx.policy.reload()
    ok, message = ctx.policy.set_public(req.get("user"),
                                        ctx.storage.safe_name(req.get("file", "")),
                                        bool(req.get("value", True)))
    send_json(sock, {"ok": ok, "message" if ok else "error": message})


def handle_ping(ctx, req, sock, stream):
    """Health check used by the load balancer; also carries the load metric."""
    send_json(sock, {"ok": True, "node": ctx.node_id, "active": ctx.active_count(),
                     "served": ctx.served_count()})


# The dispatch table. The node looks the operation up here; nothing else.
OPS = {
    "UPLOAD": handle_upload,
    "DOWNLOAD": handle_download,
    "LIST": handle_list,
    "STAT": handle_stat,
    "SHARE": handle_share,
    "PUBLIC": handle_public,
    "PING": handle_ping,
}
