"""Command line client.

It knows only the balancer's address. For every job it asks "where should I
go?", talks to the node it is given, and if that node dies mid-transfer it asks
again and resumes from wherever the bytes stopped. That single retry loop is
what covers both "abruptly terminated transfer" and "server failure".

Usage
-----
    python -m src.client.client alice upload  bigfile.iso
    python -m src.client.client bob   download bigfile.iso saved.iso
    python -m src.client.client alice list
    python -m src.client.client alice share  bigfile.iso bob read
    python -m src.client.client alice public bigfile.iso on
    python -m src.client.client alice nodes
"""

import os
import socket
import sys
import time

from ..common import config
from ..common.protocol import send_json, recv_json, human_size

RETRIES = 3            # how many times to re-dial after a failure
RETRY_PAUSE = 1.5      # seconds to wait before re-dialling
QUIET = False          # the benchmark sets this to hide per-chunk progress


class TransferError(Exception):
    """Something went wrong; retrying on another node may help."""


class RemoteRefused(TransferError):
    """The node answered properly and said no (e.g. no permission).

    Retrying would only repeat the same refusal, so it is never retried.
    """


class Client:
    def __init__(self, user):
        self.user = user

    # ------------------------------------------------------------ plumbing
    def _ask_balancer(self, payload):
        sock = socket.create_connection((config.BALANCER_HOST, config.BALANCER_PORT),
                                        timeout=config.SOCKET_TIMEOUT)
        try:
            send_json(sock, payload)
            return recv_json(sock.makefile("rb"))
        finally:
            sock.close()

    def pick_node(self):
        reply = self._ask_balancer({"op": "WHERE"})
        if not reply or not reply.get("ok"):
            raise RemoteRefused((reply or {}).get("error", "balancer unreachable"))
        return reply

    def node_status(self):
        return self._ask_balancer({"op": "STATUS"})

    def _open_node(self, node):
        sock = socket.create_connection((node["host"], node["port"]),
                                        timeout=config.SOCKET_TIMEOUT)
        return sock, sock.makefile("rb")

    def _simple_request(self, payload):
        """One request, one JSON answer, connection closed."""
        node = self.pick_node()
        sock, stream = self._open_node(node)
        try:
            send_json(sock, payload)
            return recv_json(stream), node
        finally:
            sock.close()

    # -------------------------------------------------------------- uploads
    def _remote_state(self, remote_name):
        reply, _ = self._simple_request({"op": "STAT", "user": self.user,
                                         "file": remote_name})
        return reply or {}

    def upload(self, local_path, remote_name=None):
        if not os.path.isfile(local_path):
            raise TransferError("no such local file: %s" % local_path)

        remote_name = remote_name or os.path.basename(local_path)
        total = os.path.getsize(local_path)
        started = time.time()

        for attempt in range(1, RETRIES + 2):
            state = self._remote_state(remote_name)
            offset = int(state.get("uploaded", 0))
            if offset:
                print("resuming %s from %s" % (remote_name, human_size(offset)))

            node = self.pick_node()
            print("attempt %d -> %s (%s:%d)" % (attempt, node["node"],
                                                node["host"], node["port"]))
            try:
                self._push(node, local_path, remote_name, offset, total)
            except RemoteRefused:
                raise                       # a "no" is final, do not retry
            except (OSError, TransferError) as exc:
                print("  transfer interrupted: %s" % exc)
                if attempt > RETRIES:
                    raise TransferError("upload failed after %d attempts" % attempt)
                time.sleep(RETRY_PAUSE)
                continue

            seconds = max(time.time() - started, 1e-6)
            print("uploaded %s (%s) in %.2fs  = %.2f MB/s"
                  % (remote_name, human_size(total), seconds,
                     total / seconds / (1024 * 1024)))
            return True
        return False

    def _push(self, node, local_path, remote_name, offset, total):
        sock, stream = self._open_node(node)
        try:
            send_json(sock, {"op": "UPLOAD", "user": self.user, "file": remote_name,
                             "offset": offset, "total": total})
            reply = recv_json(stream)
            if not reply or not reply.get("ok"):
                raise RemoteRefused((reply or {}).get("error", "node refused upload"))

            sock.settimeout(None)  # a multi-GB body must not hit the control timeout
            sent = offset
            with open(local_path, "rb") as fh:
                fh.seek(offset)
                while sent < total:
                    chunk = fh.read(config.CHUNK_SIZE)
                    if not chunk:
                        break
                    sock.sendall(chunk)
                    sent += len(chunk)
                    _progress("upload", remote_name, sent, total)
            if not QUIET:
                print()

            sock.settimeout(config.SOCKET_TIMEOUT)
            final = recv_json(stream)
            if not final or not final.get("ok"):
                raise TransferError((final or {}).get("error", "node did not confirm"))
        finally:
            sock.close()

    # ------------------------------------------------------------ downloads
    def download(self, remote_name, out_path=None):
        out_path = out_path or os.path.join("downloads", remote_name)
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        part_path = out_path + ".part"
        started = time.time()

        for attempt in range(1, RETRIES + 2):
            offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
            if offset:
                print("resuming download from %s" % human_size(offset))

            node = self.pick_node()
            print("attempt %d -> %s (%s:%d)" % (attempt, node["node"],
                                                node["host"], node["port"]))
            try:
                total = self._pull(node, remote_name, part_path, offset)
            except RemoteRefused:
                raise                       # a "no" is final, do not retry
            except (OSError, TransferError) as exc:
                print("  transfer interrupted: %s" % exc)
                if attempt > RETRIES:
                    raise TransferError("download failed after %d attempts" % attempt)
                time.sleep(RETRY_PAUSE)
                continue

            os.replace(part_path, out_path)
            seconds = max(time.time() - started, 1e-6)
            print("downloaded %s (%s) in %.2fs  = %.2f MB/s -> %s"
                  % (remote_name, human_size(total), seconds,
                     total / seconds / (1024 * 1024), out_path))
            return True
        return False

    def _pull(self, node, remote_name, part_path, offset):
        sock, stream = self._open_node(node)
        try:
            send_json(sock, {"op": "DOWNLOAD", "user": self.user,
                             "file": remote_name, "offset": offset})
            reply = recv_json(stream)
            if not reply or not reply.get("ok"):
                raise RemoteRefused((reply or {}).get("error", "node refused download"))

            total = int(reply["size"])
            sock.settimeout(None)
            received = offset
            mode = "r+b" if os.path.exists(part_path) else "wb"
            with open(part_path, mode) as fh:
                fh.seek(offset)
                fh.truncate(offset)
                while received < total:
                    chunk = stream.read(min(config.CHUNK_SIZE, total - received))
                    if not chunk:
                        raise TransferError("node closed the connection early")
                    fh.write(chunk)
                    received += len(chunk)
                    _progress("download", remote_name, received, total)
            if not QUIET:
                print()
            return total
        finally:
            sock.close()

    # ------------------------------------------------------- metadata calls
    def list_files(self):
        reply, node = self._simple_request({"op": "LIST", "user": self.user})
        return reply, node

    def share(self, filename, target_user, permission):
        reply, _ = self._simple_request({"op": "SHARE", "user": self.user,
                                         "file": filename, "target": target_user,
                                         "permission": permission})
        return reply

    def set_public(self, filename, value):
        reply, _ = self._simple_request({"op": "PUBLIC", "user": self.user,
                                         "file": filename, "value": value})
        return reply


def _progress(kind, name, done, total):
    if QUIET or not total:
        return
    percent = done * 100.0 / total
    sys.stdout.write("\r  %s %s  %5.1f%%  (%s / %s)"
                     % (kind, name, percent, human_size(done), human_size(total)))
    sys.stdout.flush()


# ------------------------------------------------------------------- CLI glue
def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 1

    user, command, args = argv[1], argv[2].lower(), argv[3:]
    client = Client(user)

    try:
        if command == "upload":
            client.upload(args[0], args[1] if len(args) > 1 else None)

        elif command == "download":
            client.download(args[0], args[1] if len(args) > 1 else None)

        elif command == "list":
            reply, node = client.list_files()
            files = (reply or {}).get("files", [])
            print("files visible to %s (answered by %s)" % (user, node["node"]))
            if not files:
                print("  (nothing shared with you yet)")
            for item in files:
                print("  %-28s %10s  %-11s owner=%-8s %s"
                      % (item["name"], human_size(item["size"]), item["state"],
                         item["owner"], "public" if item["public"] else ""))

        elif command == "share":
            reply = client.share(args[0], args[1], args[2] if len(args) > 2 else "read")
            print(reply.get("message") or reply.get("error"))

        elif command == "public":
            value = len(args) < 2 or args[1].lower() in ("on", "true", "yes", "1")
            reply = client.set_public(args[0], value)
            print(reply.get("message") or reply.get("error"))

        elif command == "nodes":
            reply = client.node_status()
            for name, entry in sorted((reply or {}).get("nodes", {}).items()):
                print("  %-7s %s:%-5d %-5s active=%-3d served=%d"
                      % (name, entry["host"], entry["port"],
                         "UP" if entry["alive"] else "DOWN",
                         entry["active"], entry["served"]))
        else:
            print("unknown command: %s" % command)
            return 1

    except (TransferError, IndexError) as exc:
        print("error: %s" % exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
