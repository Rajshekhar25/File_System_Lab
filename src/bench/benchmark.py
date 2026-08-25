"""Performance analysis tool.

It runs N client threads at the same time, first uploading and then downloading
a file of a chosen size, and reports per-thread time plus the aggregate
throughput. Running it with 1, 2, 4 and 8 threads is what produces the table
for the report: it shows whether the multithreaded server actually scales and
how the balancer spread the work over the nodes.

    python -m src.bench.benchmark --size 50 --threads 4
"""

import argparse
import os
import sys
import threading
import time

from ..client import client as client_module
from ..client.client import Client
from ..common import config
from ..common.protocol import human_size

WORK_DIR = os.path.join(config.BASE_DIR, "benchdata")
REPORT = os.path.join(config.RUNTIME_DIR, "perf_report.txt")


def make_test_file(path, size_mb):
    """Create (once) a file of the requested size to push around."""
    if os.path.exists(path) and os.path.getsize(path) == size_mb * 1024 * 1024:
        return path
    block = os.urandom(1024 * 1024)
    with open(path, "wb") as fh:
        for _ in range(size_mb):
            fh.write(block)
    return path


class Result:
    def __init__(self, worker):
        self.worker = worker
        self.node = "-"
        self.seconds = 0.0
        self.ok = False


def _upload_worker(index, source, size_bytes, results):
    client = Client("bench%d" % index)
    result = Result(index)
    started = time.time()
    try:
        node = client.pick_node()
        result.node = node["node"]
        client._push(node, source, "bench_%d.bin" % index, 0, size_bytes)
        result.ok = True
    except Exception as exc:                      # noqa: BLE001 - report, never crash
        print("  worker %d upload failed: %s" % (index, exc))
    result.seconds = time.time() - started
    results[index] = result


def _download_worker(index, _source, size_bytes, results):
    client = Client("bench%d" % index)
    result = Result(index)
    target = os.path.join(WORK_DIR, "out_%d.bin" % index)
    started = time.time()
    try:
        node = client.pick_node()
        result.node = node["node"]
        client._pull(node, "bench_%d.bin" % index, target + ".part", 0)
        os.replace(target + ".part", target)
        result.ok = True
    except Exception as exc:                      # noqa: BLE001
        print("  worker %d download failed: %s" % (index, exc))
    result.seconds = time.time() - started
    results[index] = result


def run_phase(name, worker, threads, size_bytes, extra):
    results = {}
    workers = []
    print("\n%s: %d thread(s) x %s" % (name, threads, human_size(size_bytes)))
    wall_start = time.time()
    for index in range(threads):
        thread = threading.Thread(target=worker,
                                  args=(index, extra, size_bytes, results))
        thread.start()
        workers.append(thread)
    for thread in workers:
        thread.join()
    wall = max(time.time() - wall_start, 1e-6)

    lines = ["", "%s  (%d threads, %s each)" % (name, threads, human_size(size_bytes))]
    lines.append("  worker   node     time(s)   MB/s     status")
    for index in sorted(results):
        r = results[index]
        rate = (size_bytes / r.seconds / (1024 * 1024)) if r.seconds else 0
        lines.append("  %-8d %-8s %-9.2f %-8.2f %s"
                     % (r.worker, r.node, r.seconds, rate, "ok" if r.ok else "FAILED"))

    done = sum(1 for r in results.values() if r.ok)
    total_bytes = done * size_bytes
    lines.append("  total: %s in %.2fs  ->  aggregate %.2f MB/s  (%d/%d succeeded)"
                 % (human_size(total_bytes), wall,
                    total_bytes / wall / (1024 * 1024), done, threads))
    spread = {}
    for r in results.values():
        spread[r.node] = spread.get(r.node, 0) + 1
    lines.append("  balancer spread: " + ", ".join("%s=%d" % kv for kv in sorted(spread.items())))

    text = "\n".join(lines)
    print(text)
    return text


def main():
    parser = argparse.ArgumentParser(description="throughput test for the platform")
    parser.add_argument("--size", type=int, default=20, help="file size in MB (default 20)")
    parser.add_argument("--threads", type=int, default=4, help="concurrent clients (default 4)")
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()

    os.makedirs(WORK_DIR, exist_ok=True)
    config.ensure_dirs()
    client_module.QUIET = True   # progress bars from 8 threads are unreadable
    size_bytes = args.size * 1024 * 1024

    source = make_test_file(os.path.join(WORK_DIR, "payload_%dMB.bin" % args.size), args.size)
    print("test payload: %s (%s)" % (source, human_size(size_bytes)))

    report = ["=" * 62,
              "performance run at %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
              "chunk size: %s" % human_size(config.CHUNK_SIZE)]
    report.append(run_phase("UPLOAD", _upload_worker, args.threads, size_bytes, source))
    if not args.skip_download:
        report.append(run_phase("DOWNLOAD", _download_worker, args.threads, size_bytes, None))

    with open(REPORT, "a", encoding="utf-8") as fh:
        fh.write("\n".join(report) + "\n")
    print("\nreport appended to %s" % REPORT)


if __name__ == "__main__":
    sys.exit(main())
