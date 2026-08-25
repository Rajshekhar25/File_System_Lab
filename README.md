# File Sharing Platform (Event-Driven, Multithreaded)

A small "local cloud" for uploading and downloading very large files. Written in
**plain Python 3 with the standard library only** — nothing to install.

It covers every point of the lab problem statement:

| Requirement from the question | Where it is implemented |
|---|---|
| Upload / download of large (~GB) files | [src/client/client.py](src/client/client.py) + [src/server/handlers.py](src/server/handlers.py) — streamed in 1 MB chunks, never held in RAM |
| Central local cloud, multithreaded server | [src/server/node.py](src/server/node.py) — `ThreadingTCPServer`, one thread per client |
| File sharing policies (who can read / write) | [src/server/policy.py](src/server/policy.py) — owner / readers / writers / public |
| Visualization of availability and status | [src/dashboard/dashboard.py](src/dashboard/dashboard.py) — live web page at `http://127.0.0.1:8080` |
| Abruptly terminated transfers | `.part` files + resume, see [flow.md](flow.md) |
| Server failures | Health checks + automatic failover to another node |
| Load balancing | [src/balancer/balancer.py](src/balancer/balancer.py) — least-connections |
| Performance analysis | [src/bench/benchmark.py](src/bench/benchmark.py) |
| **Architecture: event-driven** | [src/common/events.py](src/common/events.py) — everything publishes events, listeners react |

---

## 1. Architecture in one picture

```
                      +---------------------+
                      |   Load balancer     |   port 9000
   "where do I go?"   |  - pings every node |
   +----------------->|  - picks least busy |
   |                  +----------+----------+
   |                             | health ping (PING)
   |                             v
+--+-----+        upload/download bytes        +---------------------------+
| Client |------------------------------------>|  Storage node 1  :9101    |
+--------+                                     |  Storage node 2  :9102    |
                                               |  Storage node 3  :9103    |
                                               |  (multithreaded)          |
                                               +-------------+-------------+
                                                             |
                                    every action -> EVENT BUS (in-process)
                                                             |
                                    +------------------------+
                                    |                        |
                              console logger            monitor
                                                             |
                                              writes runtime/node_N.json
                                                             |
                                                             v
                                                   +--------------------+
                                                   | Dashboard  :8080   |
                                                   | reads the JSON,    |
                                                   | draws the status   |
                                                   +--------------------+
```

The balancer only tells the client **which** node to use. The file bytes go
straight from client to node, so the balancer never becomes a bottleneck.

All three nodes read and write the **same `storage/` folder**, so a node is
interchangeable — that is what makes failover work with no data copying.

---

## 2. Why it is called "event-driven"

No component calls the monitor, the logger or the dashboard directly. A handler
only announces what happened:

```python
bus.publish("upload.completed", user="alice", file="movie.iso", size=size)
```

Anyone interested has subscribed in advance:

```python
bus.subscribe("*", monitor.on_event)     # counts it and puts it on the dashboard
bus.subscribe("*", console_logger)       # prints it on the server console
```

To add a new feature (say, an email alert on every aborted upload) you write one
listener and subscribe it. **You never touch the upload code.** That is the
whole point of the architecture, and it is the easiest thing to demonstrate:
open [src/server/handlers.py](src/server/handlers.py) and show that it contains
no `print`, no counters and no logging.

Events used: `client.connected`, `client.disconnected`, `access.denied`,
`upload.started / progress / completed / aborted`,
`download.started / progress / completed / aborted`,
`policy.changed`, `node.started`.

---

## 3. Folder layout

```
src/
  common/
    config.py      every setting (ports, folders, chunk size) in one file
    events.py      the publish/subscribe event bus
    protocol.py    the wire format: one JSON line + raw bytes
  server/
    storage.py     .part files, resume points, finalize
    policy.py      who may read / write / share which file
    handlers.py    one function per operation (UPLOAD, DOWNLOAD, LIST, ...)
    monitor.py     event listener -> counters -> runtime/node_N.json
    node.py        the multithreaded TCP server
  balancer/
    balancer.py    health checks + least-connections choice
  dashboard/
    dashboard.py   tiny HTTP server exposing /api/status
    index.html     the live status page
  client/
    client.py      the command line client (upload, download, list, share)
  bench/
    benchmark.py   performance analysis
storage/           the shared "cloud" disk (created at run time)
runtime/           live status files (created at run time)
```

---

## 4. How to run it

Open **five** terminals in the project folder (or just double-click
`start_all.bat` on Windows, which opens them for you).

```
python -m src.balancer.balancer        # terminal 1
python -m src.server.node 1            # terminal 2
python -m src.server.node 2            # terminal 3
python -m src.server.node 3            # terminal 4
python -m src.dashboard.dashboard      # terminal 5
```

Then open <http://127.0.0.1:8080> in a browser and keep it visible — everything
you do next will appear there live.

---

## 5. Client commands

```
python -m src.client.client <user> upload   <local file> [name on server]
python -m src.client.client <user> download <name on server> [local file]
python -m src.client.client <user> list
python -m src.client.client <user> share    <file> <other user> <read|write>
python -m src.client.client <user> public   <file> <on|off>
python -m src.client.client <user> nodes
```

---

## 6. Demo script for the lab (run these in order)

**a) Upload and ownership**

```
python -m src.client.client alice upload benchdata/big.bin report.iso
python -m src.client.client alice list
```

**b) Sharing policy** — bob is refused, then allowed:

```
python -m src.client.client bob download report.iso        -> "no read permission"
python -m src.client.client alice share report.iso bob read
python -m src.client.client bob download report.iso        -> works
```

The refusal shows up on the dashboard as an `access.denied` event.

**c) Abruptly terminated transfer**

Start a big upload and press **Ctrl+C** half way:

```
python -m src.client.client dave upload benchdata/big.bin big.bin
```

Look in `storage/` — there is a `big.bin.part` file, and **no** `big.bin`
(an unfinished file can never be mistaken for a real one). Now run the exact
same command again:

```
resuming big.bin from 187.0 MB
```

It continues from where it stopped instead of starting over.

**d) Server failure**

Start a big download, then close the terminal of the node that is serving it
(the dashboard shows which one). The client prints:

```
attempt 1 -> node3 (127.0.0.1:9103)
  transfer interrupted: node closed the connection early
resuming download from 84.0 MB
attempt 2 -> node1 (127.0.0.1:9101)
downloaded big.bin (600.0 MB) ...
```

Within two seconds the dashboard marks node3 **DOWN**, and the balancer stops
offering it to anybody.

**e) Load balancing and performance**

```
python -m src.bench.benchmark --size 50 --threads 8
```

---

## 7. Performance analysis (measured on this machine)

Three nodes, 50 MB per client thread, loopback network:

| Clients | Upload aggregate | Download aggregate | Spread over nodes |
|---:|---:|---:|---|
| 1 | 759 MB/s | 562 MB/s | 1 node used |
| 2 | 704 MB/s | 642 MB/s | 1 + 1 |
| 4 | 978 MB/s | 1053 MB/s | 1 + 1 + 2 |
| 8 | 901 MB/s | 872 MB/s | 3 + 3 + 2 |

What to say about these numbers:

* Throughput **rises from 1 to 4 clients** — the extra threads really do work in
  parallel, because each connection has its own thread and the reads/writes
  release the Python GIL.
* It **flattens after 4** — with everything on one machine the bottleneck moves
  to the disk and the loopback copy, not to the server code.
* The **spread column** proves the balancer works: with 8 clients the work is
  split 3 / 3 / 2 instead of all landing on one node.
* These numbers are high because client, server and disk are the same laptop
  and Windows caches the file. On a real network the limit would be the link
  speed; the design (raw byte streaming, no base64, no proxying) is what keeps
  it at line speed.

Every run is also appended to `runtime/perf_report.txt`.

---

## 8. Related documents

* [flow.md](flow.md) — step-by-step flow of every operation, with diagrams.
* [decision.md](decision.md) — every design decision and why the alternative
  was rejected.
