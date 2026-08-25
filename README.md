# File Sharing Platform (Event-Driven, Node.js)

A small "local cloud" for uploading and downloading very large files. Written in
**plain Node.js with built-in modules only** — `npm install` installs nothing,
because there are no dependencies.

It covers every point of the lab problem statement:

| Requirement from the question | Where it is implemented |
|---|---|
| Upload / download of large (~GB) files | [src/client/client.js](src/client/client.js) + [src/server/handlers.js](src/server/handlers.js) — streamed in 1 MB chunks, never held in RAM |
| Central local cloud server | [src/server/node.js](src/server/node.js) — three nodes over one shared disk |
| Concurrent clients | Node's event loop; see **section 3** below for the "multithreaded" question |
| File sharing policies (who can read / write) | [src/server/policy.js](src/server/policy.js) — owner / readers / writers / public |
| Visualization of availability and status | [src/dashboard/dashboard.js](src/dashboard/dashboard.js) — live web page at `http://127.0.0.1:8080` |
| Abruptly terminated transfers | `.part` files + resume, see [flow.md](flow.md) |
| Server failures | Health checks + automatic failover to another node |
| Load balancing | [src/balancer/balancer.js](src/balancer/balancer.js) — least-connections |
| Performance analysis | [src/bench/benchmark.js](src/bench/benchmark.js) |
| **Architecture: event-driven** | [src/common/events.js](src/common/events.js) — everything publishes events, listeners react |

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
                                               +-------------+-------------+
                                                             |
                                    every action -> EVENT BUS (EventEmitter)
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

In Node this is not something bolted on: `EventEmitter` is the same mechanism
that sockets, streams and the HTTP server already use internally. Our bus is a
30-line subclass of it.

No component calls the monitor, the logger or the dashboard directly. A handler
only announces what happened:

```js
bus.publish('upload.completed', { user: 'alice', file: 'movie.iso', size });
```

Anyone interested has subscribed in advance:

```js
bus.subscribe('*', (e) => monitor.onEvent(e));  // counts it, puts it on the dashboard
bus.subscribe('*', consoleLogger);              // prints it on the server console
```

To add a new feature (say, an email alert on every aborted upload) you write one
listener and subscribe it. **You never touch the upload code.** That is the
whole point of the architecture, and it is the easiest thing to demonstrate:
open [src/server/handlers.js](src/server/handlers.js) and show that it contains
no `console.log`, no counters and no logging.

Events used: `client.connected`, `client.disconnected`, `access.denied`,
`upload.started / progress / completed / aborted`,
`download.started / progress / completed / aborted`,
`policy.changed`, `node.started`.

---

## 3. The "multithreaded server" question — read this before the viva

The problem statement says *multithreaded*. Node.js runs **one thread with an
event loop**, so be ready for the question. The honest answer has three parts:

**a) The goal of multithreading is met.** The reason a file server wants
threads is so that one client's 5 GB upload does not block another client's
small request. Node achieves that without threads: it never sits waiting for a
disk or a socket. It registers "wake me when the next chunk is ready" and
serves somebody else in the meantime. The benchmark below proves it — eight
simultaneous 50 MB transfers all complete, and the total time is far less than
eight times a single transfer.

**b) The platform is genuinely parallel at the process level.** Three node
processes run side by side, on three CPU cores, behind the load balancer. So
the *system* does use multiple cores; it just does not use multiple threads
*inside* one node.

**c) The honest limitation.** A single node cannot use more than one core for
its own work. If the bottleneck were CPU (encryption, compression, checksums),
this design would be the wrong choice and you would use Node's `worker_threads`
or `cluster` module. For file transfer the bottleneck is disk and network, not
CPU, so the event loop is the right tool.

If your examiner wants literal threads, `cluster` is a ~15 line change to
[src/server/node.js](src/server/node.js) — it forks N workers that share one
listening socket. This is discussed in [decision.md](decision.md) section 3.

---

## 4. Folder layout

```
src/
  common/
    config.js      every setting (ports, folders, chunk size) in one file
    events.js      the publish/subscribe event bus (extends EventEmitter)
    protocol.js    the wire format: one JSON line + raw bytes
  server/
    storage.js     .part files, resume points, finalize
    policy.js      who may read / write / share which file
    handlers.js    one function per operation (UPLOAD, DOWNLOAD, LIST, ...)
    monitor.js     event listener -> counters -> runtime/node_N.json
    node.js        the TCP server
  balancer/
    balancer.js    health checks + least-connections choice
  dashboard/
    dashboard.js   tiny HTTP server exposing /api/status
    index.html     the live status page
  client/
    client.js      the command line client (upload, download, list, share)
  bench/
    benchmark.js   performance analysis
storage/           the shared "cloud" disk (created at run time)
runtime/           live status files (created at run time)
```

---

## 5. How to run it

Requires **Node.js 18 or newer** (`node --version` to check). Nothing to
install.

Open **five** terminals in the project folder (or just double-click
`start_all.bat` on Windows, which opens them for you).

```
npm run balancer      # terminal 1
npm run node1         # terminal 2
npm run node2         # terminal 3
npm run node3         # terminal 4
npm run dashboard     # terminal 5
```

Those are shortcuts for `node src/balancer/balancer.js`,
`node src/server/node.js 1`, and so on — use whichever form you prefer.

Then open <http://127.0.0.1:8080> in a browser and keep it visible — everything
you do next will appear there live.

---

## 6. Client commands

```
node src/client/client.js <user> upload   <local file> [name on server]
node src/client/client.js <user> download <name on server> [local file]
node src/client/client.js <user> list
node src/client/client.js <user> share    <file> <other user> <read|write>
node src/client/client.js <user> public   <file> <on|off>
node src/client/client.js <user> nodes
```

---

## 7. Demo script for the lab (run these in order)

**a) Upload and ownership**

```
node src/client/client.js alice upload benchdata/big.bin report.iso
node src/client/client.js alice list
```

**b) Sharing policy** — bob is refused, then allowed:

```
node src/client/client.js bob download report.iso        -> "no read permission"
node src/client/client.js alice share report.iso bob read
node src/client/client.js bob download report.iso        -> works
```

The refusal shows up on the dashboard as an `access.denied` event.

**c) Abruptly terminated transfer**

Start a big upload and press **Ctrl+C** half way:

```
node src/client/client.js dave upload benchdata/big.bin big.bin
```

Look in `storage/` — there is a `big.bin.part` file, and **no** `big.bin`
(an unfinished file can never be mistaken for a real one). Now run the exact
same command again:

```
resuming big.bin from 288.4 MB
```

It continues from where it stopped instead of starting over.

**d) Server failure**

Start a big download, then close the terminal of the node that is serving it
(the dashboard shows which one). The client prints:

```
attempt 1 -> node3 (127.0.0.1:9103)
  download report.iso   32.7%  (196.4 MB / 600.0 MB)
  transfer interrupted: connection closed after 205913856 bytes
resuming download from 196.4 MB
attempt 2 -> node1 (127.0.0.1:9101)
  download report.iso  100.0%  (600.0 MB / 600.0 MB)
downloaded report.iso (600.0 MB) in 2.85s  = 210.90 MB/s -> downloads/fo3.iso
```

(That is a real captured run — node3 was killed at 32.7%, and the file that
arrived was byte-for-byte identical to the original.)

Within two seconds the dashboard marks node3 **DOWN**, and the balancer stops
offering it to anybody.

**e) Load balancing and performance**

```
node src/bench/benchmark.js --size 50 --clients 8
```

---

## 8. Performance analysis (measured on this machine)

Three nodes, 50 MB per client, loopback network:

| Clients | Upload aggregate | Download aggregate | Spread over nodes |
|---:|---:|---:|---|
| 1 | 500 MB/s | 417 MB/s | 1 node used |
| 2 | 833 MB/s | 575 MB/s | 1 + 1 |
| 4 | 775 MB/s | 602 MB/s | 1 + 1 + 2 |
| 8 | 805 MB/s | 167–317 MB/s | 3 + 3 / 2 |

What to say about these numbers:

* **One event loop really does overlap transfers.** Eight simultaneous 50 MB
  uploads (400 MB total) finish in about 0.5 s. One 50 MB upload alone takes
  0.10 s. If the transfers were serialised, eight would need 0.8 s; instead the
  aggregate throughput rises. This is the evidence for section 3 above.
* **The spread column proves the balancer works** — with 8 clients the work is
  split 3 / 3 / 2 instead of all landing on one node.
* **The 8-client download figure is genuinely slower and varies a lot**
  (measured 167, 161, 237 and 317 MB/s across four repeats). This is a property
  of the *benchmark*, not the server: the upload phase reads the same 50 MB
  payload file eight times, so it is served from the OS cache, while the
  download phase reads eight *different* 50 MB files and writes eight more —
  around 800 MB of real disk traffic. Worth mentioning as an honest observation
  rather than hiding it.
* These numbers are high overall because client, server and disk are the same
  laptop and Windows caches the files. On a real network the limit would be the
  link speed; the design (raw byte streaming, no base64, no proxying through the
  balancer) is what keeps it at line speed.

Every run is also appended to `runtime/perf_report.txt`.

---

## 9. Related documents

* [flow.md](flow.md) — step-by-step flow of every operation, with diagrams.
* [decision.md](decision.md) — every design decision and why the alternative
  was rejected.
