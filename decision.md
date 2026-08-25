# decision.md — what was decided, and why

Each entry is a choice made while building this project, the reason for it, and
the option that was rejected. These are the questions most likely to be asked in
the viva.

---

### 1. Node.js with built-in modules only, no framework

**Decision.** Plain `net`, `fs`, `stream`, `events`, `http`. No Express, no
socket.io, no dependencies at all — `package.json` has an empty `dependencies`
block on purpose.

**Why.** The project must run on any lab machine with Node installed and
nothing else; there is no `npm install` step to fail behind a college
firewall. It also keeps the interesting parts visible: with a framework, the
streaming and the event handling would be hidden inside somebody else's library
instead of something we wrote and can explain.

**Rejected.** Express + multer for uploads — three lines to write, but multer
buffers the upload, which is exactly the wrong behaviour for a multi-GB file,
and it would hide every part of the assignment that is being marked.

---

### 2. Event-driven via EventEmitter

**Decision.** The event bus is a 30-line subclass of Node's built-in
`EventEmitter`, adding only `publish()` (stamps a type and timestamp) and a
`'*'` wildcard channel. [src/common/events.js](src/common/events.js)

**Why.** It satisfies "architecture: event-driven" using the language's own
model rather than something invented for the assignment — `EventEmitter` is the
same mechanism Node's own sockets and streams use. The proof that it is real:
[src/server/handlers.js](src/server/handlers.js) contains no `console.log`, no
counters and no reference to the monitor — yet the dashboard, the counters and
the server log all update.

**Rejected.**
* *A message broker (Redis / RabbitMQ / Kafka)* — a real event backbone, but it
  adds an external service to install and configure for zero educational gain
  here.
* *Writing a custom pub/sub class* — pointless when the runtime ships one.

---

### 3. One event loop per node, not a thread pool

**Decision.** Each storage node is a single Node.js process with one event
loop. Concurrency comes from non-blocking I/O, not from threads. Three such
nodes run side by side behind the balancer.

**Why.** The reason a file server wants threads is so that one client's 5 GB
upload cannot block another client's small request. Node achieves that without
threads: it never sits waiting on a disk or a socket, it registers a callback
and serves somebody else meanwhile. The benchmark confirms it — eight
simultaneous 50 MB uploads (400 MB total) finish in ~0.5 s, while a single
50 MB upload takes 0.10 s. If they were serialised, eight would need 0.8 s.

The platform is also genuinely parallel at the **process** level: three node
processes use three CPU cores.

**The honest limitation** (say this before you are asked). A single node cannot
use more than one core for its own work. If the bottleneck were CPU —
encryption, compression, checksumming every chunk — this would be the wrong
design. For file transfer the bottleneck is disk and network, so the event loop
is the right tool.

**Rejected, and what we would switch to.** If literal threads were required,
Node's `cluster` module is a ~15 line change to
[src/server/node.js](src/server/node.js): the primary process forks N workers
that share one listening socket, and the OS distributes connections between
them. `worker_threads` gives real threads inside one process, but passing an
open socket between threads needs extra plumbing and would make the code harder
to explain for no benefit here.

---

### 4. Nodes are stateless workers over one shared storage folder

**Decision.** All three nodes read and write the same `storage/` directory.

**Why.** This single choice makes failover almost free: if node3 dies, node1
has the same bytes, so the client just reconnects and resumes. It matches the
problem statement's phrase "a central local cloud" — one storage pool, several
server processes in front of it.

**Rejected.** *Each node owns its own disk plus replication between nodes.*
That is what a real distributed store does, but it needs a replication
protocol, consistency handling and a placement directory — far beyond a lab
project, and it would bury the features that are actually being marked.

---

### 5. The balancer redirects; it does not proxy

**Decision.** The client asks `WHERE`, gets an address, and then sends the file
bytes **directly** to that node.

**Why.** If the balancer relayed the data, every byte of every GB-sized
transfer would pass through one process — it would become the bottleneck and a
single point of failure for throughput. Redirecting keeps it doing metadata
work only.

**Rejected.** A proxying balancer (like nginx) — simpler for the client, but it
would make the "large files" and "load balancing" requirements fight each
other.

---

### 6. Least-connections, corrected with a `pending` counter

**Decision.** Pick the healthy node with the smallest `active + pending`, where
`pending` counts clients handed out since the last health ping.

**Why.** Plain least-connections looked correct but failed the first test:
eight clients starting together all read the same 2-second-old "0 active"
figure and were all sent to one node. Counting our own recent hand-outs fixed
it, and the benchmark now shows a 3 / 3 / 2 split across the three nodes.

**Rejected.** *Round-robin* — spreads counts evenly but ignores that one client
may be uploading 10 GB while another does a 1 KB `list`. Least-connections
reacts to real load.

---

### 7. `.part` file + rename = the entire crash-recovery scheme

**Decision.** An upload in progress is `x.part`; it is renamed to `x` only
after the last byte. The size of the `.part` file *is* the resume point.

**Why.** No database, no journal, no chunk manifest, no metadata to keep in
sync — and it is impossible for a half-written file to be served as complete,
because it does not have the real name yet. `fs.rename` is atomic on both
Windows and Linux.

**Rejected.** *A chunk manifest recording which numbered chunks arrived.* That
allows out-of-order and parallel chunk upload, but needs extra state that
itself can be corrupted by a crash. Sequential append gives 95% of the benefit
for 5% of the code.

---

### 8. Manual backpressure on upload, `pipeline` on download

**Decision.** The download path is one `await pipeline(file, counter, socket)`.
The upload path uses a hand-written `receiveBytes()` loop instead.

**Why.** They are not symmetric. On download the server only sends, so a
pipeline is perfect and Node handles backpressure for free. On upload the
connection must stay **open after the body** so the two sides can still
exchange a final JSON reply — so the server has to stop at exactly
`total - offset` bytes rather than reading until the stream ends. That means
handling backpressure explicitly: when `file.write()` returns `false` we
`socket.pause()`, and only `socket.resume()` on the file's `'drain'` event.

Without that pause, a fast network and a slow disk would grow an unbounded
buffer in memory — the classic bug that kills a naive implementation on a
multi-GB file.

---

### 9. A refusal is not retried; a network failure is

**Decision.** Two error classes: `TransferError` (retry on another node) and
`RemoteRefused` (give up immediately).

**Why.** A user without permission would otherwise see four identical
"no read permission" failures and a 6-second wait. Asking a different node
cannot change a policy answer, because all nodes read the same policy file.

---

### 10. Policy writes are guarded twice: a lock file *and* a promise queue

**Decision.** `policies.json` is re-read, modified and saved inside a `'wx'`
lock file, and the whole operation is queued behind a promise chain.
[src/server/policy.js](src/server/policy.js) `_exclusive`

**Why — and this is the most interesting bug in the project.** Two different
races had to be closed:

* **Across processes:** three nodes each held their own in-memory copy of the
  whole file and each wrote all of it back. With 8 uploads landing on 3 nodes
  at once, one of the 8 ownership rules disappeared — a classic lost update.
  A thread lock cannot help, because these are separate processes; the lock has
  to live in the filesystem where all of them can see it.
* **Inside one process:** it is tempting to think a single-threaded runtime is
  automatically safe. It is not. Every `await` is a point where the event loop
  can switch to another request, so two connections can interleave in the
  middle of a read-modify-write just as two threads would. The promise queue
  serialises them.

**Rejected.** *SQLite with its own locking* — correct and robust, but a JSON
file that the examiner can open and read in five seconds is worth more here
than a binary database.

---

### 11. The dashboard reads files; it never talks to the nodes

**Decision.** Monitors write `runtime/node_N.json` once a second; the balancer
writes `runtime/balancer.json`; the dashboard only reads those.

**Why.** It keeps the dashboard completely decoupled — it cannot slow a
transfer down, cannot crash a node, and still works when every server process
is dead (it just shows everything as DOWN, which is the correct answer). Files
are written to `.tmp` and renamed so a reader never sees half a file.

**Rejected.** *WebSockets pushing events live.* Prettier and more "real-time",
but it would mean the nodes hold connections to the dashboard, and a slow
browser could then affect the file servers.

---

### 12. Progress events are throttled to one per 8 MB

**Decision.** `PROGRESS_EVERY = 8 * CHUNK_SIZE` in
[src/server/handlers.js](src/server/handlers.js).

**Why.** Publishing one event per 1 MB chunk means 10,240 events for a 10 GB
upload, each one walking the whole listener list. Every 8 MB is still smooth on
screen (the progress bar updates ~125 times for a 1 GB file) at an eighth of
the cost.

---

### 13. Identity is a plain user name, with no password

**Decision.** The client passes `"user": "alice"` and the server believes it.

**Why.** The requirement is *file sharing policies* — who may access what — not
authentication. Adding password hashing and sessions would double the code
without addressing anything in the question.

**Stated limitation.** This is not secure against a malicious user, who could
simply claim to be alice. Real authentication (a password or a token checked on
every request) would be the first thing to add for production use. The policy
engine itself would not change: only the line that decides *who the caller is*.
