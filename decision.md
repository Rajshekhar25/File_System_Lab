# decision.md — what was decided, and why

Each entry is a choice made while building this project, the reason for it, and
the option that was rejected. These are the questions most likely to be asked in
the viva.

---

### 1. Python standard library only, no framework

**Decision.** Plain `socket`, `socketserver`, `threading`, `json`,
`http.server`. No Flask, no FastAPI, no `pip install` at all.

**Why.** The project must run on any lab machine with Python 3 and nothing else.
It also keeps the interesting parts visible: with a framework, "multithreaded
server" would be one hidden line in somebody else's library instead of something
we wrote and can explain.

**Rejected.** Flask + a WSGI server — easier to write, but the threading, the
chunking and the event loop would all be invisible, which defeats the purpose of
the lab.

---

### 2. Event-driven via an in-process publish/subscribe bus

**Decision.** One tiny `EventBus` class. Handlers publish; the monitor and the
console logger subscribe. [src/common/events.py](src/common/events.py)

**Why.** It is the smallest thing that genuinely satisfies "architecture:
event-driven" and can be shown in 30 lines. The proof is that
[src/server/handlers.py](src/server/handlers.py) contains no `print`, no
counters and no reference to the monitor — yet the dashboard, the counters and
the server log all update.

**Rejected.**
* *A message broker (Redis / RabbitMQ / Kafka)* — a real event backbone, but it
  adds an external service to install and configure for zero educational gain
  here.
* *`asyncio` event loop* — that is event-driven *I/O*, a different meaning of
  the word, and it would clash with the explicit "multithreaded server"
  requirement. Threads are also far easier to explain in a viva.

---

### 3. Thread per connection (`ThreadingTCPServer`)

**Decision.** Each accepted connection gets its own worker thread.

**Why.** The requirement literally says "multithreaded server". It is also the
right model here: file transfer is I/O-bound, and Python releases the GIL during
socket and disk operations, so the threads really do overlap — the benchmark in
the README shows throughput rising from 1 to 4 concurrent clients.

**Rejected.** A fixed thread pool would cap concurrency and add queueing code;
for a lab-sized number of clients, one thread each is simpler and faster to
explain. (Honest limitation: with thousands of clients, thread-per-connection
would not scale, and a pool or `asyncio` would be the correct answer.)

---

### 4. Nodes are stateless workers over one shared storage folder

**Decision.** All three nodes read and write the same `storage/` directory.

**Why.** This single choice makes failover almost free: if node3 dies, node1 has
the same bytes, so the client just reconnects and resumes. It matches the
problem statement's phrase "a central local cloud" — one storage pool, several
server processes in front of it.

**Rejected.** *Each node owns its own disk plus replication between nodes.* That
is what a real distributed store does, but it needs a replication protocol,
consistency handling and a placement directory — far beyond a lab project, and
it would bury the features that are actually being marked.

---

### 5. The balancer redirects; it does not proxy

**Decision.** The client asks `WHERE`, gets an address, and then sends the file
bytes **directly** to that node.

**Why.** If the balancer relayed the data, every byte of every GB-sized transfer
would pass through one process — it would become the bottleneck and a single
point of failure for throughput. Redirecting keeps it doing metadata work only.

**Rejected.** A proxying balancer (like nginx) — simpler for the client, but it
would make the "large files" and "load balancing" requirements fight each other.

---

### 6. Least-connections, corrected with a `pending` counter

**Decision.** Pick the healthy node with the smallest `active + pending`, where
`pending` counts clients handed out since the last health ping.

**Why.** Plain least-connections looked correct but failed the first test: eight
clients starting together all read the same 2-second-old "0 active" figure and
were all sent to node2. Counting our own recent hand-outs fixed it, and the
benchmark now shows a 3 / 3 / 2 split across the three nodes.

**Rejected.** *Round-robin* — spreads counts evenly but ignores that one client
may be uploading 10 GB while another does a 1 KB `list`. Least-connections
reacts to real load.

---

### 7. `.part` file + rename = the entire crash-recovery scheme

**Decision.** An upload in progress is `x.part`; it is renamed to `x` only after
the last byte. The size of the `.part` file *is* the resume point.

**Why.** No database, no journal, no chunk manifest, no metadata to keep in sync
— and it is impossible for a half-written file to be served as complete, because
it does not have the real name yet. `os.replace` is atomic on both Windows and
Linux.

**Rejected.** *A chunk manifest recording which numbered chunks arrived.* That
allows out-of-order and parallel chunk upload, but needs extra state that itself
can be corrupted by a crash. Sequential append gives 95% of the benefit for 5%
of the code.

---

### 8. A refusal is not retried; a network failure is

**Decision.** Two exception types: `TransferError` (retry on another node) and
`RemoteRefused` (give up immediately).

**Why.** Found while testing: a user without permission saw four identical
"no read permission" failures and a 6-second wait. Asking a different node
cannot change a policy answer, because all nodes read the same policy file.

---

### 9. A lock file around every policy write

**Decision.** `policies.json` is re-read, modified and saved inside an
`O_EXCL` lock file. [src/server/policy.py](src/server/policy.py) `_exclusive`

**Why.** This was a real bug, found by the benchmark: with 8 uploads landing on
3 nodes at once, one of the 8 ownership rules disappeared. Three separate
processes were each writing their own in-memory copy of the whole file — a
classic lost update. A thread lock is not enough because the nodes are separate
processes; the lock has to live in the filesystem, where all of them can see it.

**Rejected.** *SQLite with its own locking* — correct and robust, but a JSON
file that the examiner can open and read in five seconds is worth more here than
a binary database.

---

### 10. The dashboard reads files; it never talks to the nodes

**Decision.** Monitors write `runtime/node_N.json` once a second; the balancer
writes `runtime/balancer.json`; the dashboard only reads those.

**Why.** It keeps the dashboard completely decoupled — it cannot slow a transfer
down, cannot crash a node, and still works when every server process is dead
(it just shows everything as DOWN, which is the correct answer). Files are
written to `.tmp` and renamed so a reader never sees half a file.

**Rejected.** *WebSockets pushing events live.* Prettier and more "real-time",
but it would mean the nodes hold connections to the dashboard, and a slow
browser could then affect the file servers.

---

### 11. Progress events are throttled to one per 8 MB

**Decision.** `PROGRESS_EVERY = 8 * CHUNK_SIZE` in
[src/server/handlers.py](src/server/handlers.py).

**Why.** Publishing one event per 1 MB chunk means 10,240 events for a 10 GB
upload, each one taking the monitor's lock. Every 8 MB is still smooth on screen
(the progress bar updates ~125 times for a 1 GB file) at an eighth of the cost.

---

### 12. Identity is a plain user name, with no password

**Decision.** The client passes `"user": "alice"` and the server believes it.

**Why.** The requirement is *file sharing policies* — who may access what — not
authentication. Adding password hashing and sessions would double the code
without addressing anything in the question.

**Stated limitation.** This is not secure against a malicious user, who could
simply claim to be alice. Real authentication (a password or a token checked on
every request) would be the first thing to add for production use. The policy
engine itself would not change: only the line that decides *who the caller is*.
