# flow.md — how everything actually flows

This file walks through each operation step by step. Read it next to the code;
the file names in brackets tell you where to look.

---

## 0. The wire format

Every conversation is one TCP connection carrying:

```
{"op": "UPLOAD", "user": "alice", "file": "movie.iso", "offset": 0, "total": 2147483648}\n
<raw file bytes, exactly (total - offset) of them>
```

One JSON line, then optional raw bytes. That's all.
[src/common/protocol.py](src/common/protocol.py)

---

## 1. Starting up

```
balancer starts
   -> starts a background thread that PINGs every node every 2 s
   -> writes runtime/balancer.json (who is up, how busy)

node N starts
   -> creates Storage (storage/ folder) and PolicyStore (storage/policies.json)
   -> creates a Monitor, which subscribes to '*' on the event bus
   -> subscribes console_logger to '*' as well
   -> publishes "node.started"
   -> listens on its port; each accepted connection gets its own thread

dashboard starts
   -> HTTP server; on every request it reads runtime/*.json and shows it
```

---

## 2. Upload (the important one)

```
CLIENT                          BALANCER                 NODE                 EVENT BUS
  |                                 |                      |                      |
  |-- {"op":"STAT","file":"x"} ---------------------------->|                     |
  |<-- {"uploaded": 187000000} -----------------------------|                     |
  |    (how much of an earlier attempt survived)            |                     |
  |                                 |                      |                      |
  |-- {"op":"WHERE"} -------------->|                      |                      |
  |<-- {"node":"node2",...} --------|                      |                      |
  |                                 |                      |                      |
  |-- {"op":"UPLOAD", offset:187000000, total:600000000} -->|                     |
  |                                                        |-- can_write(user)?   |
  |                                                        |   no  -> access.denied
  |<-- {"ok":true} ----------------------------------------|-- yes -> upload.started
  |                                                        |                      |
  |== 1 MB ==>                                             |  append to x.part    |
  |== 1 MB ==>                                             |  every 8 MB -------> upload.progress
  |== ...  ==>                                             |                      |
  |                                                        |  rename x.part -> x  |
  |                                                        |  policy.claim(owner) |
  |<-- {"ok":true,"status":"complete"} --------------------|--------------------> upload.completed
```

Key points to explain:

* The file is read and sent **1 MB at a time**, so a 10 GB file uses 1 MB of
  memory, not 10 GB. [src/client/client.py](src/client/client.py) `_push`
* The server writes to `x.part`, and only renames it to `x` after the last
  byte. So an interrupted upload can never look like a finished file.
  [src/server/storage.py](src/server/storage.py)
* The permission check happens **before** a single byte is accepted.

---

## 3. Download

```
CLIENT                          BALANCER                 NODE
  |-- {"op":"WHERE"} -------------->|                      |
  |<-- {"node":"node1",...} --------|                      |
  |-- {"op":"DOWNLOAD","offset":0} ---------------------- >|
  |                                                        |-- can_read(user)?
  |<-- {"ok":true,"size":600000000} -----------------------|   (else access.denied)
  |<== 1 MB ==                                             |   download.started
  |<== 1 MB ==                                             |   download.progress
  |   writes into out.bin.part                             |
  |   when part size == size -> rename to out.bin          |   download.completed
```

The client writes into `out.bin.part` for exactly the same reason the server
does: a half-downloaded file must never be mistaken for the real thing.

---

## 4. Sharing policy

```
alice: python -m src.client.client alice share report.iso bob read

client -> node: {"op":"SHARE","user":"alice","file":"report.iso",
                 "target":"bob","permission":"read"}

node:  reload policies.json (another node may have changed it)
       is alice the owner?   no  -> {"ok":false,"error":"only the owner can share"}
                             yes -> add "bob" to readers, save, publish policy.changed
```

The rules live in `storage/policies.json`:

```json
{
  "report.iso": { "owner": "alice", "readers": ["bob"], "writers": [], "public": false }
}
```

Decision table used by the server:

| Action | Allowed when |
|---|---|
| upload a **new** file | always (the uploader becomes the owner) |
| overwrite an existing file | user is the owner, or is in `writers` |
| download | user is the owner, or is in `readers`, or the file is `public` |
| share / make public | user is the owner |
| see it in `list` | user is owner / reader / writer, or the file is public |

Because three separate node processes edit the same JSON file, every change is
made inside a **lock file** (`policies.json.lock`, created with `O_EXCL` so only
one process can win). Without it, two nodes finishing an upload at the same
millisecond would each save their own copy and one rule would silently vanish.
[src/server/policy.py](src/server/policy.py) `_exclusive`

---

## 5. Abruptly terminated transfer

```
upload running ......... client is killed / Ctrl+C / cable pulled
        |
        v
server's stream.read() returns b"" (clean close) or raises ConnectionReset (kill)
        |
        v
publish "upload.aborted" with resume_at = bytes already written
        |
        v
x.part is LEFT ON DISK on purpose. x is never created.
        |
   ... user runs the same upload command again ...
        |
        v
client asks STAT -> "uploaded": 402653184
client sends UPLOAD with offset = 402653184
server seeks there, truncates anything past it, and carries on
```

The same idea covers a download: the client's `.part` file size is the resume
point.

---

## 6. Server failure and failover

```
balancer thread, every 2 s:      node1 PING -> ok      node2 PING -> ok
                                 node3 PING -> connection refused
                                 => node3.alive = False, printed "node3 is DOWN"
                                 => node3 is no longer offered to any client

client in the middle of a transfer with node3:
    read fails / connection closed early
        -> caught in the retry loop
        -> asks WHERE again  (gets node1, because node3 is now excluded)
        -> resumes from the byte count it already has
        -> finishes normally
```

The client retries up to 3 times with a 1.5 s pause.
[src/client/client.py](src/client/client.py) `upload` / `download`

One important distinction: a **network failure is retried**, but a **refusal is
not**. If the node answers "no read permission", asking a different node would
produce the same answer, so `RemoteRefused` is raised immediately instead of
looping four times.

---

## 7. Load balancing

```
client: {"op":"WHERE"}

balancer.pick():
    keep only nodes whose last ping succeeded
    sort by (active + pending, served)
    chosen.pending += 1
    return chosen
```

* `active` = connections the node reported at the last ping (up to 2 s old).
* `pending` = clients the balancer has handed out **since** that ping. Without
  it, eight clients arriving in the same millisecond would all see the same
  stale "0 active" and pile onto one node — which is exactly what happened
  before this was added.
* `pending` is reset to 0 every time a ping brings the real number back.

[src/balancer/balancer.py](src/balancer/balancer.py) `NodeRegistry.pick`

---

## 8. Status visualization

```
handler publishes an event
        |
        v
Monitor.on_event  (subscribed to '*')   -> updates counters + live transfer list
        |
        v
Monitor's writer thread, once a second  -> runtime/node_1.json   (written to a
                                            .tmp file then renamed, so the
                                            dashboard never reads half a file)
        |
        v
dashboard /api/status : merges runtime/node_*.json + runtime/balancer.json
                        + the file list + policies.json
        |
        v
index.html polls /api/status every 1.5 s and redraws the tables
```

A node is shown **UP** only if the balancer's last ping succeeded *and* its
status file is fresh (updated within 4 s). A killed node therefore turns red on
its own, without anybody telling the dashboard.

---

## 9. Performance analysis run

```
benchmark.py --size 50 --threads 8
    creates benchdata/payload_50MB.bin once
    starts 8 threads, each: WHERE -> UPLOAD 50 MB
    joins them, records per-thread seconds and MB/s
    repeats the same with DOWNLOAD
    prints the table, notes which node served each thread,
    and appends everything to runtime/perf_report.txt
```

It calls `_push` / `_pull` directly instead of `upload()` / `download()` on
purpose: the retry-and-resume wrapper would hide a failure, and for a
measurement we want the raw single-attempt time.
