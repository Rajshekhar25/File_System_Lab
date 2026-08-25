# flow.md — how everything actually flows

This file walks through each operation step by step. Read it next to the code;
the file names in brackets tell you where to look.

---

## 0. The wire format

Every conversation is one TCP connection carrying:

```
{"op":"UPLOAD","user":"alice","file":"movie.iso","offset":0,"total":2147483648}\n
<raw file bytes, exactly (total - offset) of them>
```

One JSON line, then optional raw bytes. That's all.
[src/common/protocol.js](src/common/protocol.js)

The one subtle part is reading it back. A socket is a stream, so the first
chunk that arrives usually holds the header **and** the first slice of file
data. `readJsonLine()` takes the header and then calls `socket.unshift(rest)`
to push the leftover bytes back into the stream, so the next reader sees a
stream that begins exactly at the first byte of the body.

---

## 1. Starting up

```
balancer starts
   -> starts a health loop that PINGs every node every 2 s
   -> writes runtime/balancer.json (who is up, how busy)

node N starts
   -> creates Storage (storage/ folder) and PolicyStore (storage/policies.json)
   -> creates a Monitor, which subscribes to '*' on the event bus
   -> subscribes consoleLogger to '*' as well
   -> publishes "node.started"
   -> net.createServer(...).listen(port)

dashboard starts
   -> HTTP server; on every request it reads runtime/*.json and shows it
```

---

## 2. Upload (the important one)

```
CLIENT                          BALANCER                 NODE                 EVENT BUS
  |                                 |                      |                      |
  |-- {"op":"STAT","file":"x"} ---------------------------->|                     |
  |<-- {"uploaded": 302402289} ----------------------------|                      |
  |    (how much of an earlier attempt survived)            |                     |
  |                                 |                      |                      |
  |-- {"op":"WHERE"} -------------->|                      |                      |
  |<-- {"node":"node2",...} --------|                      |                      |
  |                                 |                      |                      |
  |-- {"op":"UPLOAD", offset:302402289, total:629145600} -->|                     |
  |                                                        |-- canWrite(user)?    |
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

* The client side is one stream pipeline —
  `fs.createReadStream(file) -> CountingStream -> socket` — so a 10 GB file
  uses about 1 MB of memory, not 10 GB, and Node applies backpressure
  automatically. [src/client/client.js](src/client/client.js) `_push`
* The server side cannot use `pipe`, because the connection must stay open
  after the body for the final JSON reply. `receiveBytes()` therefore counts to
  exactly `total - offset` and handles backpressure by hand: when
  `file.write()` returns `false` it pauses the socket and only resumes on
  `'drain'`. [src/common/protocol.js](src/common/protocol.js)
* The server writes to `x.part`, and only renames it to `x` after the last
  byte. So an interrupted upload can never look like a finished file.
* The permission check happens **before** a single byte is accepted.

---

## 3. Download

```
CLIENT                          BALANCER                 NODE
  |-- {"op":"WHERE"} -------------->|                      |
  |<-- {"node":"node1",...} --------|                      |
  |-- {"op":"DOWNLOAD","offset":0} ---------------------- >|
  |                                                        |-- canRead(user)?
  |<-- {"ok":true,"size":629145600} -----------------------|   (else access.denied)
  |<== 1 MB ==                                             |   download.started
  |<== 1 MB ==                                             |   download.progress
  |   writes into out.bin.part                             |
  |   when part size == size -> rename to out.bin          |   download.completed
```

Here the server *can* use a single pipeline, because it is only sending:

```js
await pipeline(storage.createReadStream(name, offset), counter, socket,
               { end: false });
```

`{ end: false }` keeps the socket open instead of closing it when the file ends.

The client writes into `out.bin.part` for exactly the same reason the server
does: a half-downloaded file must never be mistaken for the real thing.

---

## 4. Sharing policy

```
alice: node src/client/client.js alice share report.iso bob read

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

Every change is made inside `_exclusive()`, which solves **two** problems at
once — this is worth understanding properly, because the second one surprises
people:

1. **Three separate node processes** edit the same JSON file. A lock file
   opened with the `'wx'` flag — which only one process can create — serialises
   them. Without it, two nodes finishing an upload in the same millisecond
   would each save their own copy of the whole file and one rule would silently
   vanish.
2. **Inside one process**, `await` lets two connections interleave halfway
   through the read-modify-write. Being single-threaded does *not* save you
   here: the event loop can switch to another request at every `await`. A
   promise queue makes them line up.

[src/server/policy.js](src/server/policy.js) `_exclusive`

---

## 5. Abruptly terminated transfer

```
upload running ......... client is killed / Ctrl+C / cable pulled
        |
        v
socket emits 'end' (clean close) or an ECONNRESET error (hard kill)
receiveBytes() turns both into one PartialTransferError carrying `received`
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
client asks STAT -> "uploaded": 302402289
client sends UPLOAD with offset = 302402289
server truncates the .part file to exactly that length and carries on
```

Note the `endStream(file)` call on the error path: it flushes whatever was
still buffered before we read the `.part` size, so the resume point is exact.

The same idea covers a download: the client's `.part` file size is the resume
point.

---

## 6. Server failure and failover

```
balancer health loop, every 2 s:  node1 PING -> ok      node2 PING -> ok
                                  node3 PING -> ECONNREFUSED
                                  => node3.alive = false, printed "node3 is DOWN"
                                  => node3 is no longer offered to any client

client in the middle of a transfer with node3:
    the socket errors or closes early
        -> caught in the retry loop
        -> asks WHERE again  (gets node1, because node3 is now excluded)
        -> resumes from the byte count it already has
        -> finishes normally
```

The client retries up to 3 times with a 1.5 s pause.
[src/client/client.js](src/client/client.js) `upload` / `download`

One important distinction: a **network failure is retried**, but a **refusal is
not**. If the node answers "no read permission", asking a different node would
produce the same answer, so `RemoteRefused` is thrown straight out of the retry
loop instead of looping four times.

---

## 7. Load balancing

```
client: {"op":"WHERE"}

registry.pick():
    keep only nodes whose last ping succeeded
    sort by (active + pending), then by served
    chosen.pending += 1
    return chosen
```

* `active` = connections the node reported at the last ping (up to 2 s old).
* `pending` = clients the balancer has handed out **since** that ping. Without
  it, eight clients arriving in the same millisecond would all see the same
  stale "0 active" and pile onto one node — which is exactly what happened
  before this was added.
* `pending` is reset to 0 every time a ping brings the real number back.

[src/balancer/balancer.js](src/balancer/balancer.js) `NodeRegistry.pick`

---

## 8. Status visualization

```
handler publishes an event
        |
        v
Monitor.onEvent  (subscribed to '*')    -> updates counters + live transfer list
        |
        v
Monitor's 1-second interval             -> runtime/node_1.json   (written to a
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
benchmark.js --size 50 --clients 8
    creates benchdata/payload_50MB.bin once
    starts 8 concurrent clients, each: WHERE -> UPLOAD 50 MB
    Promise.all waits for them, records per-client seconds and MB/s
    repeats the same with DOWNLOAD
    prints the table, notes which node served each client,
    and appends everything to runtime/perf_report.txt
```

It calls `_push` / `_pull` directly instead of `upload()` / `download()` on
purpose: the retry-and-resume wrapper would hide a failure, and for a
measurement we want the raw single-attempt time.
