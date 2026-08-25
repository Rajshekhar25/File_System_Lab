'use strict';
/**
 * One handler function per operation, plus the table that maps them.
 *
 * This is where the event-driven style shows up in day to day code: a handler
 * never prints, never updates a counter and never writes a log file. It only
 * publishes events ("upload.started", "upload.aborted", ...) and the monitor,
 * which subscribed to the bus, does the rest.
 *
 * Adding a new operation = write a function and add one line to OPS.
 */

const { pipeline } = require('stream/promises');

const config = require('../common/config');
const {
  bus, ACCESS_DENIED,
  UPLOAD_STARTED, UPLOAD_PROGRESS, UPLOAD_COMPLETED, UPLOAD_ABORTED,
  DOWNLOAD_STARTED, DOWNLOAD_PROGRESS, DOWNLOAD_COMPLETED, DOWNLOAD_ABORTED,
} = require('../common/events');
const {
  sendJson, receiveBytes, CountingStream, PartialTransferError, isDisconnect,
} = require('../common/protocol');

// Publish a progress event at most this often (every 8 MB) so a 10 GB upload
// does not flood the bus with thousands of tiny updates.
const PROGRESS_EVERY = 8 * config.CHUNK_SIZE;

function deny(ctx, socket, user, file, operation, reason) {
  bus.publish(ACCESS_DENIED, { user, file, operation, reason, node: ctx.nodeId });
  sendJson(socket, { ok: false, error: reason });
}

/** Close a write stream and wait until every byte really reached the disk. */
function endStream(stream) {
  return new Promise((resolve) => stream.end(resolve));
}

// ----------------------------------------------------------------------- upload
async function handleUpload(ctx, req, socket) {
  const user = req.user || 'anonymous';
  const name = ctx.storage.safeName(req.file);
  const total = Number(req.total) || 0;
  const offset = Number(req.offset) || 0;

  await ctx.policy.reload();
  if (!ctx.policy.canWrite(user, name)) {
    return deny(ctx, socket, user, name, 'upload', `no write permission on ${name}`);
  }

  // The client must resume exactly where the .part file ends.
  const onDisk = await ctx.storage.uploadedBytes(name);
  if (offset > onDisk) {
    return sendJson(socket, { ok: false, error: 'bad offset', resume_at: onDisk });
  }

  sendJson(socket, { ok: true, offset, node: ctx.nodeId });
  bus.publish(UPLOAD_STARTED, { user, file: name, offset, total, node: ctx.nodeId });

  // Serialised per file name: two uploads of the same name never interleave.
  await ctx.storage.withFileLock(name, async () => {
    const file = await ctx.storage.openForAppend(name, offset);
    let nextReport = offset + PROGRESS_EVERY;

    try {
      await receiveBytes(socket, file, total - offset, (received) => {
        const done = offset + received;
        if (done >= nextReport) {
          bus.publish(UPLOAD_PROGRESS, { user, file: name, done, total, node: ctx.nodeId });
          nextReport = done + PROGRESS_EVERY;
        }
      });
      await endStream(file);
    } catch (err) {
      await endStream(file); // flush what did arrive: that IS the resume point

      if (err instanceof PartialTransferError || isDisconnect(err)) {
        // Client vanished (Ctrl+C, network drop, killed process). The .part
        // file stays on disk, so the next attempt resumes from here.
        const received = offset + (err.received || 0);
        bus.publish(UPLOAD_ABORTED, {
          user, file: name, received, total, resume_at: received, node: ctx.nodeId,
        });
        return;
      }
      throw err;
    }

    const size = await ctx.storage.finalize(name);
    await ctx.policy.claim(user, name);
    bus.publish(UPLOAD_COMPLETED, { user, file: name, size, node: ctx.nodeId });
    sendJson(socket, { ok: true, status: 'complete', size });
  });
}

// --------------------------------------------------------------------- download
async function handleDownload(ctx, req, socket) {
  const user = req.user || 'anonymous';
  const name = ctx.storage.safeName(req.file);
  const offset = Number(req.offset) || 0;

  await ctx.policy.reload();
  if (!(await ctx.storage.exists(name))) {
    return sendJson(socket, { ok: false, error: 'file not found' });
  }
  if (!ctx.policy.canRead(user, name)) {
    return deny(ctx, socket, user, name, 'download', `no read permission on ${name}`);
  }

  const size = await ctx.storage.size(name);
  sendJson(socket, { ok: true, size, offset, node: ctx.nodeId });
  bus.publish(DOWNLOAD_STARTED, { user, file: name, offset, total: size, node: ctx.nodeId });

  let nextReport = offset + PROGRESS_EVERY;
  const counter = new CountingStream((done) => {
    if (done >= nextReport) {
      bus.publish(DOWNLOAD_PROGRESS, { user, file: name, done, total: size, node: ctx.nodeId });
      nextReport = done + PROGRESS_EVERY;
    }
  }, offset);

  try {
    // One pipeline, and Node handles the backpressure for us: if the receiver
    // is slow, the file stream is paused automatically.
    // `end: false` keeps the socket open so we could still talk afterwards.
    await pipeline(ctx.storage.createReadStream(name, offset), counter, socket,
      { end: false });
  } catch (err) {
    if (isDisconnect(err)) {
      // Receiver disappeared half way; it can resume from counter.count.
      bus.publish(DOWNLOAD_ABORTED, {
        user, file: name, sent: counter.count, total: size, node: ctx.nodeId,
      });
      return;
    }
    throw err;
  }

  bus.publish(DOWNLOAD_COMPLETED, { user, file: name, size, node: ctx.nodeId });
}

// ------------------------------------------------------------- small metadata ops
async function handleList(ctx, req, socket) {
  const user = req.user || 'anonymous';
  await ctx.policy.reload();

  const allowed = new Set(ctx.policy.visibleTo(user));
  const rules = ctx.policy.allRules();
  const all = await ctx.storage.listFiles();

  const files = all
    .filter((entry) => allowed.has(entry.name))
    .map((entry) => ({
      ...entry,
      owner: (rules[entry.name] || {}).owner || '-',
      public: Boolean((rules[entry.name] || {}).public),
    }));

  sendJson(socket, { ok: true, files, node: ctx.nodeId });
}

/** Tell the client how much of an interrupted upload already landed. */
async function handleStat(ctx, req, socket) {
  const name = ctx.storage.safeName(req.file);
  sendJson(socket, {
    ok: true,
    file: name,
    complete: await ctx.storage.exists(name),
    size: await ctx.storage.size(name),
    uploaded: await ctx.storage.uploadedBytes(name),
    node: ctx.nodeId,
  });
}

async function handleShare(ctx, req, socket) {
  await ctx.policy.reload();
  const result = await ctx.policy.share(
    req.user, ctx.storage.safeName(req.file), req.target, req.permission || 'read',
  );
  sendJson(socket, result.ok
    ? { ok: true, message: result.message }
    : { ok: false, error: result.message });
}

async function handlePublic(ctx, req, socket) {
  await ctx.policy.reload();
  const result = await ctx.policy.setPublic(
    req.user, ctx.storage.safeName(req.file), Boolean(req.value),
  );
  sendJson(socket, result.ok
    ? { ok: true, message: result.message }
    : { ok: false, error: result.message });
}

/** Health check used by the load balancer; also carries the load metric. */
async function handlePing(ctx, req, socket) {
  sendJson(socket, {
    ok: true, node: ctx.nodeId, active: ctx.activeCount(), served: ctx.servedCount(),
  });
}

// The dispatch table. The node looks the operation up here; nothing else.
const OPS = {
  UPLOAD: handleUpload,
  DOWNLOAD: handleDownload,
  LIST: handleList,
  STAT: handleStat,
  SHARE: handleShare,
  PUBLIC: handlePublic,
  PING: handlePing,
};

module.exports = { OPS, PROGRESS_EVERY };
