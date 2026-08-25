'use strict';
/**
 * The wire format spoken between client, balancer and storage nodes.
 *
 * Deliberately minimal so it can be explained in one line:
 *
 *     one JSON header terminated by '\n', optionally followed by raw file bytes.
 *
 * Example upload request:
 *     {"op":"UPLOAD","user":"alice","file":"movie.iso","offset":0,"total":2147483648}\n
 *     <2147483648 raw bytes>
 *
 * Keeping the payload raw (instead of base64 or JSON) is what allows multi-GB
 * files to stream at disk speed.
 */

const { Transform } = require('stream');

const NEWLINE = 0x0a;
const MAX_HEADER = 64 * 1024; // a JSON header is never anywhere near this big

/** Raised when a transfer stopped early; carries how much did arrive. */
class PartialTransferError extends Error {
  constructor(received) {
    super(`connection closed after ${received} bytes`);
    this.name = 'PartialTransferError';
    this.received = received;
  }
}

/** Send one JSON message as a single newline-terminated line. */
function sendJson(socket, obj) {
  socket.write(JSON.stringify(obj) + '\n');
}

/**
 * Read exactly one JSON header line from a socket.
 *
 * The interesting part is the last step. A socket is a stream, so the first
 * chunk that arrives usually contains the header AND the first slice of file
 * data. We take the header, then `unshift` the leftover bytes back into the
 * stream so the next reader (the file writer, or a pipe) sees a stream that
 * starts exactly at the first byte of the body.
 *
 * Resolves to null if the peer closed without sending anything - which is what
 * an abruptly killed client looks like.
 */
function readJsonLine(socket) {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);

    const cleanup = () => {
      socket.removeListener('data', onData);
      socket.removeListener('end', onEnd);
      socket.removeListener('error', onError);
    };

    const onData = (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      const at = buffer.indexOf(NEWLINE);

      if (at === -1) {
        if (buffer.length > MAX_HEADER) {
          cleanup();
          reject(new Error('header too long'));
        }
        return; // header not complete yet, wait for more
      }

      cleanup();
      socket.pause();

      const line = buffer.subarray(0, at).toString('utf8');
      const rest = buffer.subarray(at + 1);
      if (rest.length > 0) socket.unshift(rest); // give the body back

      try {
        resolve(JSON.parse(line));
      } catch (err) {
        reject(new Error('bad JSON header: ' + err.message));
      }
    };

    const onEnd = () => { cleanup(); resolve(null); };
    const onError = (err) => { cleanup(); reject(err); };

    socket.on('data', onData);
    socket.on('end', onEnd);
    socket.on('error', onError);
    socket.resume();
  });
}

/**
 * Read exactly `total` bytes from the socket into a writable stream.
 *
 * We cannot simply `socket.pipe(file)` here: the connection stays open after
 * the body so the two sides can still exchange a final JSON reply, so we have
 * to stop at exactly the right byte.
 *
 * Backpressure is handled by hand and is worth pointing at: when the disk says
 * "my buffer is full" (write() returns false) we pause the socket, and we only
 * resume once the file stream emits 'drain'. Without this, a fast network and
 * a slow disk would grow an unbounded buffer in memory - the exact thing that
 * kills a naive implementation on a multi-GB file.
 */
function receiveBytes(socket, writeStream, total, onProgress) {
  return new Promise((resolve, reject) => {
    let received = 0;

    const cleanup = () => {
      socket.removeListener('data', onData);
      socket.removeListener('end', onEnd);
      socket.removeListener('error', onError);
      writeStream.removeListener('drain', onDrain);
    };

    const onData = (chunk) => {
      let piece = chunk;

      // Never swallow more than we were promised; hand any extra back.
      if (received + piece.length > total) {
        const keep = total - received;
        socket.pause();
        socket.unshift(piece.subarray(keep));
        piece = piece.subarray(0, keep);
      }

      received += piece.length;
      const roomLeft = writeStream.write(piece);
      if (!roomLeft) socket.pause(); // disk is behind: stop reading

      if (onProgress) onProgress(received);

      if (received >= total) {
        cleanup();
        resolve(received);
      }
    };

    const onDrain = () => socket.resume(); // disk caught up: read again
    const onEnd = () => { cleanup(); reject(new PartialTransferError(received)); };
    const onError = (err) => {
      cleanup();
      // A reset from a killed peer means the same thing as a clean close.
      reject(isDisconnect(err) ? new PartialTransferError(received) : err);
    };

    socket.on('data', onData);
    socket.on('end', onEnd);
    socket.on('error', onError);
    writeStream.on('drain', onDrain);
    socket.resume();
  });
}

/**
 * A pass-through stream that counts the bytes flowing through it.
 *
 * Lets the download path stay a single pipeline - read file -> count -> socket -
 * while still reporting progress. Backpressure is preserved along the chain.
 */
class CountingStream extends Transform {
  constructor(onProgress, startAt = 0) {
    super();
    this.count = startAt;
    this.onProgress = onProgress;
  }

  _transform(chunk, _encoding, done) {
    this.count += chunk.length;
    if (this.onProgress) this.onProgress(this.count);
    done(null, chunk);
  }
}

/** True for the errors that just mean "the other side went away". */
function isDisconnect(err) {
  return err && ['ECONNRESET', 'EPIPE', 'ECONNABORTED', 'ERR_STREAM_PREMATURE_CLOSE']
    .includes(err.code);
}

/** Format a byte count for logs and the dashboard. */
function humanSize(bytes) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = Number(bytes) || 0;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(1)} ${units[unit]}`;
}

module.exports = {
  sendJson,
  readJsonLine,
  receiveBytes,
  CountingStream,
  PartialTransferError,
  isDisconnect,
  humanSize,
};
