'use strict';
/**
 * Command line client.
 *
 * It knows only the balancer's address. For every job it asks "where should I
 * go?", talks to the node it is given, and if that node dies mid-transfer it
 * asks again and resumes from wherever the bytes stopped. That single retry
 * loop is what covers both "abruptly terminated transfer" and "server
 * failure".
 *
 * Usage
 * -----
 *     node src/client/client.js alice upload   bigfile.iso
 *     node src/client/client.js bob   download bigfile.iso saved.iso
 *     node src/client/client.js alice list
 *     node src/client/client.js alice share    bigfile.iso bob read
 *     node src/client/client.js alice public   bigfile.iso on
 *     node src/client/client.js alice nodes
 */

const fs = require('fs');
const fsp = require('fs/promises');
const net = require('net');
const path = require('path');
const { pipeline } = require('stream/promises');

const config = require('../common/config');
const {
  sendJson, readJsonLine, receiveBytes, CountingStream, humanSize,
} = require('../common/protocol');

const RETRIES = 3;          // how many times to re-dial after a failure
const RETRY_PAUSE = 1500;   // milliseconds to wait before re-dialling

// The benchmark flips this to hide per-chunk progress from 8 threads at once.
const options = { quiet: false };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Something went wrong; retrying on another node may help. */
class TransferError extends Error {}

/**
 * The node answered properly and said no (e.g. no permission).
 * Retrying would only repeat the same refusal, so it is never retried.
 */
class RemoteRefused extends TransferError {}

/** Open a TCP connection, with a timeout that applies only to connecting. */
function connect(host, port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port });
    socket.setTimeout(config.SOCKET_TIMEOUT);

    const onConnect = () => {
      socket.setTimeout(0); // a multi-GB body must not hit the control timeout
      socket.removeListener('error', onError);
      socket.removeListener('timeout', onTimeout);
      socket.on('error', () => {}); // handled per-operation from here on
      resolve(socket);
    };
    const onError = (err) => { socket.destroy(); reject(err); };
    const onTimeout = () => { socket.destroy(); reject(new Error('connection timed out')); };

    socket.once('connect', onConnect);
    socket.once('error', onError);
    socket.once('timeout', onTimeout);
  });
}

/** Close a write stream and wait until every byte really reached the disk. */
function endStream(stream) {
  return new Promise((resolve) => stream.end(resolve));
}

function showProgress(kind, name, done, total) {
  if (options.quiet || !total) return;
  const percent = ((done * 100) / total).toFixed(1).padStart(5);
  process.stdout.write(
    `\r  ${kind} ${name}  ${percent}%  (${humanSize(done)} / ${humanSize(total)})`,
  );
}

class Client {
  constructor(user) {
    this.user = user;
  }

  // ------------------------------------------------------------------ plumbing
  async _askBalancer(payload) {
    const socket = await connect(config.BALANCER_HOST, config.BALANCER_PORT);
    try {
      sendJson(socket, payload);
      return await readJsonLine(socket);
    } finally {
      socket.destroy();
    }
  }

  async pickNode() {
    const reply = await this._askBalancer({ op: 'WHERE' });
    if (!reply || !reply.ok) {
      throw new RemoteRefused((reply && reply.error) || 'balancer unreachable');
    }
    return reply;
  }

  nodeStatus() {
    return this._askBalancer({ op: 'STATUS' });
  }

  /** One request, one JSON answer, connection closed. */
  async _simpleRequest(payload) {
    const node = await this.pickNode();
    const socket = await connect(node.host, node.port);
    try {
      sendJson(socket, payload);
      return { reply: await readJsonLine(socket), node };
    } finally {
      socket.destroy();
    }
  }

  // -------------------------------------------------------------------- uploads
  async _remoteState(remoteName) {
    const { reply } = await this._simpleRequest({
      op: 'STAT', user: this.user, file: remoteName,
    });
    return reply || {};
  }

  async upload(localPath, remoteName = null) {
    const stat = await fsp.stat(localPath).catch(() => null);
    if (!stat || !stat.isFile()) {
      throw new TransferError(`no such local file: ${localPath}`);
    }

    const name = remoteName || path.basename(localPath);
    const total = stat.size;
    const started = Date.now();

    for (let attempt = 1; attempt <= RETRIES + 1; attempt += 1) {
      const state = await this._remoteState(name);
      const offset = Number(state.uploaded) || 0;
      if (offset) console.log(`resuming ${name} from ${humanSize(offset)}`);

      const node = await this.pickNode();
      console.log(`attempt ${attempt} -> ${node.node} (${node.host}:${node.port})`);

      try {
        await this._push(node, localPath, name, offset, total);
      } catch (err) {
        if (err instanceof RemoteRefused) throw err; // a "no" is final
        console.log(`  transfer interrupted: ${err.message}`);
        if (attempt > RETRIES) {
          throw new TransferError(`upload failed after ${attempt} attempts`);
        }
        await sleep(RETRY_PAUSE);
        continue;
      }

      const seconds = Math.max((Date.now() - started) / 1000, 1e-6);
      console.log(`uploaded ${name} (${humanSize(total)}) in ${seconds.toFixed(2)}s`
        + `  = ${(total / seconds / (1024 * 1024)).toFixed(2)} MB/s`);
      return true;
    }
    return false;
  }

  async _push(node, localPath, remoteName, offset, total) {
    const socket = await connect(node.host, node.port);
    try {
      sendJson(socket, {
        op: 'UPLOAD', user: this.user, file: remoteName, offset, total,
      });

      const reply = await readJsonLine(socket);
      if (!reply || !reply.ok) {
        throw new RemoteRefused((reply && reply.error) || 'node refused upload');
      }

      if (total > offset) {
        const counter = new CountingStream(
          (sent) => showProgress('upload', remoteName, sent, total), offset,
        );
        const reader = fs.createReadStream(localPath, {
          start: offset, highWaterMark: config.CHUNK_SIZE,
        });
        // Streams handle the backpressure: if the network is slower than the
        // disk, the file read is paused automatically.
        await pipeline(reader, counter, socket, { end: false });
        if (!options.quiet) process.stdout.write('\n');
      }

      const final = await readJsonLine(socket);
      if (!final || !final.ok) {
        throw new TransferError((final && final.error) || 'node did not confirm');
      }
    } finally {
      socket.destroy();
    }
  }

  // ------------------------------------------------------------------ downloads
  async download(remoteName, outPath = null) {
    const target = outPath || path.join('downloads', remoteName);
    await fsp.mkdir(path.dirname(path.resolve(target)), { recursive: true });
    const partPath = target + '.part';
    const started = Date.now();

    for (let attempt = 1; attempt <= RETRIES + 1; attempt += 1) {
      const partStat = await fsp.stat(partPath).catch(() => null);
      const offset = partStat ? partStat.size : 0;
      if (offset) console.log(`resuming download from ${humanSize(offset)}`);

      const node = await this.pickNode();
      console.log(`attempt ${attempt} -> ${node.node} (${node.host}:${node.port})`);

      let total;
      try {
        total = await this._pull(node, remoteName, partPath, offset);
      } catch (err) {
        if (err instanceof RemoteRefused) throw err; // a "no" is final
        console.log(`  transfer interrupted: ${err.message}`);
        if (attempt > RETRIES) {
          throw new TransferError(`download failed after ${attempt} attempts`);
        }
        await sleep(RETRY_PAUSE);
        continue;
      }

      await fsp.rm(target, { force: true });
      await fsp.rename(partPath, target);

      const seconds = Math.max((Date.now() - started) / 1000, 1e-6);
      console.log(`downloaded ${remoteName} (${humanSize(total)}) in ${seconds.toFixed(2)}s`
        + `  = ${(total / seconds / (1024 * 1024)).toFixed(2)} MB/s -> ${target}`);
      return true;
    }
    return false;
  }

  async _pull(node, remoteName, partPath, offset) {
    const socket = await connect(node.host, node.port);
    try {
      sendJson(socket, {
        op: 'DOWNLOAD', user: this.user, file: remoteName, offset,
      });

      const reply = await readJsonLine(socket);
      if (!reply || !reply.ok) {
        throw new RemoteRefused((reply && reply.error) || 'node refused download');
      }

      const total = Number(reply.size);
      if (total <= offset) return total; // nothing left to fetch

      let file;
      if (offset > 0) {
        await fsp.truncate(partPath, offset);
        file = fs.createWriteStream(partPath, { flags: 'r+', start: offset });
      } else {
        file = fs.createWriteStream(partPath, { flags: 'w' });
      }

      try {
        await receiveBytes(socket, file, total - offset, (received) => {
          showProgress('download', remoteName, offset + received, total);
        });
        await endStream(file);
        if (!options.quiet) process.stdout.write('\n');
      } catch (err) {
        await endStream(file); // keep what did arrive: that IS the resume point
        throw err;
      }

      return total;
    } finally {
      socket.destroy();
    }
  }

  // --------------------------------------------------------------- metadata ops
  async listFiles() {
    return this._simpleRequest({ op: 'LIST', user: this.user });
  }

  async share(filename, targetUser, permission) {
    const { reply } = await this._simpleRequest({
      op: 'SHARE', user: this.user, file: filename,
      target: targetUser, permission,
    });
    return reply || {};
  }

  async setPublic(filename, value) {
    const { reply } = await this._simpleRequest({
      op: 'PUBLIC', user: this.user, file: filename, value,
    });
    return reply || {};
  }
}

// --------------------------------------------------------------------- CLI glue
const USAGE = `usage:
  node src/client/client.js <user> upload   <local file> [name on server]
  node src/client/client.js <user> download <name on server> [local file]
  node src/client/client.js <user> list
  node src/client/client.js <user> share    <file> <other user> <read|write>
  node src/client/client.js <user> public   <file> <on|off>
  node src/client/client.js <user> nodes`;

async function main(argv) {
  const [user, rawCommand, ...args] = argv.slice(2);
  if (!user || !rawCommand) {
    console.log(USAGE);
    return 1;
  }

  const command = rawCommand.toLowerCase();
  const client = new Client(user);

  try {
    if (command === 'upload') {
      await client.upload(args[0], args[1] || null);
    } else if (command === 'download') {
      await client.download(args[0], args[1] || null);
    } else if (command === 'list') {
      const { reply, node } = await client.listFiles();
      const files = (reply && reply.files) || [];
      console.log(`files visible to ${user} (answered by ${node.node})`);
      if (files.length === 0) console.log('  (nothing shared with you yet)');
      for (const item of files) {
        console.log(`  ${item.name.padEnd(28)} ${humanSize(item.size).padStart(10)}`
          + `  ${item.state.padEnd(11)} owner=${String(item.owner).padEnd(8)}`
          + ` ${item.public ? 'public' : ''}`);
      }
    } else if (command === 'share') {
      const reply = await client.share(args[0], args[1], args[2] || 'read');
      console.log(reply.message || reply.error);
    } else if (command === 'public') {
      const value = !args[1] || ['on', 'true', 'yes', '1'].includes(args[1].toLowerCase());
      const reply = await client.setPublic(args[0], value);
      console.log(reply.message || reply.error);
    } else if (command === 'nodes') {
      const reply = await client.nodeStatus();
      for (const entry of Object.values((reply && reply.nodes) || {})) {
        console.log(`  ${entry.node.padEnd(7)} ${entry.host}:${String(entry.port).padEnd(6)}`
          + ` ${(entry.alive ? 'UP' : 'DOWN').padEnd(5)}`
          + ` active=${String(entry.active).padEnd(3)} served=${entry.served}`);
      }
    } else {
      console.log(`unknown command: ${command}`);
      return 1;
    }
  } catch (err) {
    console.log(`error: ${err.message}`);
    return 1;
  }
  return 0;
}

if (require.main === module) {
  main(process.argv).then((code) => { process.exitCode = code; });
}

module.exports = { Client, TransferError, RemoteRefused, options, connect, endStream };
