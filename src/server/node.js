'use strict';
/**
 * A storage node: the server that actually moves the bytes.
 *
 * Concurrency model
 * -----------------
 * Node.js runs ONE thread with an event loop. It does not need a thread per
 * client, because it never blocks waiting for a disk or a socket: it registers
 * "tell me when this chunk is ready" and goes off to serve somebody else.
 * A single node therefore handles many simultaneous GB-sized transfers, and
 * the platform runs three such nodes side by side.
 *
 * See decision.md section 3 for the full argument, including the honest
 * limitation (one node cannot use more than one CPU core for its own work).
 *
 * Run three of them for the demo:
 *     npm run node1        (or: node src/server/node.js 1)
 *     npm run node2
 *     npm run node3
 */

const net = require('net');

const config = require('../common/config');
const {
  bus, WILDCARD, CLIENT_CONNECTED, CLIENT_DISCONNECTED, NODE_STARTED,
} = require('../common/events');
const {
  readJsonLine, sendJson, isDisconnect, PartialTransferError,
} = require('../common/protocol');
const { OPS } = require('./handlers');
const { Monitor } = require('./monitor');
const { PolicyStore } = require('./policy');
const { Storage } = require('./storage');

// Events that are too chatty to print on the console.
const QUIET_EVENTS = ['upload.progress', 'download.progress'];

/** Everything a handler needs, plus the live connection counters. */
class NodeContext {
  constructor(nodeId, storage, policy) {
    this.nodeId = nodeId;
    this.storage = storage;
    this.policy = policy;
    this._active = 0;
    this._served = 0;
  }

  beginRequest() {
    this._active += 1;
    this._served += 1;
  }

  endRequest() {
    this._active -= 1;
  }

  activeCount() {
    return this._active;
  }

  servedCount() {
    return this._served;
  }
}

/** A second bus listener, kept separate from the monitor on purpose. */
function consoleLogger(event) {
  if (QUIET_EVENTS.includes(event.type)) return;

  const stamp = new Date(event.ts * 1000).toLocaleTimeString();
  const details = Object.entries(event)
    .filter(([key, value]) => key !== 'type' && key !== 'ts' && value !== undefined)
    .map(([key, value]) => `${key}=${value}`)
    .join(' ');
  console.log(`[${stamp}] ${event.type.padEnd(20)} ${details}`);
}

/** Handle one client connection from its first byte to its last. */
async function handleConnection(ctx, socket) {
  const peer = `${socket.remoteAddress}:${socket.remotePort}`;
  let counted = false;

  // A socket with no 'error' listener throws and kills the process. A client
  // that is killed mid-upload sends a reset, so this is not a rare case.
  socket.on('error', () => {});

  try {
    const request = await readJsonLine(socket);
    if (!request) return; // peer closed without asking for anything

    const operation = String(request.op || '').toUpperCase();

    // Health pings are bookkeeping, not user load: they must not show up in
    // the number the balancer uses to compare nodes.
    if (operation !== 'PING') {
      ctx.beginRequest();
      counted = true;
      bus.publish(CLIENT_CONNECTED, { peer, node: ctx.nodeId });
    }

    const handler = OPS[operation];
    if (!handler) {
      sendJson(socket, { ok: false, error: `unknown operation: ${operation}` });
    } else {
      await handler(ctx, request, socket);
    }
  } catch (err) {
    // Abrupt client death: the handlers already published the abort event.
    if (!isDisconnect(err) && !(err instanceof PartialTransferError)) {
      console.error(`[${ctx.nodeId}] request failed:`, err.message);
    }
  } finally {
    if (counted) {
      ctx.endRequest();
      bus.publish(CLIENT_DISCONNECTED, { peer, node: ctx.nodeId });
    }
    socket.end();
  }
}

function main() {
  const index = Number(process.argv[2]) - 1;
  if (!Number.isInteger(index) || index < 0 || index >= config.NODES.length) {
    console.log(`usage: node src/server/node.js <node-number 1..${config.NODES.length}>`);
    process.exit(1);
  }

  config.ensureDirs();
  const { host, port } = config.NODES[index];
  const nodeId = `node${index + 1}`;

  const storage = new Storage();
  const policy = new PolicyStore();
  const ctx = new NodeContext(nodeId, storage, policy);

  const monitor = new Monitor(nodeId, `${host}:${port}`);
  monitor.attachActiveSource(() => ctx.activeCount());
  bus.subscribe(WILDCARD, consoleLogger);

  const server = net.createServer((socket) => handleConnection(ctx, socket));

  server.listen(port, host, () => {
    bus.publish(NODE_STARTED, { node: nodeId, address: `${host}:${port}` });
    console.log(`${nodeId} listening on ${host}:${port}  (storage: ${storage.root})`);
  });

  server.on('error', (err) => {
    console.error(`${nodeId} could not start:`, err.message);
    process.exit(1);
  });

  const shutdown = () => {
    console.log(`\n${nodeId} shutting down`);
    monitor.removeStatusFile();
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1000).unref(); // do not hang on open sockets
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) main();

module.exports = { NodeContext, handleConnection };
