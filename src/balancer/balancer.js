'use strict';
/**
 * Load balancer + health checker: the one address every client knows.
 *
 * It does NOT relay file data (that would make it the bottleneck for GB-sized
 * transfers). It only answers one question:
 *
 *     client:   {"op": "WHERE"}
 *     balancer: {"ok": true, "host": "127.0.0.1", "port": 9102, "node": "node2"}
 *
 * and the client then talks to that node directly. This is "redirect" style
 * balancing, the same idea used by many real storage systems.
 *
 * Choice rule: among the nodes that answered the last health ping, pick the
 * one with the fewest active connections (least-connections). A dead node
 * simply stops being offered, which is how a server failure is survived.
 *
 * Run it with:
 *     npm run balancer        (or: node src/balancer/balancer.js)
 */

const fsp = require('fs/promises');
const net = require('net');
const path = require('path');

const config = require('../common/config');
const { sendJson, readJsonLine } = require('../common/protocol');

const PING_TIMEOUT_MS = 2000;

/** One health check. Never rejects: a dead node is a normal answer. */
function pingNode(entry) {
  return new Promise((resolve) => {
    const dead = { alive: false, active: 0, served: 0 };
    const socket = net.createConnection({ host: entry.host, port: entry.port });
    socket.setTimeout(PING_TIMEOUT_MS);

    const finish = (result) => {
      socket.destroy();
      resolve(result);
    };

    socket.on('error', () => finish(dead));
    socket.on('timeout', () => finish(dead));
    socket.on('connect', async () => {
      try {
        sendJson(socket, { op: 'PING' });
        const reply = await readJsonLine(socket);
        if (reply && reply.ok) {
          finish({ alive: true, active: reply.active || 0, served: reply.served || 0 });
        } else {
          finish(dead);
        }
      } catch {
        finish(dead);
      }
    });
  });
}

/** Live view of every storage node, refreshed by the health loop. */
class NodeRegistry {
  constructor(nodes) {
    this.state = new Map();
    nodes.forEach((entry, index) => {
      const nodeId = `node${index + 1}`;
      this.state.set(nodeId, {
        node: nodeId, host: entry.host, port: entry.port,
        alive: false, active: 0, served: 0,
        pending: 0, last_seen: 0, failures: 0,
      });
    });
  }

  snapshot() {
    return Object.fromEntries([...this.state.entries()].map(([k, v]) => [k, { ...v }]));
  }

  async checkAll() {
    await Promise.all([...this.state.values()].map(async (entry) => {
      const result = await pingNode(entry);
      const wasAlive = entry.alive;

      entry.alive = result.alive;
      entry.active = result.active;
      entry.served = result.served;

      if (result.alive) {
        entry.last_seen = Date.now() / 1000;
        entry.failures = 0;
        // The ping carries the real number, so forget the guesses we made
        // since the previous round.
        entry.pending = 0;
        if (!wasAlive) console.log(`[balancer] ${entry.node} is UP`);
      } else {
        entry.failures += 1;
        if (wasAlive) console.log(`[balancer] ${entry.node} is DOWN`);
      }
    }));

    await this.writeStatus();
  }

  startHealthLoop() {
    const tick = async () => {
      await this.checkAll();
      setTimeout(tick, config.HEALTH_INTERVAL).unref();
    };
    tick();
  }

  /**
   * Least-connections choice among the healthy nodes.
   *
   * `active` is only as fresh as the last health ping (2 s old at worst), so
   * ten clients arriving together would all be sent to the same node.
   * `pending` fixes that: it counts the clients we have just handed out and is
   * cleared the moment a real ping brings the true number back.
   */
  pick() {
    const healthy = [...this.state.values()].filter((entry) => entry.alive);
    if (healthy.length === 0) return null;

    healthy.sort((a, b) => (a.active + a.pending) - (b.active + b.pending)
      || a.served - b.served);

    const chosen = healthy[0];
    chosen.pending += 1;
    return { ...chosen };
  }

  async writeStatus() {
    const target = path.join(config.RUNTIME_DIR, 'balancer.json');
    const payload = { updated: Date.now() / 1000, nodes: this.snapshot() };
    try {
      await fsp.writeFile(target + '.tmp', JSON.stringify(payload), 'utf8');
      await fsp.rename(target + '.tmp', target);
    } catch {
      // The dashboard can live with a missed update.
    }
  }
}

async function handleConnection(registry, socket) {
  socket.on('error', () => {});

  try {
    const request = await readJsonLine(socket);
    if (!request) return;

    const operation = String(request.op || '').toUpperCase();

    if (operation === 'WHERE') {
      const chosen = registry.pick();
      sendJson(socket, chosen
        ? { ok: true, node: chosen.node, host: chosen.host, port: chosen.port }
        : { ok: false, error: 'no healthy node available' });
    } else if (operation === 'STATUS') {
      sendJson(socket, { ok: true, nodes: registry.snapshot() });
    } else {
      sendJson(socket, { ok: false, error: 'unknown operation' });
    }
  } catch {
    // A client that hangs up mid-question needs no reply.
  } finally {
    socket.end();
  }
}

function main() {
  config.ensureDirs();

  const registry = new NodeRegistry(config.NODES);
  registry.startHealthLoop();

  const server = net.createServer((socket) => handleConnection(registry, socket));

  server.listen(config.BALANCER_PORT, config.BALANCER_HOST, () => {
    console.log(`balancer listening on ${config.BALANCER_HOST}:${config.BALANCER_PORT}, `
      + `watching ${config.NODES.length} nodes`);
  });

  server.on('error', (err) => {
    console.error('balancer could not start:', err.message);
    process.exit(1);
  });

  const shutdown = () => {
    console.log('\nbalancer shutting down');
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1000).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) main();

module.exports = { NodeRegistry, pingNode };
