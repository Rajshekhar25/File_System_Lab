'use strict';
/**
 * Web dashboard: the "visualization of availability and status" requirement.
 *
 * It is intentionally a read-only viewer. It never talks to the nodes over the
 * network; it just reads the JSON status files that the monitors and the
 * balancer already write into runtime/. If every server process dies, the
 * dashboard still runs and simply shows everything as DOWN.
 *
 *     npm run dashboard      ->  http://127.0.0.1:8080
 */

const fsp = require('fs/promises');
const http = require('http');
const path = require('path');

const config = require('../common/config');
const { PolicyStore } = require('../server/policy');
const { Storage } = require('../server/storage');

const PAGE = path.join(__dirname, 'index.html');

// A node whose status file has not been touched for this long is called stale.
const STALE_AFTER = 4.0;

async function readJsonFile(file) {
  try {
    return JSON.parse(await fsp.readFile(file, 'utf8'));
  } catch {
    return null;
  }
}

/** Merge everything the components have written into one picture. */
async function collectStatus() {
  const now = Date.now() / 1000;

  // --- what each node says about itself
  const entries = await fsp.readdir(config.RUNTIME_DIR).catch(() => []);
  const nodes = [];
  for (const entry of entries.sort()) {
    if (!entry.startsWith('node_') || !entry.endsWith('.json')) continue;
    const data = await readJsonFile(path.join(config.RUNTIME_DIR, entry));
    if (data) {
      data.stale = (now - (data.updated || 0)) > STALE_AFTER;
      nodes.push(data);
    }
  }
  nodes.sort((a, b) => a.node.localeCompare(b.node));

  // --- what the balancer thinks of them
  const balancer = await readJsonFile(path.join(config.RUNTIME_DIR, 'balancer.json')) || {};
  const health = balancer.nodes || {};

  // A node counts as available only if the balancer's last ping succeeded.
  for (const node of nodes) {
    node.reachable = Boolean((health[node.node] || {}).alive);
  }

  // Nodes configured but never started still deserve a row in the table.
  const seen = new Set(nodes.map((n) => n.node));
  for (const [name, entry] of Object.entries(health).sort()) {
    if (seen.has(name)) continue;
    nodes.push({
      node: name, address: `${entry.host}:${entry.port}`,
      reachable: entry.alive, stale: true, uptime: 0,
      active_connections: 0, counters: {}, transfers: [], recent: [],
    });
  }

  // --- the files and who may see them
  const storage = new Storage();
  const policy = new PolicyStore();
  await policy.reload();
  const rules = policy.allRules();

  const files = (await storage.listFiles()).map((item) => {
    const rule = rules[item.name] || {};
    return {
      name: item.name,
      size: item.size,
      state: item.state,
      owner: rule.owner || '-',
      readers: rule.readers || [],
      writers: rule.writers || [],
      public: Boolean(rule.public),
    };
  });

  // --- cluster-wide totals and a merged event feed
  const totals = {};
  let events = [];
  for (const node of nodes) {
    for (const [key, value] of Object.entries(node.counters || {})) {
      totals[key] = (totals[key] || 0) + value;
    }
    events = events.concat((node.recent || []).map((e) => ({ ...e, node: node.node })));
  }
  events.sort((a, b) => b.ts - a.ts);

  return {
    now,
    balancer_alive: Boolean(balancer.updated) && (now - balancer.updated) < STALE_AFTER,
    nodes,
    files,
    totals,
    events: events.slice(0, 30),
  };
}

function respond(res, code, contentType, body) {
  res.writeHead(code, {
    'Content-Type': contentType,
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

async function handleRequest(req, res) {
  try {
    if (req.url.startsWith('/api/status')) {
      respond(res, 200, 'application/json', JSON.stringify(await collectStatus()));
    } else if (req.url === '/' || req.url === '/index.html') {
      respond(res, 200, 'text/html; charset=utf-8', await fsp.readFile(PAGE));
    } else {
      respond(res, 404, 'text/plain', 'not found');
    }
  } catch (err) {
    respond(res, 500, 'text/plain', `dashboard error: ${err.message}`);
  }
}

function main() {
  config.ensureDirs();

  const server = http.createServer(handleRequest);
  server.listen(config.DASHBOARD_PORT, '127.0.0.1', () => {
    console.log(`dashboard: http://127.0.0.1:${config.DASHBOARD_PORT}`);
  });

  server.on('error', (err) => {
    console.error('dashboard could not start:', err.message);
    process.exit(1);
  });

  const shutdown = () => {
    console.log('\ndashboard shutting down');
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1000).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) main();

module.exports = { collectStatus };
