'use strict';
/**
 * Single place for every setting used by the platform.
 *
 * Change a port or a folder here and every component picks it up, so a demo
 * never needs edits in more than one file.
 */

const fs = require('fs');
const path = require('path');

// Project root = two levels above this file (src/common/config.js)
const BASE_DIR = path.resolve(__dirname, '..', '..');

// The shared "local cloud" disk. Every storage node reads/writes here, which
// is what makes a node replaceable when it dies.
const STORAGE_DIR = path.join(BASE_DIR, 'storage');

// Live status files written by the nodes and the balancer, read by dashboard.
const RUNTIME_DIR = path.join(BASE_DIR, 'runtime');

// Access-control rules (who owns what, who may read/write what).
const POLICY_FILE = path.join(STORAGE_DIR, 'policies.json');

module.exports = {
  BASE_DIR,
  STORAGE_DIR,
  RUNTIME_DIR,
  POLICY_FILE,

  // Files move 1 MB at a time so a 10 GB file never sits in RAM.
  CHUNK_SIZE: 1024 * 1024,

  // Load balancer: the single address a client needs to know.
  BALANCER_HOST: '127.0.0.1',
  BALANCER_PORT: 9000,

  // The pool of storage nodes.
  NODES: [
    { host: '127.0.0.1', port: 9101 },
    { host: '127.0.0.1', port: 9102 },
    { host: '127.0.0.1', port: 9103 },
  ],

  // How often the balancer pings each node to see if it is still alive.
  HEALTH_INTERVAL: 2000,

  // Web dashboard.
  DASHBOARD_PORT: 8080,

  // Timeout for small control messages (not for the file body itself).
  SOCKET_TIMEOUT: 10000,

  /** Create the storage and runtime folders if they do not exist yet. */
  ensureDirs() {
    fs.mkdirSync(STORAGE_DIR, { recursive: true });
    fs.mkdirSync(RUNTIME_DIR, { recursive: true });
  },
};
