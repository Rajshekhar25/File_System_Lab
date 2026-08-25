'use strict';
/**
 * The listener that turns raw events into the numbers you see on screen.
 *
 * The monitor subscribes to '*' on the event bus, so it sees every single
 * thing that happens inside a node without any handler knowing it exists. It
 * keeps the picture in memory and dumps it to runtime/node_<id>.json once a
 * second; the dashboard just reads those files.
 */

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

const config = require('../common/config');
const { bus, WILDCARD } = require('../common/events');

const MAX_RECENT_EVENTS = 40;
const WRITE_INTERVAL_MS = 1000;

class Monitor {
  constructor(nodeId, address) {
    this.nodeId = nodeId;
    this.address = address;
    this.startedAt = Date.now() / 1000;

    this.recent = [];
    this.transfers = new Map(); // key -> live transfer description
    this.counters = {
      uploads_started: 0, uploads_completed: 0, uploads_aborted: 0,
      downloads_started: 0, downloads_completed: 0, downloads_aborted: 0,
      access_denied: 0, connections: 0,
      bytes_in: 0, bytes_out: 0,
    };

    this.activeSource = () => 0;
    this.statusPath = path.join(config.RUNTIME_DIR, `node_${nodeId}.json`);
    fs.mkdirSync(config.RUNTIME_DIR, { recursive: true });

    bus.subscribe(WILDCARD, (event) => this.onEvent(event));

    this._writing = false;
    this._timer = setInterval(() => this.writeStatus(), WRITE_INTERVAL_MS);
    this._timer.unref(); // never keep the process alive just for the timer
  }

  /** Let the node tell us how many client connections are busy right now. */
  attachActiveSource(fn) {
    this.activeSource = fn;
  }

  // ----------------------------------------------------------- event listener
  onEvent(event) {
    const kind = event.type;
    const key = `${event.user}:${event.file}:${kind.split('.')[0]}`;

    this.recent.push({ ts: event.ts, type: kind, user: event.user, file: event.file });
    if (this.recent.length > MAX_RECENT_EVENTS) this.recent.shift();

    switch (kind) {
      case 'client.connected':
        this.counters.connections += 1;
        break;
      case 'access.denied':
        this.counters.access_denied += 1;
        break;

      case 'upload.started':
        this.counters.uploads_started += 1;
        this.transfers.set(key, {
          kind: 'upload', user: event.user, file: event.file,
          done: event.offset || 0, total: event.total || 0,
        });
        break;
      case 'upload.progress':
        if (this.transfers.has(key)) this.transfers.get(key).done = event.done || 0;
        break;
      case 'upload.completed':
        this.counters.uploads_completed += 1;
        this.counters.bytes_in += event.size || 0;
        this.transfers.delete(key);
        break;
      case 'upload.aborted':
        this.counters.uploads_aborted += 1;
        this.transfers.delete(key);
        break;

      case 'download.started':
        this.counters.downloads_started += 1;
        this.transfers.set(key, {
          kind: 'download', user: event.user, file: event.file,
          done: event.offset || 0, total: event.total || 0,
        });
        break;
      case 'download.progress':
        if (this.transfers.has(key)) this.transfers.get(key).done = event.done || 0;
        break;
      case 'download.completed':
        this.counters.downloads_completed += 1;
        this.counters.bytes_out += event.size || 0;
        this.transfers.delete(key);
        break;
      case 'download.aborted':
        this.counters.downloads_aborted += 1;
        this.transfers.delete(key);
        break;

      default:
        break; // node.started, policy.changed, client.disconnected: only logged
    }
  }

  // -------------------------------------------------------------- publishing
  snapshot() {
    return {
      node: this.nodeId,
      address: this.address,
      alive: true,
      uptime: Number((Date.now() / 1000 - this.startedAt).toFixed(1)),
      updated: Date.now() / 1000,
      active_connections: this.activeSource(),
      counters: { ...this.counters },
      transfers: [...this.transfers.values()],
      recent: this.recent.slice(-MAX_RECENT_EVENTS),
    };
  }

  async writeStatus() {
    if (this._writing) return; // a slow disk must not queue up writes
    this._writing = true;
    const temp = this.statusPath + '.tmp';
    try {
      await fsp.writeFile(temp, JSON.stringify(this.snapshot()), 'utf8');
      // Atomic rename: a reader never sees half a file.
      await fsp.rename(temp, this.statusPath);
    } catch {
      // The dashboard can live with a missed update.
    } finally {
      this._writing = false;
    }
  }

  /** Called on a clean shutdown so the dashboard stops showing this node. */
  removeStatusFile() {
    clearInterval(this._timer);
    try {
      fs.rmSync(this.statusPath, { force: true });
    } catch {
      // nothing useful to do while exiting
    }
  }
}

module.exports = { Monitor };
