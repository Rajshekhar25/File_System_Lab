'use strict';
/**
 * The event bus - the heart of the event-driven architecture.
 *
 * In Node this is almost free: EventEmitter is built into the language runtime
 * and is the same mechanism sockets, streams and the HTTP server already use.
 * We only add two conveniences on top of it:
 *
 *   publish()   stamps every event with a type and a timestamp
 *   '*'         a wildcard channel, so a listener can watch everything
 *
 * Nothing in the server calls the monitor or the logger directly. A component
 * only announces "this happened" and whoever cares has subscribed in advance.
 * Adding a listener never means changing the code that emits.
 */

const { EventEmitter } = require('events');

const WILDCARD = '*';

class EventBus extends EventEmitter {
  /** Announce that something happened. Returns the event that was sent. */
  publish(type, data = {}) {
    const event = { type, ts: Date.now() / 1000, ...data };
    this.emit(type, event);
    this.emit(WILDCARD, event);
    return event;
  }

  /** Register handler(event) for one event type, or '*' for all of them. */
  subscribe(type, handler) {
    this.on(type, handler);
  }
}

// Every component inside one process shares this single bus.
const bus = new EventBus();

// A busy node has several listeners on '*'; raise Node's warning threshold.
bus.setMaxListeners(50);

// One broken listener must never take the server down.
bus.on('error', (err) => console.error('[event-bus] listener failed:', err));

module.exports = {
  bus,
  WILDCARD,

  // --- Event names used across the project (kept in one place for reference)
  CLIENT_CONNECTED: 'client.connected',
  CLIENT_DISCONNECTED: 'client.disconnected',
  ACCESS_DENIED: 'access.denied',

  UPLOAD_STARTED: 'upload.started',
  UPLOAD_PROGRESS: 'upload.progress',
  UPLOAD_COMPLETED: 'upload.completed',
  UPLOAD_ABORTED: 'upload.aborted',

  DOWNLOAD_STARTED: 'download.started',
  DOWNLOAD_PROGRESS: 'download.progress',
  DOWNLOAD_COMPLETED: 'download.completed',
  DOWNLOAD_ABORTED: 'download.aborted',

  POLICY_CHANGED: 'policy.changed',
  NODE_STARTED: 'node.started',
};
