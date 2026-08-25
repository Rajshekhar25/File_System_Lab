'use strict';
/**
 * Disk layer of the local cloud.
 *
 * Two rules keep the whole "large files + crashes" problem simple:
 *
 *   1. An upload in progress is written to  <name>.part
 *   2. Only when the last byte arrives is it renamed to  <name>
 *
 * So a half-finished file can never be mistaken for a real one, and the size
 * of the .part file *is* the resume point. Nothing else has to be remembered.
 */

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

const config = require('../common/config');

class Storage {
  constructor(root = config.STORAGE_DIR) {
    this.root = root;
    fs.mkdirSync(this.root, { recursive: true });
    // One promise chain per file name so two connections never append at once.
    this._chains = new Map();
  }

  // ------------------------------------------------------------------ helpers
  /** Strip any path so a client cannot write outside the storage folder. */
  static safeName(name) {
    return path.basename(String(name || '').replace(/\\/g, '/'));
  }

  safeName(name) {
    return Storage.safeName(name);
  }

  finalPath(name) {
    return path.join(this.root, this.safeName(name));
  }

  partPath(name) {
    return this.finalPath(name) + '.part';
  }

  /**
   * Run `fn` with exclusive access to one file name.
   *
   * Node runs one thread, but `await` still lets two connections interleave
   * inside the same function. Chaining the promises makes them queue up.
   */
  withFileLock(name, fn) {
    const key = this.safeName(name);
    const previous = this._chains.get(key) || Promise.resolve();
    const run = previous.then(fn, fn); // run whatever happened before
    this._chains.set(key, run.catch(() => {})); // one failure must not block the rest
    return run;
  }

  // -------------------------------------------------------------------- reads
  async exists(name) {
    return fsp.access(this.finalPath(name)).then(() => true, () => false);
  }

  async size(name) {
    return fsp.stat(this.finalPath(name)).then((s) => s.size, () => 0);
  }

  /** How many bytes of an interrupted upload are already on disk. */
  async uploadedBytes(name) {
    return fsp.stat(this.partPath(name)).then((s) => s.size, () => 0);
  }

  /** Every complete file plus every partial one, for the dashboard. */
  async listFiles() {
    const entries = await fsp.readdir(this.root).catch(() => []);
    const files = [];

    for (const entry of entries.sort()) {
      if (entry === 'policies.json' || entry === 'policies.json.lock') continue;
      const stat = await fsp.stat(path.join(this.root, entry)).catch(() => null);
      if (!stat || !stat.isFile()) continue;

      if (entry.endsWith('.part')) {
        files.push({ name: entry.slice(0, -5), size: stat.size, state: 'incomplete' });
      } else {
        files.push({ name: entry, size: stat.size, state: 'available' });
      }
    }
    return files;
  }

  createReadStream(name, offset = 0) {
    return fs.createReadStream(this.finalPath(name), {
      start: offset,
      highWaterMark: config.CHUNK_SIZE,
    });
  }

  // ------------------------------------------------------------------- writes
  /** Open the .part file positioned at offset (0 restarts the upload). */
  async openForAppend(name, offset) {
    const target = this.partPath(name);

    if (offset > 0) {
      // Drop anything past the agreed resume point, then continue from there.
      await fsp.truncate(target, offset);
      return fs.createWriteStream(target, { flags: 'r+', start: offset });
    }
    return fs.createWriteStream(target, { flags: 'w' });
  }

  /** Promote a finished .part file to its real name. */
  async finalize(name) {
    const part = this.partPath(name);
    const final = this.finalPath(name);
    await fsp.rm(final, { force: true }); // overwrite; policy already allowed it
    await fsp.rename(part, final);
    const stat = await fsp.stat(final);
    return stat.size;
  }

  async delete(name) {
    await fsp.rm(this.finalPath(name), { force: true });
    await fsp.rm(this.partPath(name), { force: true });
  }
}

module.exports = { Storage };
