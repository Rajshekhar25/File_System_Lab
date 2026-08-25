'use strict';
/**
 * File sharing policies: who may read, write or share which file.
 *
 * Stored as one small JSON file so anyone can open it and read the rules:
 *
 *     {
 *       "report.pdf": {
 *         "owner":   "alice",
 *         "readers": ["bob"],
 *         "writers": [],
 *         "public":  false
 *       }
 *     }
 *
 * Rules, in plain English:
 *   - whoever uploads a file first becomes its owner;
 *   - the owner may always read, overwrite and share it;
 *   - a reader may download it, a writer may overwrite it;
 *   - a public file may be downloaded by anyone;
 *   - a file with no entry yet is free for anyone to claim.
 */

const fsp = require('fs/promises');
const path = require('path');

const config = require('../common/config');
const { bus, POLICY_CHANGED } = require('../common/events');

const READ = 'read';
const WRITE = 'write';

const LOCK_RETRY_MS = 10;
const LOCK_TIMEOUT_MS = 5000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class PolicyStore {
  constructor(file = config.POLICY_FILE) {
    this.path = file;
    this.lockPath = file + '.lock';
    this._rules = {};
    this._queue = Promise.resolve(); // in-process serialisation
  }

  // -------------------------------------------------------------- persistence
  async _load() {
    try {
      this._rules = JSON.parse(await fsp.readFile(this.path, 'utf8'));
    } catch {
      this._rules = {}; // missing or corrupt: start empty
    }
  }

  async _save() {
    await fsp.mkdir(path.dirname(this.path), { recursive: true });
    await fsp.writeFile(this.path, JSON.stringify(this._rules, null, 2), 'utf8');
  }

  /** Re-read the rules from disk (another node may have changed them). */
  async reload() {
    await this._load();
  }

  /**
   * Guard a read-modify-write of the shared policy file.
   *
   * Two different problems have to be solved at once:
   *
   *  1. Three node PROCESSES all edit the same policies.json. Without a guard,
   *     two nodes finishing an upload at the same moment would each save their
   *     own copy of the whole file and one rule would silently vanish. A lock
   *     file opened with the 'wx' flag - which only one process can create -
   *     serialises them.
   *
   *  2. Inside ONE process, `await` lets two connections interleave halfway
   *     through the read-modify-write. Being single-threaded does not save us
   *     here. The promise queue makes them line up.
   */
  async _exclusive(mutate) {
    const run = this._queue.then(async () => {
      const handle = await this._acquireLockFile();
      try {
        await this._load(); // pick up anything another node just wrote
        const result = await mutate();
        await this._save();
        return result;
      } finally {
        await handle.close();
        await fsp.rm(this.lockPath, { force: true });
      }
    });

    this._queue = run.catch(() => {}); // one failure must not block the rest
    return run;
  }

  async _acquireLockFile() {
    const deadline = Date.now() + LOCK_TIMEOUT_MS;
    for (;;) {
      try {
        return await fsp.open(this.lockPath, 'wx'); // fails if it already exists
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
        if (Date.now() > deadline) {
          await fsp.rm(this.lockPath, { force: true }); // a crashed node left it
          continue;
        }
        await sleep(LOCK_RETRY_MS);
      }
    }
  }

  allRules() {
    return JSON.parse(JSON.stringify(this._rules)); // plain copy
  }

  // ------------------------------------------------------------------ queries
  ownerOf(filename) {
    const rule = this._rules[filename];
    return rule ? rule.owner : null;
  }

  canRead(user, filename) {
    const rule = this._rules[filename];
    if (!rule) return false; // cannot read a file that was never uploaded
    return Boolean(rule.public) || user === rule.owner
      || (rule.readers || []).includes(user);
  }

  canWrite(user, filename) {
    const rule = this._rules[filename];
    if (!rule) return true; // brand new file: the first uploader claims it
    return user === rule.owner || (rule.writers || []).includes(user);
  }

  /** The set of file names this user is allowed to see. */
  visibleTo(user) {
    return Object.entries(this._rules)
      .filter(([, rule]) => rule.public || user === rule.owner
        || (rule.readers || []).includes(user)
        || (rule.writers || []).includes(user))
      .map(([name]) => name);
  }

  // ------------------------------------------------------------------- writes
  /** Called after a successful upload: create the rule if it is new. */
  async claim(user, filename) {
    const created = await this._exclusive(() => {
      if (this._rules[filename]) return false;
      this._rules[filename] = { owner: user, readers: [], writers: [], public: false };
      return true;
    });

    if (created) bus.publish(POLICY_CHANGED, { file: filename, user, action: 'owner' });
  }

  /** Owner grants read or write access to another user. */
  async share(owner, filename, targetUser, permission) {
    const outcome = await this._exclusive(() => {
      const rule = this._rules[filename];
      if (!rule) return { ok: false, message: 'file not found' };
      if (rule.owner !== owner) {
        return { ok: false, message: 'only the owner can share this file' };
      }

      let bucket;
      if (permission === READ) bucket = (rule.readers ||= []);
      else if (permission === WRITE) bucket = (rule.writers ||= []);
      else return { ok: false, message: 'permission must be read or write' };

      if (!bucket.includes(targetUser)) bucket.push(targetUser);
      return {
        ok: true,
        message: `granted ${permission} access on ${filename} to ${targetUser}`,
      };
    });

    if (outcome.ok) {
      bus.publish(POLICY_CHANGED, {
        file: filename, user: owner, action: 'share',
        target: targetUser, permission,
      });
    }
    return outcome;
  }

  async setPublic(owner, filename, isPublic = true) {
    const outcome = await this._exclusive(() => {
      const rule = this._rules[filename];
      if (!rule) return { ok: false, message: 'file not found' };
      if (rule.owner !== owner) {
        return { ok: false, message: 'only the owner can change visibility' };
      }
      rule.public = Boolean(isPublic);
      return {
        ok: true,
        message: `${filename} is now ${isPublic ? 'public' : 'private'}`,
      };
    });

    if (outcome.ok) {
      bus.publish(POLICY_CHANGED, { file: filename, user: owner, action: 'public' });
    }
    return outcome;
  }
}

module.exports = { PolicyStore, READ, WRITE };
