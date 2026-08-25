"""File sharing policies: who may read, write or share which file.

Stored as one small JSON file so anyone can open it and read the rules:

    {
      "report.pdf": {
        "owner":   "alice",
        "readers": ["bob"],
        "writers": [],
        "public":  false
      }
    }

Rules, in plain English:
  * whoever uploads a file first becomes its owner;
  * the owner may always read, overwrite and share it;
  * a reader may download it, a writer may overwrite it;
  * a public file may be downloaded by anyone;
  * a file with no entry yet is free for anyone to claim.
"""

import contextlib
import json
import os
import threading
import time

from ..common import config
from ..common.events import bus, POLICY_CHANGED

READ = "read"
WRITE = "write"


class PolicyStore:
    def __init__(self, path=None):
        self.path = path or config.POLICY_FILE
        self._lock = threading.Lock()
        self._rules = {}
        self._load()

    # ------------------------------------------------------------ persistence
    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    self._rules = json.load(fh)
            except (ValueError, OSError):
                self._rules = {}

    def _save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(self._rules, fh, indent=2)

    @contextlib.contextmanager
    def _exclusive(self):
        """Guard a read-modify-write of the shared policy file.

        Every node runs in its own process but they all edit the same
        policies.json. Without this, two nodes finishing an upload at the same
        moment would each save their own copy and one rule would be lost. A
        simple lock file (created with O_EXCL, which only one process can win)
        serialises them.
        """
        lock_path = self.path + ".lock"
        deadline = time.time() + 5.0
        while True:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                break
            except FileExistsError:
                if time.time() > deadline:
                    os.remove(lock_path)   # a crashed node left it behind
                    continue
                time.sleep(0.01)
        try:
            self._load()      # pick up anything another node just wrote
            yield
            self._save()
        finally:
            os.close(fd)
            try:
                os.remove(lock_path)
            except OSError:
                pass

    def reload(self):
        """Re-read the rules from disk (other nodes may have changed them)."""
        with self._lock:
            self._load()

    def all_rules(self):
        with self._lock:
            return json.loads(json.dumps(self._rules))  # plain copy

    # ---------------------------------------------------------------- queries
    def owner_of(self, filename):
        with self._lock:
            rule = self._rules.get(filename)
            return rule["owner"] if rule else None

    def can_read(self, user, filename):
        with self._lock:
            rule = self._rules.get(filename)
            if rule is None:
                return False  # cannot read a file that was never uploaded
            return (rule.get("public", False)
                    or user == rule["owner"]
                    or user in rule.get("readers", []))

    def can_write(self, user, filename):
        with self._lock:
            rule = self._rules.get(filename)
            if rule is None:
                return True  # brand new file: the first uploader claims it
            return user == rule["owner"] or user in rule.get("writers", [])

    # ----------------------------------------------------------------- writes
    def claim(self, user, filename):
        """Called after a successful upload: create the rule if it is new."""
        with self._lock, self._exclusive():
            created = filename not in self._rules
            if created:
                self._rules[filename] = {
                    "owner": user,
                    "readers": [],
                    "writers": [],
                    "public": False,
                }
        if created:
            bus.publish(POLICY_CHANGED, file=filename, user=user, action="owner")

    def share(self, owner, filename, target_user, permission):
        """Owner grants read or write access to another user."""
        with self._lock, self._exclusive():
            rule = self._rules.get(filename)
            if rule is None:
                return False, "file not found"
            if rule["owner"] != owner:
                return False, "only the owner can share this file"

            if permission == READ:
                bucket = rule.setdefault("readers", [])
            elif permission == WRITE:
                bucket = rule.setdefault("writers", [])
            else:
                return False, "permission must be read or write"

            if target_user not in bucket:
                bucket.append(target_user)

        bus.publish(POLICY_CHANGED, file=filename, user=owner,
                    action="share", target=target_user, permission=permission)
        return True, "granted %s access on %s to %s" % (permission, filename, target_user)

    def set_public(self, owner, filename, public=True):
        with self._lock, self._exclusive():
            rule = self._rules.get(filename)
            if rule is None:
                return False, "file not found"
            if rule["owner"] != owner:
                return False, "only the owner can change visibility"
            rule["public"] = bool(public)
        bus.publish(POLICY_CHANGED, file=filename, user=owner, action="public")
        return True, "%s is now %s" % (filename, "public" if public else "private")

    def visible_to(self, user):
        """The set of file names this user is allowed to see."""
        with self._lock:
            return [name for name, rule in self._rules.items()
                    if rule.get("public") or user == rule["owner"]
                    or user in rule.get("readers", [])
                    or user in rule.get("writers", [])]
