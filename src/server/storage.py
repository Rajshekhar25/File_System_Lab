"""Disk layer of the local cloud.

Two rules keep the whole "large files + crashes" problem simple:

1. An upload in progress is written to  <name>.part
2. Only when the last byte arrives is it renamed to  <name>

So a half-finished file can never be mistaken for a real one, and the size of
the .part file *is* the resume point. Nothing else has to be remembered.
"""

import os
import threading

from ..common import config


class Storage:
    def __init__(self, root=None):
        self.root = root or config.STORAGE_DIR
        os.makedirs(self.root, exist_ok=True)
        # One lock per file name so two threads never append to the same file.
        self._locks = {}
        self._locks_guard = threading.Lock()

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def safe_name(name):
        """Strip any path so a client cannot write outside the storage folder."""
        return os.path.basename(name.replace("\\", "/"))

    def _lock_for(self, name):
        with self._locks_guard:
            if name not in self._locks:
                self._locks[name] = threading.Lock()
            return self._locks[name]

    def final_path(self, name):
        return os.path.join(self.root, self.safe_name(name))

    def part_path(self, name):
        return self.final_path(name) + ".part"

    # ------------------------------------------------------------------ reads
    def exists(self, name):
        return os.path.exists(self.final_path(name))

    def size(self, name):
        path = self.final_path(name)
        return os.path.getsize(path) if os.path.exists(path) else 0

    def uploaded_bytes(self, name):
        """How many bytes of an interrupted upload are already on disk."""
        path = self.part_path(name)
        return os.path.getsize(path) if os.path.exists(path) else 0

    def list_files(self):
        """Every complete file plus every partial one, for the dashboard."""
        files = []
        for entry in sorted(os.listdir(self.root)):
            full = os.path.join(self.root, entry)
            if not os.path.isfile(full) or entry == "policies.json":
                continue
            if entry.endswith(".part"):
                files.append({
                    "name": entry[:-5],
                    "size": os.path.getsize(full),
                    "state": "incomplete",
                })
            else:
                files.append({
                    "name": entry,
                    "size": os.path.getsize(full),
                    "state": "available",
                })
        return files

    def open_for_read(self, name, offset=0):
        handle = open(self.final_path(name), "rb")
        if offset:
            handle.seek(offset)
        return handle

    # ----------------------------------------------------------------- writes
    def open_for_append(self, name, offset):
        """Open the .part file positioned at offset (0 restarts the upload)."""
        path = self.part_path(name)
        mode = "r+b" if os.path.exists(path) else "wb"
        handle = open(path, mode)
        handle.seek(offset)
        handle.truncate(offset)  # drop anything past the agreed resume point
        return handle

    def finalize(self, name):
        """Promote a finished .part file to its real name."""
        part, final = self.part_path(name), self.final_path(name)
        if os.path.exists(final):
            os.remove(final)  # overwrite (the policy layer already allowed it)
        os.replace(part, final)
        return os.path.getsize(final)

    def delete(self, name):
        for path in (self.final_path(name), self.part_path(name)):
            if os.path.exists(path):
                os.remove(path)

    def file_lock(self, name):
        return self._lock_for(self.safe_name(name))
