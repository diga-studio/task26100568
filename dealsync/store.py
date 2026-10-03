"""Offline document batch version store backed by SQLite and local files."""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import threading
import uuid
from typing import BinaryIO, Dict, Iterable, List, Tuple

from .errors import (
    BatchConflictError,
    DuplicateDocIdError,
    VersionNotFoundError,
)

KEEP_VERSIONS = 25

_CHUNK_SIZE = 1024 * 1024

_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT NOT NULL UNIQUE,
    next_version INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS versions (
    doc_pk INTEGER NOT NULL REFERENCES documents(id),
    version INTEGER NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    PRIMARY KEY (doc_pk, version)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_entries (
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    seq INTEGER NOT NULL,
    doc_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (batch_id, seq)
);
"""


class DocStore:
    """Versioned document store.

    Metadata lives in a SQLite database; version bodies live as files under
    a bodies directory. All multi-document mutations are atomic.
    """

    def __init__(self, root: str) -> None:
        self._root = root
        self._bodies_dir = os.path.join(root, "bodies")
        self._tmp_dir = os.path.join(root, "tmp")
        os.makedirs(self._bodies_dir, exist_ok=True)
        # Remove temp bodies left behind by interrupted calls.
        if os.path.isdir(self._tmp_dir):
            shutil.rmtree(self._tmp_dir)
        os.makedirs(self._tmp_dir)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(
            os.path.join(root, "meta.sqlite3"), check_same_thread=False
        )
        self._db.isolation_level = None
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(_SCHEMA)

    def put_batch(
        self, batch_id: str, entries: Iterable[Tuple[str, BinaryIO]]
    ) -> Dict[str, int]:
        """Commit a batch of (doc_id, forward-only binary stream) entries.

        Returns a mapping of doc_id to the version number it received.
        """
        items = list(entries)
        doc_ids = [doc_id for doc_id, _ in items]
        if len(set(doc_ids)) != len(doc_ids):
            raise DuplicateDocIdError(
                "batch contains duplicate doc_id values"
            )

        with self._lock:
            spooled = []  # (doc_id, tmp_path, size, sha256)
            try:
                request_hasher = hashlib.sha256()
                for doc_id, stream in items:
                    tmp_path, size, digest = self._spool(stream)
                    spooled.append((doc_id, tmp_path, size, digest))
                    request_hasher.update(len(doc_id).to_bytes(8, "big"))
                    request_hasher.update(doc_id.encode("utf-8"))
                    request_hasher.update(bytes.fromhex(digest))
                request_hash = request_hasher.hexdigest()

                row = self._db.execute(
                    "SELECT request_hash FROM batches WHERE batch_id = ?",
                    (batch_id,),
                ).fetchone()
                if row is not None:
                    if row[0] != request_hash:
                        raise BatchConflictError(
                            f"batch_id {batch_id!r} already committed with "
                            "different content"
                        )
                    replay = self._db.execute(
                        "SELECT doc_id, version FROM batch_entries "
                        "WHERE batch_id = ? ORDER BY seq",
                        (batch_id,),
                    ).fetchall()
                    return {doc_id: version for doc_id, version in replay}

                return self._commit_batch(batch_id, spooled, request_hash)
            finally:
                for _, tmp_path, _, _ in spooled:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)

    def _spool(self, stream: BinaryIO) -> Tuple[str, int, str]:
        """Read a forward-only stream into a temp file; hash it on the way."""
        tmp_path = os.path.join(self._tmp_dir, uuid.uuid4().hex)
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp_path, "wb") as out:
                while True:
                    chunk = stream.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    out.write(chunk)
                    hasher.update(chunk)
                    size += len(chunk)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        return tmp_path, size, hasher.hexdigest()

    def _commit_batch(
        self,
        batch_id: str,
        spooled: List[Tuple[str, str, int, str]],
        request_hash: str,
    ) -> Dict[str, int]:
        placed = []  # body paths already moved into place for this batch
        try:
            result: Dict[str, int] = {}
            moves = []
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute(
                "INSERT INTO batches (batch_id, request_hash) VALUES (?, ?)",
                (batch_id, request_hash),
            )
            for seq, (doc_id, tmp_path, size, digest) in enumerate(spooled):
                row = self._db.execute(
                    "SELECT id, next_version FROM documents WHERE doc_id = ?",
                    (doc_id,),
                ).fetchone()
                if row is None:
                    cur = self._db.execute(
                        "INSERT INTO documents (doc_id) VALUES (?)", (doc_id,)
                    )
                    doc_pk, version = cur.lastrowid, 1
                else:
                    doc_pk, version = row
                self._db.execute(
                    "UPDATE documents SET next_version = ? WHERE id = ?",
                    (version + 1, doc_pk),
                )
                self._db.execute(
                    "INSERT INTO versions (doc_pk, version, size, sha256) "
                    "VALUES (?, ?, ?, ?)",
                    (doc_pk, version, size, digest),
                )
                self._db.execute(
                    "INSERT INTO batch_entries "
                    "(batch_id, seq, doc_id, version) VALUES (?, ?, ?, ?)",
                    (batch_id, seq, doc_id, version),
                )
                body_path = self._body_path(doc_pk, version)
                moves.append((tmp_path, body_path))
                result[doc_id] = version

            evicted = []
            for doc_pk, version in self._db.execute(
                "SELECT v.doc_pk, v.version FROM versions v "
                "JOIN documents d ON d.id = v.doc_pk "
                "WHERE v.version <= d.next_version - 1 - ?",
                (KEEP_VERSIONS,),
            ).fetchall():
                self._db.execute(
                    "DELETE FROM versions WHERE doc_pk = ? AND version = ?",
                    (doc_pk, version),
                )
                evicted.append(self._body_path(doc_pk, version))

            # Put new bodies in place before committing the metadata that
            # references them, so a committed version never lacks its body.
            for tmp_path, body_path in moves:
                os.makedirs(os.path.dirname(body_path), exist_ok=True)
                os.replace(tmp_path, body_path)
                placed.append(body_path)
            self._db.execute("COMMIT")
        except Exception:
            self._db.execute("ROLLBACK")
            for body_path in placed:
                if os.path.exists(body_path):
                    os.unlink(body_path)
            raise

        # Committed: remove evicted bodies; the batch is fully durable.
        for path in evicted:
            if os.path.exists(path):
                os.unlink(path)
        return result

    def _body_path(self, doc_pk: int, version: int) -> str:
        return os.path.join(self._bodies_dir, str(doc_pk), f"{version}.bin")

    def list_versions(self, doc_id: str) -> List[int]:
        """Return the currently readable version numbers of a document."""
        rows = self._db.execute(
            "SELECT v.version FROM versions v "
            "JOIN documents d ON d.id = v.doc_pk "
            "WHERE d.doc_id = ? ORDER BY v.version",
            (doc_id,),
        ).fetchall()
        return [row[0] for row in rows]

    def open_version(self, doc_id: str, version: int) -> BinaryIO:
        """Open a readable binary stream for a retained version body."""
        row = self._db.execute(
            "SELECT v.doc_pk, v.version FROM versions v "
            "JOIN documents d ON d.id = v.doc_pk "
            "WHERE d.doc_id = ? AND v.version = ?",
            (doc_id, version),
        ).fetchone()
        if row is None:
            raise VersionNotFoundError(
                f"version {version} of document {doc_id!r} is not readable"
            )
        return open(self._body_path(row[0], row[1]), "rb")

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "DocStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
