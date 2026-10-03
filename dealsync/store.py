"""DealSync 离线文档批次版本库：SQLite 元数据 + 本地文件正文。"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from typing import BinaryIO, Dict, List, Sequence, Tuple, Union

from .errors import (
    BatchConflictError,
    DuplicateDocIdError,
    VersionNotFoundError,
)

KEEP_VERSIONS = 25
_CHUNK = 1 << 16

Entry = Union[Tuple[str, BinaryIO], Dict[str, object]]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_id TEXT PRIMARY KEY,
    next_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    doc_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    relpath TEXT NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    PRIMARY KEY (doc_id, version)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    result TEXT NOT NULL
);
"""


def _parse_entry(entry: Entry) -> Tuple[str, BinaryIO]:
    if isinstance(entry, dict):
        return entry["doc_id"], entry["stream"]  # type: ignore[return-value]
    doc_id, stream = entry
    return doc_id, stream


class DocStore:
    """离线文档批次版本库。

    元数据存于 ``<root>/meta.sqlite3``，正文存于 ``<root>/bodies/``。
    """

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        self._bodies = os.path.join(self.root, "bodies")
        self._tmp = os.path.join(self.root, "tmp")
        os.makedirs(self._bodies, exist_ok=True)
        os.makedirs(self._tmp, exist_ok=True)
        self._conn = sqlite3.connect(os.path.join(self.root, "meta.sqlite3"))
        self._conn.isolation_level = None
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DocStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ put

    def put_batch(
        self, batch_id: str, entries: Sequence[Entry]
    ) -> List[Tuple[str, int]]:
        """批量上传，返回各文档本次获得的版本号（按传入顺序）。"""
        parsed = [_parse_entry(e) for e in entries]
        seen = set()
        for doc_id, _ in parsed:
            if doc_id in seen:
                raise DuplicateDocIdError(f"批次内 doc_id 重复: {doc_id!r}")
            seen.add(doc_id)

        # 1. 把流式正文写入临时文件，同时计算指纹（不依赖可寻址流）。
        staged: List[Tuple[str, str, int, str]] = []  # doc_id, tmp, size, sha
        try:
            for doc_id, stream in parsed:
                tmp_path, size, digest = self._spool(stream)
                staged.append((doc_id, tmp_path, size, digest))
        except Exception:
            for _, tmp_path, _, _ in staged:
                _silent_remove(tmp_path)
            raise

        fingerprint = self._fingerprint(staged)

        # 2. 元数据事务：版本分配、淘汰、批次记录原子生效。
        placed: List[str] = []  # 已就位的正文文件，失败时回滚删除
        evicted: List[str] = []
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT fingerprint, result FROM batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if row is not None:
                if row[0] != fingerprint:
                    raise BatchConflictError(
                        f"batch_id {batch_id!r} 已提交且内容不一致"
                    )
                result = [tuple(item) for item in json.loads(row[1])]
                self._conn.execute("COMMIT")
                for _, tmp_path, _, _ in staged:
                    _silent_remove(tmp_path)
                return result  # 幂等重放：不创建新版本、不产生正文

            result: List[Tuple[str, int]] = []
            for doc_id, tmp_path, size, digest in staged:
                version = self._next_version(doc_id)
                relpath = self._place(doc_id, version, tmp_path)
                placed.append(relpath)
                self._conn.execute(
                    "INSERT INTO versions"
                    " (doc_id, version, relpath, size, sha256, batch_id)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (doc_id, version, relpath, size, digest, batch_id),
                )
                result.append((doc_id, version))
                evicted.extend(self._evict(doc_id))
            self._conn.execute(
                "INSERT INTO batches (batch_id, fingerprint, result)"
                " VALUES (?, ?, ?)",
                (batch_id, fingerprint, json.dumps(result)),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            for relpath in placed:
                _silent_remove(os.path.join(self.root, relpath))
            for _, tmp_path, _, _ in staged:
                _silent_remove(tmp_path)
            raise

        # 3. 提交成功后回收被淘汰版本的正文文件。
        for relpath in evicted:
            path = os.path.join(self.root, relpath)
            _silent_remove(path)
            _silent_rmdir(os.path.dirname(path))
        return result

    def _spool(self, stream: BinaryIO) -> Tuple[str, int, str]:
        fd, tmp_path = tempfile.mkstemp(dir=self._tmp, prefix="stage-")
        size = 0
        digest = hashlib.sha256()
        try:
            with os.fdopen(fd, "wb") as out:
                while True:
                    chunk = stream.read(_CHUNK)
                    if not chunk:
                        break
                    out.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        except Exception:
            _silent_remove(tmp_path)
            raise
        return tmp_path, size, digest.hexdigest()

    @staticmethod
    def _fingerprint(staged: List[Tuple[str, str, int, str]]) -> str:
        h = hashlib.sha256()
        for doc_id, _, size, digest in staged:
            raw = doc_id.encode("utf-8")
            h.update(len(raw).to_bytes(8, "big"))
            h.update(raw)
            h.update(size.to_bytes(8, "big"))
            h.update(bytes.fromhex(digest))
        return h.hexdigest()

    def _next_version(self, doc_id: str) -> int:
        row = self._conn.execute(
            "SELECT next_version FROM documents WHERE doc_id = ?", (doc_id,)
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO documents (doc_id, next_version) VALUES (?, 2)",
                (doc_id,),
            )
            return 1
        version = row[0]
        self._conn.execute(
            "UPDATE documents SET next_version = ? WHERE doc_id = ?",
            (version + 1, doc_id),
        )
        return version

    def _place(self, doc_id: str, version: int, tmp_path: str) -> str:
        doc_dir = os.path.join(self._bodies, hashlib.sha256(
            doc_id.encode("utf-8")).hexdigest())
        os.makedirs(doc_dir, exist_ok=True)
        final = os.path.join(doc_dir, f"{version}.bin")
        os.replace(tmp_path, final)
        return os.path.relpath(final, self.root)

    def _evict(self, doc_id: str) -> List[str]:
        rows = self._conn.execute(
            "SELECT version, relpath FROM versions WHERE doc_id = ?"
            " ORDER BY version DESC",
            (doc_id,),
        ).fetchall()
        doomed = rows[KEEP_VERSIONS:]
        for version, _ in doomed:
            self._conn.execute(
                "DELETE FROM versions WHERE doc_id = ? AND version = ?",
                (doc_id, version),
            )
        return [relpath for _, relpath in doomed]

    # ------------------------------------------------------------- read API

    def list_versions(self, doc_id: str) -> List[int]:
        """返回当前可读取（未淘汰）的版本号，升序。"""
        rows = self._conn.execute(
            "SELECT version FROM versions WHERE doc_id = ? ORDER BY version",
            (doc_id,),
        ).fetchall()
        return [r[0] for r in rows]

    def open_version(self, doc_id: str, version: int) -> BinaryIO:
        """流式读取指定版本正文；已淘汰或不存在则抛 VersionNotFoundError。"""
        row = self._conn.execute(
            "SELECT relpath FROM versions WHERE doc_id = ? AND version = ?",
            (doc_id, version),
        ).fetchone()
        if row is None:
            raise VersionNotFoundError(f"{doc_id!r} 的版本 {version} 不可读取")
        return open(os.path.join(self.root, row[0]), "rb")


def _silent_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _silent_rmdir(path: str) -> None:
    try:
        os.rmdir(path)
    except OSError:
        pass
