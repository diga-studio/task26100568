"""Tests for the DealSync offline document batch version store."""

import io
import os

import pytest

from dealsync import (
    BatchConflictError,
    DocStore,
    DuplicateDocIdError,
    KEEP_VERSIONS,
    VersionNotFoundError,
)


def body(data: bytes) -> io.BytesIO:
    return io.BytesIO(data)


def payload(doc: str, version: int) -> bytes:
    return f"{doc}-version-{version}-".encode() * (version % 7 + 1)


def stored_files(root: str) -> set:
    found = set()
    for dirpath, _, files in os.walk(os.path.join(root, "bodies")):
        for name in files:
            found.add(os.path.relpath(os.path.join(dirpath, name), root))
    return found


def tmp_files(root: str) -> list:
    tmp_dir = os.path.join(root, "tmp")
    if not os.path.isdir(tmp_dir):
        return []
    return os.listdir(tmp_dir)


def test_batch_commit_retention_and_file_reclaim(tmp_path):
    root = str(tmp_path)
    store = DocStore(root)
    docs = ["alpha", "beta"]

    # Bring both documents to 25 committed versions each.
    for round_no in range(KEEP_VERSIONS):
        entries = [
            (doc, body(payload(doc, round_no + 1))) for doc in docs
        ]
        result = store.put_batch(f"setup-{round_no}", entries)
        assert result == {doc: round_no + 1 for doc in docs}

    for doc in docs:
        assert store.list_versions(doc) == list(range(1, KEEP_VERSIONS + 1))

    # One batch that adds a new version to both documents at once.
    result = store.put_batch(
        "joint-batch",
        [(doc, body(payload(doc, KEEP_VERSIONS + 1))) for doc in docs],
    )
    assert result == {doc: KEEP_VERSIONS + 1 for doc in docs}

    # Version numbers stay continuous; only the newest 25 are readable.
    for doc in docs:
        versions = store.list_versions(doc)
        assert versions == list(range(2, KEEP_VERSIONS + 2))
        for version in versions:
            with store.open_version(doc, version) as stream:
                assert stream.read() == payload(doc, version)
        with pytest.raises(VersionNotFoundError):
            store.open_version(doc, 1)

    # Only bodies of retained versions remain; no temp bodies linger.
    assert len(stored_files(root)) == 2 * KEEP_VERSIONS
    assert tmp_files(root) == []
    store.close()


def test_idempotent_replay_after_eviction_then_conflict(tmp_path):
    root = str(tmp_path)
    store = DocStore(root)
    docs = ["doc-a", "doc-b", "doc-c"]

    original = {doc: payload(doc, 1) for doc in docs}
    first = store.put_batch(
        "batch-x", [(doc, body(original[doc])) for doc in docs]
    )
    assert first == {doc: 1 for doc in docs}

    # Push enough later batches to evict every version created by batch-x.
    for round_no in range(KEEP_VERSIONS):
        store.put_batch(
            f"later-{round_no}",
            [(doc, body(payload(doc, round_no + 2))) for doc in docs],
        )
    for doc in docs:
        assert store.list_versions(doc) == list(range(2, KEEP_VERSIONS + 2))
    files_before = stored_files(root)
    assert len(files_before) == len(docs) * KEEP_VERSIONS

    # Replaying the original batch returns the original version numbers
    # even though those versions were evicted, and stores no new bodies.
    replay = store.put_batch(
        "batch-x", [(doc, body(original[doc])) for doc in docs]
    )
    assert replay == first
    for doc in docs:
        assert store.list_versions(doc) == list(range(2, KEEP_VERSIONS + 2))
        with pytest.raises(VersionNotFoundError):
            store.open_version(doc, 1)
    assert stored_files(root) == files_before
    assert tmp_files(root) == []

    # Same batch_id with the last file's final byte changed conflicts and
    # leaves every document untouched.
    tampered = dict(original)
    last_doc = docs[-1]
    tampered[last_doc] = tampered[last_doc][:-1] + bytes(
        [tampered[last_doc][-1] ^ 0xFF]
    )
    with pytest.raises(BatchConflictError):
        store.put_batch(
            "batch-x", [(doc, body(tampered[doc])) for doc in docs]
        )
    for doc in docs:
        assert store.list_versions(doc) == list(range(2, KEEP_VERSIONS + 2))
        for version in range(2, KEEP_VERSIONS + 2):
            with store.open_version(doc, version) as stream:
                assert stream.read() == payload(doc, version)
    assert stored_files(root) == files_before
    assert tmp_files(root) == []
    store.close()


def test_failed_upload_does_not_consume_version_numbers(tmp_path):
    store = DocStore(str(tmp_path))
    store.put_batch("ok-1", [("doc", body(b"first"))])

    class BrokenStream:
        def read(self, size=-1):
            raise IOError("stream exploded")

    with pytest.raises(IOError):
        store.put_batch("bad", [("doc", BrokenStream())])
    assert store.list_versions("doc") == [1]

    result = store.put_batch("ok-2", [("doc", body(b"second"))])
    assert result == {"doc": 2}
    assert store.list_versions("doc") == [1, 2]
    store.close()


def test_duplicate_doc_id_rejected(tmp_path):
    store = DocStore(str(tmp_path))
    with pytest.raises(DuplicateDocIdError):
        store.put_batch(
            "dup", [("doc", body(b"a")), ("doc", body(b"b"))]
        )
    assert store.list_versions("doc") == []
    store.close()


def test_batch_is_atomic_across_documents(tmp_path):
    root = str(tmp_path)
    store = DocStore(root)
    store.put_batch("init", [("a", body(b"a0")), ("b", body(b"b0"))])

    class BrokenStream:
        def read(self, size=-1):
            raise IOError("stream exploded")

    files_before = stored_files(root)
    with pytest.raises(IOError):
        store.put_batch(
            "atomic", [("a", body(b"a1")), ("b", BrokenStream())]
        )
    # Neither document may be partially committed.
    assert store.list_versions("a") == [1]
    assert store.list_versions("b") == [1]
    assert stored_files(root) == files_before
    assert tmp_files(root) == []

    # An uncommitted batch_id is not bound to the failed request content.
    result = store.put_batch(
        "atomic", [("a", body(b"different")), ("b", body(b"b1"))]
    )
    assert result == {"a": 2, "b": 2}
    store.close()


def test_same_content_still_creates_new_version(tmp_path):
    store = DocStore(str(tmp_path))
    store.put_batch("b1", [("doc", body(b"same"))])
    result = store.put_batch("b2", [("doc", body(b"same"))])
    assert result == {"doc": 2}
    assert store.list_versions("doc") == [1, 2]
    store.close()


def test_open_version_streaming_and_persistence(tmp_path):
    root = str(tmp_path)
    data = bytes(range(256)) * 4096  # 1 MiB, exercises chunked reads
    store = DocStore(root)
    store.put_batch("big", [("doc", body(data))])
    store.close()

    reopened = DocStore(root)
    assert reopened.list_versions("doc") == [1]
    chunks = []
    with reopened.open_version("doc", 1) as stream:
        while True:
            chunk = stream.read(65536)
            if not chunk:
                break
            chunks.append(chunk)
    assert b"".join(chunks) == data
    with pytest.raises(VersionNotFoundError):
        reopened.open_version("doc", 99)
    with pytest.raises(VersionNotFoundError):
        reopened.open_version("ghost", 1)
    reopened.close()
