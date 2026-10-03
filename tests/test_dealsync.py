import io
import os
import tempfile
import unittest

from dealsync import (
    BatchConflictError,
    DocStore,
    DuplicateDocIdError,
    VersionNotFoundError,
)


def body(doc: str, version: int) -> bytes:
    return f"{doc}-v{version}-".encode() + bytes([version % 256]) * 64


class ForwardOnlyStream(io.RawIOBase):
    """只能向前读取的二进制流：不支持 seek/tell。"""

    def __init__(self, data: bytes, chunk: int = 7):
        self._buf = io.BytesIO(data)
        self._chunk = chunk

    def readable(self):
        return True

    def seekable(self):
        return False

    def read(self, n=-1):
        if n is None or n < 0:
            n = self._chunk
        return self._buf.read(min(n, self._chunk))


class DocStoreTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.store = DocStore(self.root)

    def tearDown(self):
        self.store.close()
        self._tmp.cleanup()

    def put(self, batch_id, pairs):
        return self.store.put_batch(
            batch_id, [(d, ForwardOnlyStream(b)) for d, b in pairs]
        )

    def body_files(self):
        out = []
        for dirpath, _, names in os.walk(os.path.join(self.root, "bodies")):
            out.extend(os.path.join(dirpath, n) for n in names)
        return sorted(out)

    def tmp_files(self):
        tmp = os.path.join(self.root, "tmp")
        return os.listdir(tmp) if os.path.isdir(tmp) else []


class RetentionTest(DocStoreTestBase):
    def test_two_docs_25_versions_then_joint_batch(self):
        docs = ["alpha", "beta"]
        # 每个文档先各自提交到 25 版
        for v in range(1, 26):
            for d in docs:
                r = self.put(f"seed-{d}-{v}", [(d, body(d, v))])
                self.assertEqual(r, [(d, v)])
        for d in docs:
            self.assertEqual(self.store.list_versions(d), list(range(1, 26)))

        # 一个批次同时更新两个文档 -> 各自产生第 26 版，淘汰第 1 版
        r = self.put("joint-26", [("alpha", body("alpha", 26)),
                                ("beta", body("beta", 26))])
        self.assertEqual(r, [("alpha", 26), ("beta", 26)])

        for d in docs:
            versions = self.store.list_versions(d)
            # 只保留最新 25 个，且版本号连续
            self.assertEqual(versions, list(range(2, 27)))
            self.assertEqual(len(versions), 25)
            # 可读取版本逐字节正确
            for v in versions:
                with self.store.open_version(d, v) as f:
                    self.assertEqual(f.read(), body(d, v))
            # 已淘汰版本不可读取
            with self.assertRaises(VersionNotFoundError):
                self.store.open_version(d, 1)

        # 正文文件回收：目录中只剩 2*25 个文件，且无临时文件残留
        self.assertEqual(len(self.body_files()), 50)
        self.assertEqual(self.tmp_files(), [])


class IdempotentReplayTest(DocStoreTestBase):
    def test_replay_after_eviction_and_conflict_on_change(self):
        docs = ["x", "y", "z"]
        original = [(d, body(d, 1)) for d in docs]
        r1 = self.put("batch-A", original)
        self.assertEqual(r1, [("x", 1), ("y", 1), ("z", 1)])

        # 继续上传直到 batch-A 产生的版本全部淘汰（再推 25 版）
        for v in range(2, 27):
            for d in docs:
                self.put(f"churn-{d}-{v}", [(d, body(d, v))])
        for d in docs:
            self.assertEqual(self.store.list_versions(d), list(range(2, 27)))

        files_before = self.body_files()

        # 用原 batch_id 重传完全相同的内容：返回原版本号，不产生正文
        r2 = self.put("batch-A", original)
        self.assertEqual(r2, r1)
        self.assertEqual(self.body_files(), files_before)
        for d in docs:
            self.assertEqual(self.store.list_versions(d), list(range(2, 27)))
        # 原版本号虽被返回，但已淘汰的旧正文不得复活
        for d in docs:
            with self.assertRaises(VersionNotFoundError):
                self.store.open_version(d, 1)

        # 仅修改最后一个文件的末尾字节 -> 冲突，且其他文档不变
        changed = list(original)
        changed[-1] = (changed[-1][0], changed[-1][1][:-1] + b"\x00")
        self.assertNotEqual(changed[-1][1], original[-1][1])
        with self.assertRaises(BatchConflictError):
            self.put("batch-A", changed)
        for d in docs:
            self.assertEqual(self.store.list_versions(d), list(range(2, 27)))
            for v in range(2, 27):
                with self.store.open_version(d, v) as f:
                    self.assertEqual(f.read(), body(d, v))
        self.assertEqual(self.body_files(), files_before)
        self.assertEqual(self.tmp_files(), [])


class BasicSemanticsTest(DocStoreTestBase):
    def test_versions_start_at_1_and_never_merge_same_content(self):
        r1 = self.put("b1", [("doc", b"same")])
        r2 = self.put("b2", [("doc", b"same")])
        self.assertEqual(r1, [("doc", 1)])
        self.assertEqual(r2, [("doc", 2)])
        self.assertEqual(self.store.list_versions("doc"), [1, 2])

    def test_failed_upload_does_not_consume_version(self):
        self.put("ok-1", [("doc", b"v1")])
        with self.assertRaises(DuplicateDocIdError):
            self.put("bad", [("doc", b"a"), ("doc", b"b")])
        with self.assertRaises(BatchConflictError):
            self.put("ok-1", [("doc", b"different")])
        r = self.put("ok-2", [("doc", b"v2")])
        self.assertEqual(r, [("doc", 2)])

    def test_atomic_batch_failure_leaves_no_partial_state(self):
        self.put("base", [("a", b"a1"), ("b", b"b1")])
        with self.assertRaises(BatchConflictError):
            # 与已提交批次同 id 但内容不同 -> 整体不生效
            self.put("base", [("a", b"a1"), ("b", b"tampered")])
        self.assertEqual(self.store.list_versions("a"), [1])
        self.assertEqual(self.store.list_versions("b"), [1])
        self.assertEqual(self.tmp_files(), [])

    def test_open_unknown_doc_and_version(self):
        with self.assertRaises(VersionNotFoundError):
            self.store.open_version("nope", 1)
        self.assertEqual(self.store.list_versions("nope"), [])

    def test_uncommitted_batch_id_not_bound_to_content(self):
        # 尚未提交的 batch_id 不绑定请求内容：首次以任意内容提交即固定
        r = self.put("flex", [("d", b"content-1")])
        self.assertEqual(r, [("d", 1)])
        with self.assertRaises(BatchConflictError):
            self.put("flex", [("d", b"content-2")])


if __name__ == "__main__":
    unittest.main()
