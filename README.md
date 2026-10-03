# DealSync 离线文档批次版本库

以 SQLite 保存元数据、本地文件保存正文的离线文档版本库。纯 Python 标准库实现，无需任何外部服务。

## 用法

```python
import io
from dealsync import DocStore

store = DocStore("/path/to/storage")   # 目录下生成 meta.sqlite3、bodies/、tmp/

# 批量上传：每项为 (doc_id, 只能向前读取的二进制流)，批内 doc_id 互异
versions = store.put_batch("batch-001", [
    ("contract", io.BytesIO(b"v1 content")),
    ("invoice", open("invoice.pdf", "rb")),
])
# -> {"contract": 1, "invoice": 1}

store.list_versions("contract")        # -> [1]

with store.open_version("contract", 1) as stream:
    data = stream.read()               # 流式读取，逐字节一致

store.close()
```

## 语义

- 每个文档版本号从 1 开始连续递增；失败的上传不占用版本号。
- 一个批次内所有新版本与旧版本淘汰原子生效，不存在部分提交。
- 每个文档仅保留最新 25 个已提交版本；被淘汰版本不可读取，正文文件随即回收。
- 相同 `batch_id` 重传时，仅当文档顺序、doc_id 及全部正文完全一致才返回原版本号（幂等），否则抛 `BatchConflictError` 且不改变任何状态；即使原版本已淘汰也维持该语义。
- 成功调用结束后，存储目录中只保留当前保留版本的正文，不残留临时文件。

## 测试

```bash
python3 -m pytest tests/ -v
```
