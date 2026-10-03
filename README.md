# task26100568

DealSync 离线文档批次版本库：SQLite 保存元数据，正文存本地文件。

## 使用

```python
import io
from dealsync import DocStore

store = DocStore("/path/to/storage")   # 目录下生成 meta.sqlite3 与 bodies/
store.put_batch("batch-1", [
    ("doc-a", io.BytesIO(b"...")),     # 流只需可向前 read
    ("doc-b", io.BytesIO(b"...")),
])                                     # -> [("doc-a", 1), ("doc-b", 1)]
store.list_versions("doc-a")           # -> [1]
with store.open_version("doc-a", 1) as f:
    data = f.read()
```

- 每个文档只保留最新 25 个已提交版本；淘汰版本的正文文件随即回收。
- 同一 `batch_id` 重传完全相同内容（顺序、doc_id、正文）返回原版本号；
  任何一处不同抛 `BatchConflictError` 且不改变状态。
- 失败上传不占版本号；批次内版本与淘汰原子生效。

## 测试

```
python3 -m unittest discover -s tests -v
```
