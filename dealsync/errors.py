"""DealSync 离线文档批次版本库的异常定义。"""


class DealSyncError(Exception):
    """库内所有异常的基类。"""


class BatchConflictError(DealSyncError):
    """同一 batch_id 已提交，但本次请求内容与原提交不一致。"""


class DuplicateDocIdError(DealSyncError):
    """同一批次内出现重复的 doc_id。"""


class VersionNotFoundError(DealSyncError):
    """请求的文档版本不存在或已被淘汰。"""
