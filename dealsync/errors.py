"""Exception types for the DealSync offline document version store."""


class DealSyncError(Exception):
    """Base class for all DealSync errors."""


class BatchConflictError(DealSyncError):
    """A committed batch_id was reused with different request content."""


class DuplicateDocIdError(DealSyncError):
    """A batch contains the same doc_id more than once."""


class VersionNotFoundError(DealSyncError):
    """The requested document or version does not exist or was evicted."""
