#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#
from dataclasses import dataclass

from aistore.sdk.const import (
    HEADER_OBJECT_BLOB_CHUNK_SIZE,
    HEADER_OBJECT_BLOB_DOWNLOAD,
    HEADER_OBJECT_BLOB_READ_TIMEOUT,
    HEADER_OBJECT_BLOB_THRESHOLD,
    HEADER_OBJECT_BLOB_WORKERS,
)


@dataclass
class BlobDownloadConfig:
    """
    Configuration for downloading objects using a blob downloader

    Attributes:
        chunk_size (str, optional): Chunk size for the blob downloader. It can be specified in IEC
            or SI units, or as raw bytes (e.g., "4mb", "1MiB", "1048576", "128k")
        num_workers (str, optional): Number of concurrent workers for the blob downloader
        read_timeout (str, optional): Per-attempt timeout for reading a chunk from the backend,
            expressed as a duration string (e.g., "30s", "2m")
        threshold (int, optional): Minimum remote object size in bytes that enables the blob
            downloader. Zero disables blob download; omitting it preserves forced blob download.

    Note:
        Objects smaller than 128 KiB are served via regular GET (no blob-download job).

    Example:
        blob_settings = BlobDownloadConfig(
            threshold=64 * 1024 * 1024,  # Download objects of at least 64 MiB
            chunk_size="1MiB",  # 1 MiB per chunk
            num_workers="5",    # 5 concurrent download workers
            read_timeout="30s"  # Per-attempt backend read timeout
        )
    """

    chunk_size: str = None
    num_workers: str = None
    read_timeout: str = None
    threshold: int = None

    def __post_init__(self):
        if self.threshold is not None and (
            isinstance(self.threshold, bool)
            or not isinstance(self.threshold, int)
            or self.threshold < 0
        ):
            raise ValueError("`threshold` must be a non-negative integer.")

    def as_headers(self) -> dict:
        """Return request headers for this blob-download configuration."""
        headers = {}
        if self.threshold is None:
            headers[HEADER_OBJECT_BLOB_DOWNLOAD] = "true"
        else:
            headers[HEADER_OBJECT_BLOB_THRESHOLD] = str(self.threshold)
        if self.chunk_size is not None:
            headers[HEADER_OBJECT_BLOB_CHUNK_SIZE] = self.chunk_size
        if self.num_workers is not None:
            headers[HEADER_OBJECT_BLOB_WORKERS] = self.num_workers
        if self.read_timeout is not None:
            headers[HEADER_OBJECT_BLOB_READ_TIMEOUT] = self.read_timeout
        return headers
