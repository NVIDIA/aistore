#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
from dataclasses import dataclass


@dataclass
class BlobDownloadConfig:
    """
    Configuration for downloading objects using a blob downloader

    Attributes:
        chunk_size (str, optional): Chunk size for the blob downloader. It can be specified in IEC
            or SI units, or as raw bytes (e.g., "4mb", "1MiB", "1048576", "128k")
        num_workers (str, optional): Number of concurrent workers for the blob downloader

    Example:
        blob_settings = BlobDownloadConfig(
            chunk_size="1MiB",  # 1 MiB per chunk
            num_workers="5"     # 5 concurrent download workers
        )
    """

    chunk_size: str = None
    num_workers: str = None
