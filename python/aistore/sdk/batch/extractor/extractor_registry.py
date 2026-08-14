#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from types import MappingProxyType

from aistore.sdk.batch.extractor.tar_stream_extractor import TarStreamExtractor
from aistore.sdk.batch.extractor.zip_stream_extractor import ZipStreamExtractor
from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor

# Built once at module import time — thread-safe via Python's import system.
# Wrapped in MappingProxyType to prevent mutation after import.
_FORMAT_MAP: MappingProxyType = MappingProxyType(
    {
        fmt: extractor
        for extractor in (TarStreamExtractor(), ZipStreamExtractor())
        for fmt in extractor.get_supported_formats()
    }
)


def get_extractor(output_format: str) -> ArchiveStreamExtractor:
    """
    Returns the ArchiveStreamExtractor for a given output format.

    Args:
        output_format (str): Output format string (e.g. ".tar", ".zip")

    Returns:
        ArchiveStreamExtractor: Extractor supporting the given format

    Raises:
        ValueError: If the format is not supported
    """
    if output_format is not None:
        extractor = _FORMAT_MAP.get(output_format.lower())
        if extractor is not None:
            return extractor
    raise ValueError(f"Unsupported output format type {output_format}")
