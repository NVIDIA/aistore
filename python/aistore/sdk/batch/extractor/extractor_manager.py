#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional

from aistore.sdk.batch.extractor.tar_stream_extractor import TarStreamExtractor
from aistore.sdk.batch.extractor.zip_stream_extractor import ZipStreamExtractor
from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor


class ExtractorManager:
    """
    Manager class to handle getting proper batch archive stream extractor
    given an output format. Uses singleton pattern to reuse extractor
    instances.
    """

    # For singleton instantiation
    _instance: Optional["ExtractorManager"] = None
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        """
        Get the default singleton instance of ExtractorManager.

        Returns:
            ExtractorManager: The default singleton instance
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Using supported extractors, initializes format map which maps
        string archive formats to `ArchiveStreamExtractor` implementations.
        """
        # Only initialize once
        if self._initialized:
            return

        supported_extractors = (TarStreamExtractor(), ZipStreamExtractor())

        self._format_map = {}
        for extractor in supported_extractors:
            supported_formats = extractor.get_supported_formats()
            for fmt in supported_formats:
                self._format_map[fmt] = extractor

        self._initialized = True

    def get_extractor(self, output_format: str) -> ArchiveStreamExtractor:
        """
        Returns the matching `ArchiveStreamExtractor` child for a given output format.

        Args:
            output_format (str): Output format to extract

        Returns:
            ArchiveStreamExtractor: Extractor which supports extraction of output format

        Throws:
            ValueError: If output format is not supported by any current extractors
        """
        if (
            output_format is not None
            and self._format_map.get(output_format.lower()) is not None
        ):
            return self._format_map[output_format.lower()]

        raise ValueError(f"Unsupported output format type {output_format}")
