#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from aistore.sdk.get_batch.buffered_content_reader import BufferedContentReader


class MultipartBoundaryDetector:
    """
    Handles boundary detection for multipart streams.

    This class is responsible only for detecting boundaries and providing
    boundary-specific calculations. Data extraction is handled by the
    BufferedContentReader abstraction layer.

    Args:
        boundary (bytes): The multipart boundary marker to detect
    """

    def __init__(self, boundary: bytes):
        """
        Initialize boundary detector with boundary pattern and optional cached line ending.

        Args:
            boundary (bytes): The multipart boundary marker to detect
        """
        self._boundary = boundary

    def find_boundary_position(self, content_reader: BufferedContentReader) -> int:
        """
        Find boundary position using content reader abstraction.

        Args:
            content_reader (BufferedContentReader): Content reader to search in

        Returns:
            int: Position of boundary within buffer, -1 if not found
        """
        return content_reader.find_pattern(self._boundary)

    def get_safe_content_size(self, content_reader: BufferedContentReader) -> int:
        """
        Get size of content that can be safely consumed without risking boundary split.

        This ensures we don't consume data that might contain a partial boundary
        that spans across buffer chunks.

        Args:
            content_reader (BufferedContentReader): Content reader to analyze

        Returns:
            int: Size of content consumable without a boundary split
        """
        return max(0, content_reader.get_buffer_size() - len(self._boundary) - 8)

    def get_boundary_size(self) -> int:
        """
        Get the size of the boundary pattern.

        Returns:
            int: Size of boundary in bytes
        """
        return len(self._boundary)

    def get_boundary(self) -> bytes:
        """
        Get the boundary pattern.

        Returns:
            bytes: The boundary pattern
        """
        return self._boundary
