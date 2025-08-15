#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Iterator, Optional

from aistore.sdk.batch.multipart.sliding_window_buffer import SlidingWindowBuffer
from aistore.sdk.const import (
    WHITESPACE_CHARS,
    UNIX_LINE_END,
    WIN_LINE_END,
)

BOUNDARY_MARGIN = 8


class MultipartStreamBuffer:
    """
    Manages reading content from an iterator and buffering it efficiently.

    This class handles the low-level details of reading from content iterators
    and managing the sliding window buffer. It provides an abstraction layer
    that prevents higher-level classes from directly manipulating the buffer.

    Args:
        content_iter (Iterator[bytes]): Iterator yielding chunks of content data
        boundary (bytes): The multipart boundary marker to detect
        max_buffer_size (int): Maximum size of the internal sliding window buffer
    """

    def __init__(
        self,
        content_iter: Iterator[bytes],
        boundary: bytes,
        max_buffer_size: int,
    ):
        self._content_iter = content_iter
        self._exhausted = False
        self._boundary = boundary
        self._max_buffer_size = max_buffer_size
        self._boundary_safety_margin = len(self._boundary) + BOUNDARY_MARGIN
        self._whitespace_bytes = bytes(WHITESPACE_CHARS) + WIN_LINE_END + UNIX_LINE_END

        self._buffer = SlidingWindowBuffer(self._max_buffer_size, len(self._boundary))

    @property
    def boundary(self) -> bytes:
        """
        Get the boundary pattern.

        Returns:
            bytes: The boundary pattern
        """
        return self._boundary

    def _fill_buffer_safely(self, min_size: int = 0) -> bool:
        """
        Fill buffer ensuring we have at least min_size bytes available,
        with boundary safety margin.

        This method ensures we always have enough data in the buffer to avoid
        boundary splits when consuming data.

        Args:
            min_size (int): Minimum buffer size required

        Returns:
            bool: True if sufficient data available, False if stream exhausted
        """
        # Calculate required buffer size using boundary safety margin
        required_size = max(min_size, self._boundary_safety_margin)

        while self.get_buffer_size() < required_size and not self._exhausted:
            if not self._read_next_chunk():
                break

        return self.get_buffer_size() >= min_size

    def _read_next_chunk(self) -> bool:
        """
        Read one chunk from content iterator into buffer.

        Returns:
            bool: True if chunk read successfully, False if iterator exhausted
        """
        if self._exhausted:
            return False

        try:
            chunk = next(self._content_iter)
            self._buffer.append(chunk)
            return True
        except StopIteration:
            self._exhausted = True
            return False

    def get_buffer_size(self) -> int:
        """
        Get current buffer size for safe content calculations.

        Returns:
            int: Current size of buffer
        """
        return len(self._buffer)

    def ensure_data_available(self, min_bytes: int = 0) -> bool:
        """
        Ensure at least min_bytes are available in buffer.

        Args:
            min_bytes (int): Minimum buffer size required. Defaults to 0.

        Returns:
            bool: True if sufficient buffer available, False if stream exhausted
        """
        return self._fill_buffer_safely(min_bytes)

    def has_data(self) -> bool:
        """
        Check if buffer has any data or can get more.

        Returns:
            bool: True if data is available or can be obtained
        """
        return self.get_buffer_size() > 0 or not self._exhausted

    def is_exhausted(self) -> bool:
        """
        Check if both stream and buffer are exhausted.

        Returns:
            bool: True if no more data can be obtained
        """
        return self._exhausted and self.get_buffer_size() == 0

    def find_pattern(self, pattern: bytes, start: int = 0) -> int:
        """
        Find pattern in current buffer data.

        Args:
            pattern (bytes): Pattern to search for
            start (int): Starting offset for search. Defaults to 0.

        Returns:
            int: Position of pattern relative to buffer start, -1 if not found
        """
        return self._buffer.find_relative(pattern, start)

    def get_data_slice(self, start: int = 0, end: Optional[int] = None) -> bytes:
        """
        Get a copy of data slice from buffer.

        Args:
            start (int): Starting position. Defaults to 0.
            end (Optional[int]): Ending position, None for end of buffer.

        Returns:
            bytes: Copy of requested data range
        """
        return self._buffer.get_data_bytes(start, end)

    def consume_data(self, length: int) -> bytes:
        """
        Consume and return data from buffer, ensuring boundary safety.

        This method ensures that after consumption, we maintain enough
        buffer data to avoid boundary splits.

        Args:
            length (int): Number of bytes to consume

        Returns:
            bytes: Consumed data
        """
        # Consume the requested data
        consumed = self._buffer.consume(length)

        # Ensure we maintain boundary safety margin after consumption
        if self._boundary and not self._exhausted:
            self._fill_buffer_safely()

        return consumed

    def skip_whitespace(self) -> int:
        """
        Skip leading whitespace in buffer.

        Returns:
            int: Number of whitespace bytes that were skipped
        """
        return self._buffer.skip_leading_whitespace()

    def consume_before_pattern(
        self, pos: int, pattern: bytes, clean_endings: bool = True
    ) -> bytes:
        """
        Consume content before a pattern at a known position and return it.

        Args:
            pos (int): Known position of the pattern in the buffer
            pattern (bytes): Pattern to search for in the buffer (for length calculation)
            clean_endings (bool): Whether to strip whitespace and line endings
                from consumed content. Defaults to True.

        Returns:
            bytes: Content found before the pattern, with optional cleaning applied.
                Returns empty bytes if position is invalid.
        """
        if pos == -1:
            return b""

        content = self._buffer.get_data_bytes(0, pos)
        if clean_endings:
            content = content.rstrip(self._whitespace_bytes)

        # Consume content + pattern
        self._buffer.consume(pos + len(pattern))

        # Maintain boundary safety margin after consumption
        if self._boundary and not self._exhausted:
            self._fill_buffer_safely()

        return content

    def read_until_boundary(self, clean_endings: bool = True) -> Iterator[bytes]:
        """
        Read and consume all data until the next boundary is found.

        Args:
            clean_endings (bool): Whether to strip whitespace and line endings
                from consumed content. Defaults to True.

        Yields:
            bytes: All content before the boundary, with optional cleaning applied.

        Raises:
            ValueError: If no boundary has been set for this reader
        """
        if self._boundary is None:
            raise ValueError("No boundary set for this content reader")

        while not self._exhausted:
            if not self._fill_buffer_safely():
                break

            boundary_pos = self.find_boundary_position()

            if boundary_pos != -1:
                # Found boundary - yield content before it and finish
                if boundary_pos > 0:
                    content = self.consume_before_pattern(
                        boundary_pos, self._boundary, clean_endings=clean_endings
                    )
                    if content:
                        yield content
                break

            # No boundary found - yield safe portion and continue
            safe_size = self.get_safe_content_size()
            if safe_size > 0:
                content = self.consume_data(safe_size)
                if content:
                    yield content
            else:
                if not self._read_next_chunk():
                    break

        # Handle remaining data if stream is exhausted
        if self._exhausted and self.get_buffer_size() > 0:
            remaining = self.get_data_slice()
            if clean_endings:
                remaining = remaining.rstrip(self._whitespace_bytes)
            self._buffer.consume(self.get_buffer_size())
            if remaining:
                yield remaining

    def find_boundary_position(self) -> int:
        """
        Find boundary position in current buffer.

        Returns:
            int: Position of boundary within buffer, -1 if not found or no boundary set

        Raises:
            ValueError: If no boundary has been set for this reader
        """
        if self._boundary is None:
            raise ValueError("No boundary set for this content reader")
        return self.find_pattern(self._boundary)

    def get_safe_content_size(self) -> int:
        """
        Get size of content that can be safely consumed without risking boundary split.

        This ensures we don't consume data that might contain a partial boundary
        that spans across buffer chunks.

        Returns:
            int: Size of content consumable without a boundary split.
                 Returns full buffer size if no boundary is set.
        """
        if self._boundary is None:
            return self.get_buffer_size()
        return max(0, self.get_buffer_size() - len(self._boundary) - 8)

    def get_boundary_size(self) -> int:
        """
        Get the size of the boundary pattern.

        Returns:
            int: Size of boundary in bytes, 0 if no boundary set
        """
        return len(self._boundary) if self._boundary is not None else 0

    def has_boundary(self) -> bool:
        """
        Check if a boundary has been configured for this reader.

        Returns:
            bool: True if boundary is set, False otherwise
        """
        return self._boundary is not None
