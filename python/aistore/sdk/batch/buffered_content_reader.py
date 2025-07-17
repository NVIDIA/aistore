#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Iterator, Optional

from aistore.sdk.batch.sliding_window_buffer import SlidingWindowBuffer
from aistore.sdk.const import WHITESPACE_CHARS, UNIX_LINE_END, WIN_LINE_END


class BufferedContentReader:
    """
    Manages reading content from an iterator and buffering it efficiently.

    This class handles the low-level details of reading from content iterators
    and managing the sliding window buffer. It provides an abstraction layer
    that prevents higher-level classes from directly manipulating the buffer.

    Args:
        content_iter (Iterator[bytes]): Iterator yielding chunks of content data
        buffer (SlidingWindowBuffer): Sliding window buffer for efficient data management
    """

    def __init__(self, content_iter: Iterator[bytes], buffer: SlidingWindowBuffer):
        self._content_iter = content_iter
        self._buffer = buffer
        self._exhausted = False

    def fill_buffer(self) -> bool:
        """
        Read more chunks from content iterator into sliding window buffer.

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
        while self.get_buffer_size() < min_bytes and not self._exhausted:
            if not self.fill_buffer():
                return False
        return len(self._buffer) >= min_bytes

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
        Consume and return data from buffer.

        Args:
            length (int): Number of bytes to consume

        Returns:
            bytes: Consumed data
        """
        return self._buffer.consume(length)

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
            content = content.rstrip(
                bytes(WHITESPACE_CHARS) + WIN_LINE_END + UNIX_LINE_END
            )

        # Consume content + pattern
        self._buffer.consume(pos + len(pattern))
        return content
