#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional

from aistore.sdk.const import WHITESPACE_CHARS


class SlidingWindowBuffer:
    """
    High-performance sliding window buffer optimized for streaming data processing.

    The buffer automatically manages memory by sliding the window when it reaches
    capacity, keeping only the most recent data plus a configurable overlap for
    cross-chunk pattern detection.

    Args:
        max_size (int): Maximum logical size before sliding window activation
        boundary_size (int): Size of boundary pattern for overlap calculations

    Notes:
        ```
        buffer: [discarded data][----valid data----][unused space]
                                ^start_pos        ^end_pos
        ```
        This class is not thread-safe. Concurrent access requires external
        synchronization.
    """

    def __init__(self, max_size: int, boundary_size: int):
        """
        Initialize sliding window buffer with size limits and boundary considerations.

        Args:
            max_size (int): Maximum logical size before sliding window activation
            boundary_size (int): Size of boundary pattern for overlap calculations
        """
        self.max_size = max_size
        self.boundary_size = boundary_size
        # Allocate max_size mutable buffer
        self.buffer = bytearray(max_size)

        # Logical start and end positions of buffer
        self.start_pos = 0
        self.end_pos = 0
        self.total_processed = 0

    def append(self, data: bytes) -> None:
        """
        Append data to buffer with automatic sliding window management and compaction.

        Args:
            data (bytes): Data to append to the buffer
        """
        data_len = len(data)

        # Slide window if buffer gets too large
        current_size = self.end_pos - self.start_pos
        if current_size + data_len > self.max_size:
            # Keep overlap for boundary detection spanning chunks
            # 2x boundary_size accounts for CRLF + "--" prefix/suffix + boundary string
            overlap = self.boundary_size * 2

            # Ensure we do not try to remove more data than we have
            bytes_to_remove = max(0, current_size - overlap)

            self.start_pos += bytes_to_remove
            self.total_processed += bytes_to_remove

        # After sliding, ensure we still have valid data
        if self.start_pos >= self.end_pos:
            # Edge case: sliding removed all data
            self.start_pos = self.end_pos = 0

        # Check if we need to compact buffer
        if self.end_pos + data_len > len(self.buffer):
            self._compact_buffer()

            # If still not enough space after compacting, expand buffer
            if self.end_pos + data_len > len(self.buffer):
                new_size = max(len(self.buffer) * 2, self.end_pos + data_len)
                new_buffer = bytearray(new_size)
                new_buffer[: self.end_pos] = memoryview(self.buffer)[: self.end_pos]
                self.buffer = new_buffer

        # Copy data directly into buffer
        self.end_pos += data_len
        self.buffer[self.end_pos - data_len : self.end_pos] = data

    def _compact_buffer(self) -> None:
        """Compact buffer by moving valid data to beginning and updating position tracking."""
        current_data_len = self.end_pos - self.start_pos
        if current_data_len > 0 and self.start_pos > 0:
            # Use memoryview for efficient copying
            source = memoryview(self.buffer)[self.start_pos : self.end_pos]
            self.buffer[:current_data_len] = source

            self.total_processed += self.start_pos
            self.start_pos = 0
            self.end_pos = current_data_len

    def find(self, pattern: bytes, start: int = 0) -> int:
        """
        Find pattern in buffer and return absolute position in stream.

        Args:
            pattern (bytes): Pattern to search for
            start (int): Starting offset for search. Defaults to 0.

        Returns:
            int: Absolute position if found, -1 if not found
        """
        relative_pos = self.find_relative(pattern, start)
        return (
            self.total_processed + self.start_pos + relative_pos
            if relative_pos != -1
            else -1
        )

    def find_relative(self, pattern: bytes, start: int = 0) -> int:
        """
        Find pattern in buffer and return position relative to current buffer window.

        Args:
            pattern (bytes): Pattern to search for
            start (int): Starting offset for search. Defaults to 0.

        Returns:
            int: Relative position if found, -1 if not found
        """
        search_start = self.start_pos + start
        if search_start >= self.end_pos:
            return -1

        # Direct find - position is relative to entire buffer
        pos = self.buffer.find(pattern, search_start, self.end_pos)

        if pos != -1:
            # Convert to relative position
            return pos - self.start_pos
        return -1

    def _get_data(self, start: int = 0, end: Optional[int] = None) -> memoryview:
        """
        Get a data slice without copying using memoryview for efficiency.

        WARNING: The returned memoryview is only valid until the next call to
        append(), consume(), or any other method that modifies the buffer.
        For long-term storage, use get_data_bytes() instead.

        Safe usage pattern:
            ```
            view = self._get_data()
            data = view.tobytes()  # Immediately copy
            # Don't use 'view' after this point
            ```

        Args:
            start (int): Starting position. Defaults to 0.
            end (int, optional): Ending position, None for end of buffer.

        Returns:
            memoryview: Memory view of requested data range (TEMPORARY)
        """
        actual_start = self.start_pos + start
        if end is None:
            actual_end = self.end_pos
        else:
            actual_end = self.start_pos + end

        actual_end = min(actual_end, self.end_pos)
        actual_start = min(actual_start, self.end_pos)

        return memoryview(self.buffer)[actual_start:actual_end]

    def get_data_bytes(self, start: int = 0, end: Optional[int] = None) -> bytes:
        """
        Get data as bytes by creating a copy of the requested range.

        Args:
            start (int): Starting position. Defaults to 0.
            end (int, optional): Ending position, None for end of buffer

        Returns:
            bytes: Copy of requested data range
        """
        return self._get_data(start, end).tobytes()

    def consume(self, length: int) -> bytes:
        """
        Consume and return data from start of buffer, advancing the window.

        Args:
            length (int): Number of bytes to consume

        Returns:
            bytes: Consumed data
        """
        if length <= 0:
            return b""

        actual_length = min(length, self.end_pos - self.start_pos)

        self.start_pos += actual_length
        self.total_processed += actual_length

        return memoryview(self.buffer)[
            self.start_pos - actual_length : self.start_pos
        ].tobytes()

    def skip_leading_whitespace(self) -> int:
        """
        Skip leading whitespace characters and return count of bytes skipped.

        Returns:
            int: Number of whitespace bytes that were skipped and consumed
        """
        buffer_view = self._get_data()
        skip_count = 0

        for byte in buffer_view:
            if byte in WHITESPACE_CHARS:
                skip_count += 1
            else:
                break

        if skip_count > 0:
            self.consume(skip_count)

        return skip_count

    def __len__(self) -> int:
        """
        Return current logical length of valid data in buffer.

        Returns:
            int: Number of bytes of valid data
        """
        return self.end_pos - self.start_pos

    def clear(self) -> None:
        """Clear the buffer and update total processed byte counter."""
        self.total_processed += self.end_pos - self.start_pos
        self.start_pos = 0
        self.end_pos = 0
