#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Iterator
from io import BufferedIOBase
from overrides import override

from aistore.sdk.const import WIN_LINE_END, UNIX_LINE_END, WHITESPACE_CHARS
from aistore.sdk.get_batch.buffered_content_reader import BufferedContentReader
from aistore.sdk.get_batch.multipart_boundary_detector import MultipartBoundaryDetector


class BodyStreamIterator(Iterator, BufferedIOBase):
    """
    Stream iterator representing body of part which can be read from.
    As such, this class extends both `Iterator` and `BufferedIOBase`.

    Yields:
        bytes: Chunks data read from the stream that are part of the body.
    """

    def __init__(
        self,
        content_reader: BufferedContentReader,
        boundary_detector: MultipartBoundaryDetector,
    ):
        """
        Initialize body stream iterator with reference to parser state.

        Args:
            content_reader (BufferedContentReader): Reader for buffering content from the stream
            boundary_detector (MultipartBoundaryDetector): Detector for finding multipart boundaries
        """
        self._content_reader = content_reader
        self._boundary_detector = boundary_detector
        self._eof = False
        self._read_buffer = bytearray()

    def __iter__(self) -> Iterator[bytes]:
        """
        Return self as iterator for body content chunks.

        Returns:
            Iterator[bytes]: Self as iterator
        """
        return self

    def __next__(self) -> bytes:
        """
        Reads chunks for the stream until a multipart boundary is reached.

        Returns:
            bytes: Data read from the stream.
        """
        while not self._eof:
            # Ensure we have data to work with
            if not self._content_reader.ensure_data_available():
                return self._handle_end_of_stream()

            # Look for boundary
            boundary_pos = self._boundary_detector.find_boundary_position(
                self._content_reader
            )

            if boundary_pos != -1:
                return self._read_until_boundary(boundary_pos)

            # No boundary found - yield safe portion
            content = self._try_yield_safe_portion()
            if content:
                return content

            # Need more data
            if not self._content_reader.fill_buffer():
                return self._handle_end_of_stream()

        raise StopIteration

    def _read_until_boundary(self, boundary_pos: int) -> bytes:
        """
        Handle case where boundary is found in buffer.

        Args:
            boundary_pos (int): Position where boundary is found

        Returns:
            bytes: Content after boundary has been removed
        """
        content = self._content_reader.consume_before_pattern(
            boundary_pos, self._boundary_detector.get_boundary(), clean_endings=True
        )

        self._eof = True

        if content:
            return content
        raise StopIteration

    def _try_yield_safe_portion(self) -> bytes:
        """
        Try to yield safe portion of buffer without risking boundary split.

        Returns:
            bytes: Content from safe portion of buffer
        """
        safe_size = self._boundary_detector.get_safe_content_size(self._content_reader)
        if safe_size > 0:
            return self._content_reader.consume_data(safe_size)
        return b""

    def _handle_end_of_stream(self) -> bytes:
        """
        Handle end of stream - return any remaining content.

        Returns:
            bytes: Any remaining content not consumed
        """
        remaining_data = self._content_reader.get_data_slice()

        if remaining_data:
            content = remaining_data.rstrip(
                bytes(WHITESPACE_CHARS) + WIN_LINE_END + UNIX_LINE_END
            )
            # Clear through content reader or boundary detector
            self._eof = True
            if content:
                return content

        self._eof = True
        raise StopIteration

    @override
    def read(self, size: int = -1) -> bytes:
        """
        Read up to size bytes from the stream.

        Args:
            size (int): Number of bytes to read. If size is omitted, None, or
                negative read as much as possible. Defaults to -1.

        Returns:
            bytes: Data read from the stream.
        """
        if self._eof and not self._read_buffer:
            return b""

        # If size is omitted, None, or negative read as much as possible
        if not size or size < 0:
            result = bytearray(self._read_buffer)
            self._read_buffer.clear()

            try:
                for chunk in self:
                    result.extend(chunk)
            except StopIteration:
                pass

            return bytes(result)

        # Read specific amount
        while len(self._read_buffer) < size and not self._eof:
            try:
                self._read_buffer.extend(next(self))
            except StopIteration:
                break

        if len(self._read_buffer) <= size:
            result = bytes(self._read_buffer)
            self._read_buffer.clear()
            return result

        result = bytes(self._read_buffer[:size])
        del self._read_buffer[:size]
        return result

    @override
    def readable(self) -> bool:
        """Return True if the stream is readable."""
        return not self._eof

    @override
    def close(self) -> None:
        """Close the iterator (but not underlying stream)."""
        self._eof = True
        self._read_buffer.clear()
