#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from io import BufferedIOBase
from overrides import override

from aistore.sdk.batch.multipart.multipart_stream_buffer import MultipartStreamBuffer


class BodyStreamReader(BufferedIOBase):
    """
    Stream reader that only reads the body portion of a part from a
    buffered multipart stream. This class extends BufferedIOBase and
    provides a read-only interface for consuming multipart body content
    until a boundary is encountered.
    """

    def __init__(self, buffer_reader: MultipartStreamBuffer):
        """
        Initialize body stream reader.

        Args:
            buffer_reader (MultipartStreamBuffer): Reader for buffering content from the stream
        """
        self._buffer_reader = buffer_reader
        self._eof = False
        # Cache content/iterator for efficient reading
        self._read_iterator = None
        self._cached_content = None

    @override
    def read(self, size: int = -1) -> bytes:
        """
        Read up to size bytes from the stream.

        For multipart streams, this respects logical boundaries and may return
        more data than requested to maintain boundary integrity.

        Args:
            size (int): Number of bytes to read. If size is omitted, None, or
                negative read as much as possible. Defaults to -1.

        Returns:
            bytes: Data read from the stream.
        """
        if self._eof and not self._cached_content:
            return b""

        # Initialize the iterator if not already done
        if not self._read_iterator:
            self._read_iterator = self._buffer_reader.read_until_boundary()
            self._iterator_exhausted = False

        # If size is omitted, None, or negative read as much as possible
        if not size or size < 0:
            result = bytearray()
            if self._cached_content:
                result.extend(self._cached_content)
                self._cached_content.clear()

            try:
                for chunk in self._read_iterator:
                    result.extend(chunk)
            except StopIteration:
                pass

            self._eof = True
            return bytes(result)

        # For specific size, accumulate chunks until we have enough
        if not self._cached_content:
            self._cached_content = bytearray()

        # Read more chunks until we have enough data
        while len(self._cached_content) < size and not self._iterator_exhausted:
            try:
                chunk = next(self._read_iterator)
                self._cached_content.extend(chunk)
            except StopIteration:
                self._iterator_exhausted = True
                self._eof = True
                break

        if not self._cached_content:
            return b""

        # Return requested amount or all available
        if len(self._cached_content) <= size:
            result = bytes(self._cached_content)
            self._cached_content.clear()
            return result

        # Return partial content
        result = bytes(self._cached_content[:size])
        # More efficient than slicing assignment
        del self._cached_content[:size]
        return result

    @override
    def readable(self) -> bool:
        """Return True if the stream is readable."""
        return not self._eof or bool(self._cached_content)

    @override
    def close(self) -> None:
        """Close the reader."""
        self._eof = True
        self._cached_content = None

    @override
    def seekable(self) -> bool:
        """Return False as this stream does not support seeking."""
        return False

    @override
    def writable(self) -> bool:
        """Return False as this stream is read-only."""
        return False
