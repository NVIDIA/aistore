#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Iterator, Tuple

from aistore.sdk.const import WIN_LINE_END, UNIX_LINE_END
from aistore.sdk.utils import get_logger
from aistore.sdk.batch.sliding_window_buffer import SlidingWindowBuffer
from aistore.sdk.batch.buffered_content_reader import BufferedContentReader
from aistore.sdk.batch.multipart_boundary_detector import MultipartBoundaryDetector
from aistore.sdk.batch.body_stream_iterator import BodyStreamIterator

logger = get_logger(__name__)


# pylint: disable=too-few-public-methods
class StatefulStreamingParser:
    """
    A stateful streaming parser for efficiently processing multipart HTTP responses.

    The parser maintains state across multiple parts of a multipart response,
    automatically detecting boundaries, extracting headers, and providing streaming
    access to body content.

    Args:
        content_iter (Iterator[bytes]): Iterator yielding chunks of the HTTP response
        boundary (bytes): The multipart boundary marker (including leading dashes)
        max_buffer_size (int): Maximum size of the internal sliding window buffer

    Notes:
        This class is not thread-safe. Each instance should be used by a single
        thread or proper synchronization must be implemented by the caller.
    """

    def __init__(
        self,
        content_iter: Iterator[bytes],
        boundary: bytes,
        max_buffer_size: int,
    ):
        self._boundary = boundary
        self._max_buffer_size = max_buffer_size

        # Initialize components
        buffer = SlidingWindowBuffer(self._max_buffer_size, len(boundary))
        self._content_reader = BufferedContentReader(content_iter, buffer)
        self._boundary_detector = MultipartBoundaryDetector(boundary)

        self._first_boundary_found = False

        # Line ending in response
        self._cached_line_end = None

    def get_next_part(self) -> Optional[Tuple[bytes, BodyStreamIterator]]:
        """
        Get the next part without loading it completely into memory.

        Returns:
            Optional[Tuple[bytes, Iterator[bytes]]]: If part exists, returns tuple of headers and body stream
        """
        if self._content_reader.is_exhausted():
            return None

        # Find and skip to first boundary if not found yet
        if not self._first_boundary_found:
            if not self._locate_first_boundary():
                return None

        # Extract headers for current part
        headers = self._extract_headers()
        if headers is None:
            return None

        # Create body stream for the rest of this part
        body_stream = BodyStreamIterator(self._content_reader, self._boundary_detector)

        return headers, body_stream

    def _locate_first_boundary(self) -> bool:
        """
        Find the first boundary marker using the content reader abstraction.

        Returns:
            bool: True if boundary found, False if exhausted without finding boundary
        """
        min_size = len(self._boundary) * 2

        while self._content_reader.has_data():
            # Ensure we have enough data for reliable boundary detection
            if not self._content_reader.ensure_data_available(min_size):
                return False

            # Find boundary position through content reader
            boundary_pos = self._content_reader.find_pattern(self._boundary)
            if boundary_pos != -1:
                # Consume everything up to and including the boundary
                self._content_reader.consume_data(
                    boundary_pos + self._boundary_detector.get_boundary_size()
                )

                # Handle boundary-specific cleanup
                self._content_reader.skip_whitespace()

                self._first_boundary_found = True
                return True

            # Request more data through the reader
            if not self._content_reader.fill_buffer():
                return False

        return False

    def _extract_headers(self) -> Optional[bytes]:
        """
        Extract headers using the content reader abstraction until line ending found.

        Returns:
            Optional[bytes]: Headers as bytes if found, None if stream exhausted
        """
        while self._content_reader.has_data():
            headers_end = self._find_headers_end()
            if headers_end != -1:
                # Extract headers through content reader
                headers = self._content_reader.get_data_slice(0, headers_end)

                # Cache line ending if not already detected
                if not self._cached_line_end:
                    buffer_data = self._content_reader.get_data_slice()
                    self._cached_line_end = (
                        WIN_LINE_END if WIN_LINE_END in buffer_data else UNIX_LINE_END
                    )

                # Consume headers + line ending through content reader
                self._content_reader.consume_data(
                    headers_end + len(self._cached_line_end)
                )
                return headers

            # Only try to read more if we don't have enough data
            if not self._content_reader.fill_buffer():
                break

        return None

    def _find_headers_end(self) -> int:
        """Find where headers end using the content reader abstraction."""
        for line_end in (WIN_LINE_END, UNIX_LINE_END):
            pos = self._content_reader.find_pattern(line_end)
            if pos != -1:
                return pos
        return -1
