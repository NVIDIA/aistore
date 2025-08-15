#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Iterator, Tuple

from aistore.sdk.const import WIN_LINE_END, UNIX_LINE_END
from aistore.sdk.utils import get_logger
from aistore.sdk.batch.multipart.multipart_stream_buffer import MultipartStreamBuffer
from aistore.sdk.batch.multipart.body_stream_reader import BodyStreamReader

logger = get_logger(__name__)


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

        # Initialize buffer reader
        self._stream_buffer = MultipartStreamBuffer(
            content_iter, boundary, max_buffer_size
        )

        self._first_boundary_found = False

        # Line ending in response
        self._cached_line_end = None

    def get_next_part(self) -> Optional[Tuple[bytes, BodyStreamReader]]:
        """
        Get the next part without loading it completely into memory.

        Returns:
            Optional[Tuple[bytes, BodyStreamReader]]: If part exists, returns tuple of headers and body stream
        """
        if self._stream_buffer.is_exhausted():
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
        body_stream = BodyStreamReader(self._stream_buffer)

        return headers, body_stream

    def _locate_first_boundary(self) -> bool:
        """
        Find the first boundary marker using the optimized buffer reader.

        Returns:
            bool: True if boundary found, False if exhausted without finding boundary
        """
        min_size = len(self._boundary) * 2

        while self._stream_buffer.has_data():
            # Use the optimized ensure_data_available method
            if not self._stream_buffer.ensure_data_available(min_size):
                return False

            # Find boundary position
            boundary_pos = self._stream_buffer.find_pattern(self._boundary)
            if boundary_pos != -1:
                # Consume everything up to and including the boundary
                self._stream_buffer.consume_data(
                    boundary_pos + self._stream_buffer.get_boundary_size()
                )

                # Handle boundary-specific cleanup
                self._stream_buffer.skip_whitespace()

                self._first_boundary_found = True
                return True
            break

        return False

    def _extract_headers(self) -> Optional[bytes]:
        """
        Extract headers using the optimized content reader abstraction.

        Returns:
            Optional[bytes]: Headers as bytes if found, None if stream exhausted
        """
        while self._stream_buffer.has_data():
            headers_end, detected_line_end = -1, None

            # Single ensure_data_available call
            self._stream_buffer.ensure_data_available()

            for line_end in (WIN_LINE_END, UNIX_LINE_END):
                pos = self._stream_buffer.find_pattern(line_end)
                if pos != -1:
                    headers_end, detected_line_end = pos, line_end

            if headers_end != -1:
                # Extract headers
                headers = self._stream_buffer.get_data_slice(0, headers_end)

                # Cache the detected line ending (no redundant buffer scan)
                if not self._cached_line_end:
                    self._cached_line_end = detected_line_end

                # Consume headers + line ending
                self._stream_buffer.consume_data(
                    headers_end + len(self._cached_line_end)
                )
                return headers

            if not self._stream_buffer.ensure_data_available():
                break

        return None
