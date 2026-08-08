#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Iterator, Tuple

from aistore.sdk.const import WIN_LINE_END, UNIX_LINE_END, MULTIPART_MARKER
from aistore.sdk.utils import get_logger
from aistore.sdk.batch.errors import MultipartDecodeError
from aistore.sdk.batch.multipart.multipart_stream_buffer import MultipartStreamBuffer
from aistore.sdk.batch.multipart.body_stream_reader import BodyStreamReader

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

        # Headers starting with "--" are the closing boundary suffix ("--") or
        # epilogue text that followed a closing boundary ending with \r\n\r\n or
        # \n\n because neither is a real MIME part
        if headers.startswith(MULTIPART_MARKER):
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

            # Boundary not in current buffer. Consume bytes that are far enough
            # from the buffer edge that no boundary can span across them
            safe_size = self._stream_buffer.get_safe_content_size()
            if safe_size > 0:
                self._stream_buffer.consume_data(safe_size)
            # If nothing is safe to consume, force-read a new chunk
            # if the stream is exhausted, the boundary will never arrive
            elif self._stream_buffer.force_read_chunk() == 0:
                break

        return False

    def _get_initial_header_bytes(self) -> int:
        """
        Return the number of header bytes already in the buffer, excluding any
        trailing terminator-prefix bytes (e.g. a trailing \\r that is the first
        byte of \\r\\n\\r\\n must not inflate the header size counter).

        Returns:
            int: Number of countable header bytes currently in the buffer
        """
        buf_content = self._stream_buffer.get_data_slice()
        prefix_to_subtract = 0
        for line_end in (WIN_LINE_END, UNIX_LINE_END):
            for prefix_len in range(len(line_end) - 1, 0, -1):
                if buf_content.endswith(line_end[:prefix_len]):
                    prefix_to_subtract = max(prefix_to_subtract, prefix_len)
                    break
        return self._stream_buffer.get_buffer_size() - prefix_to_subtract

    def _count_header_bytes_in_chunk(self, chunk: bytes) -> int:
        """
        Return the number of bytes in chunk that are header content, i.e. the
        bytes before the first terminator (WIN_LINE_END or UNIX_LINE_END).

        Also detects terminators that span the buffer-chunk boundary (e.g. the
        buffer ends with \\r\\n\\r and chunk starts with \\n) to avoid
        over-counting and triggering a spurious MultipartDecodeError.

        Args:
            chunk (bytes): The raw chunk peeked from the stream before appending

        Returns:
            int: Number of bytes in chunk that are header content (0 if the chunk
                starts with or completes a terminator)
        """
        overlap = max(len(WIN_LINE_END), len(UNIX_LINE_END)) - 1
        buf_size = self._stream_buffer.get_buffer_size()
        buf_tail = self._stream_buffer.get_data_slice(max(0, buf_size - overlap))
        combined = buf_tail + chunk[: overlap + 1]
        count = len(chunk)
        for line_end in (WIN_LINE_END, UNIX_LINE_END):
            pos = chunk.find(line_end)
            if pos != -1:
                count = min(count, pos)
            cross_pos = combined.find(line_end)
            if cross_pos != -1:
                count = min(count, max(0, cross_pos - len(buf_tail)))
        return count

    def _extract_headers(self) -> Optional[bytes]:
        """
        Extract headers using the optimized content reader abstraction.

        Returns:
            Optional[bytes]: Headers as bytes if found, None if stream exhausted

        Raises:
            MultipartDecodeError: If accumulated header bytes exceed max_buffer_size,
                or the terminator is found at a position exceeding max_buffer_size.
        """
        # Reset per-part state so a mixed-line-ending response doesn't carry
        # the wrong terminator length from a previous part into this one
        self._cached_line_end = None

        # Skip any leading whitespace (the \r\n or \n after the boundary line).
        # _locate_first_boundary() already does this for the first part
        self._stream_buffer.skip_whitespace()

        header_bytes_received = self._get_initial_header_bytes()

        while self._stream_buffer.has_data():
            headers_end, detected_line_end = -1, None

            # Take the earliest terminator position because body content may contain
            # \n\n, which would overwrite an earlier correct \r\n\r\n if we did
            # not guard with pos < headers_end.
            for line_end in (WIN_LINE_END, UNIX_LINE_END):
                pos = self._stream_buffer.find_pattern(line_end)
                if pos != -1 and (headers_end == -1 or pos < headers_end):
                    headers_end, detected_line_end = pos, line_end

            if headers_end != -1:
                if headers_end > self._max_buffer_size:
                    raise MultipartDecodeError(
                        f"Multipart header exceeds maximum buffer size ({self._max_buffer_size} bytes)"
                    )
                headers = self._stream_buffer.get_data_slice(0, headers_end)
                if not self._cached_line_end:
                    self._cached_line_end = detected_line_end
                self._stream_buffer.consume_data(
                    headers_end + len(self._cached_line_end)
                )
                return headers

            # No terminator in the current buffer so peek at the next chunk before
            # appending so we can count only header bytes (bytes before the terminator).
            # Body bytes that arrive in the same chunk as the terminator must not
            # contribute to the header size check
            chunk = self._stream_buffer.read_chunk()
            if chunk is None:
                break

            header_bytes_received += self._count_header_bytes_in_chunk(chunk)
            if header_bytes_received > self._max_buffer_size:
                raise MultipartDecodeError(
                    f"Multipart header exceeds maximum buffer size ({self._max_buffer_size} bytes)"
                )

            # no_slide=True preserves all buffered header bytes so sliding here
            # would silently truncate the returned header.
            self._stream_buffer.append_chunk(chunk, no_slide=True)

        # EOF without a header terminator. header_bytes_received may undercount
        # when the buffer ends with a partial terminator prefix (e.g. a lone \r
        # subtracted by _get_initial_header_bytes). Check the actual buffer size
        # to catch those off-by-one cases.
        if self._stream_buffer.get_buffer_size() > self._max_buffer_size:
            raise MultipartDecodeError(
                f"Multipart header exceeds maximum buffer size ({self._max_buffer_size} bytes)"
            )
        return None
