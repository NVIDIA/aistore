#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Iterator, Tuple, Union
from re import search, IGNORECASE
from requests.models import Response

from aistore.sdk.utils import get_logger
from aistore.sdk.const import (
    MULTIPART_MARKER,
    WIN_LINE_END,
    UNIX_LINE_END,
    UTF_ENCODING,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_BUFFER_SIZE,
    HTTP_BOUNDARY_REGEX,
)
from aistore.sdk.batch.multipart.stateful_streaming_parser import (
    StatefulStreamingParser,
)
from aistore.sdk.batch.errors import MultipartDecodeError
from aistore.sdk.batch.multipart.body_stream_reader import BodyStreamReader

logger = get_logger(__name__)


# pylint: disable=too-few-public-methods
class MultipartDecoder:
    """
    A multipart decoder for parsing multipart HTTP responses.

    Handles boundary extraction, content parsing, and data extraction from individual parts.
    The decoder supports both Windows (\\r\\n) and Unix (\\n) line endings and gracefully
    handles malformed content by skipping invalid parts.

    Args:
        encoding (str, optional): Character encoding to use. Defaults to "utf-8".
        parse_as_stream (bool, optional): If True, yields part body content as
            an iterator of chunks that can be directly read from. If False,
            yields entire content in memory as bytes. Defaults to False. Note that
            this functionality is in development, not fully tested, and subject to change.
        chunk_size (int, optional): Size of chunks when streaming. Defaults to 8192.
        max_buffer_size (int, optional): Maximum buffer size for sliding window. Defaults to 64KB.

    Example:
        >>> # Traditional usage (loads entire body into memory)
        >>> decoder = MultipartDecoder()
        >>> for headers, body in decoder.decode(response):
        ...     print(f"Headers: {headers}, Body size: {len(body)} bytes")

        >>> # Streaming usage (processes body in chunks)
        >>> decoder = MultipartDecoder(parse_as_stream=True)
        >>> for headers, body_stream in decoder.decode(response):
        ...     for chunk in body_stream:
        ...         process_chunk(chunk)
    """

    def __init__(
        self,
        encoding: str = UTF_ENCODING,
        parse_as_stream: bool = False,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE,
    ):
        """
        Initialize MultipartDecoder with specified options.

        Args:
            encoding (str, optional): Character encoding to use. Defaults to "utf-8".
            parse_as_stream (bool, optional): Enable parsing as stream mode. Defaults to False.
            chunk_size (int, optional): Size of chunks when streaming. Defaults to 32KiB.
            max_buffer_size (int, optional): Maximum buffer size for sliding window. Defaults to 64KiB.
        """
        self.encoding = encoding
        self.parse_as_stream = parse_as_stream
        self.chunk_size = chunk_size
        self.max_buffer_size = max_buffer_size

    def decode(
        self, response: Response
    ) -> Iterator[Tuple[bytes, Union[bytes, BodyStreamReader]]]:
        """
        Decode contents of a multipart HTTP response.

        Args:
            response (Response): HTTP response object containing multipart data

        Yields:
            Tuple[bytes, Union[bytes, BodyStreamReader]]: Extracted body parts.
                If parse_as_stream=False, yields (headers, body).
                If parse_as_stream=True, yields (headers, body_stream).
        """
        # Validate content type
        content_type = response.headers.get("Content-Type", "")
        if "multipart" not in content_type.lower():
            raise MultipartDecodeError("Response is not of multipart content type")

        # Extract boundary from content type header
        # Match boundary format: boundary=<BOUNDARY-ID>
        match = search(HTTP_BOUNDARY_REGEX, content_type, IGNORECASE)

        if match is None:
            raise ValueError("Boundary not found in Content-Type header")

        # Remove quotes if present
        boundary = match.group(1).strip("\"'")

        if self.parse_as_stream:
            yield from self._parse_content_streaming(response, boundary)
        else:
            yield from self._parse_content(response.content, boundary)

    def _parse_content(
        self,
        content: bytes,
        boundary: str,
    ) -> Iterator[Tuple[bytes, bytes]]:
        """
        Parse multipart content and extract data from each body part (non-streaming).

        Args:
            boundary (str): Boundary identifier used to separate body parts
            content (bytes): Raw multipart content to parse

        Returns:
            Iterator[Tuple[bytes, bytes]]: Iterator over extracted body part header and data
        """
        # Create delimiter: -- + boundary
        delimiter = MULTIPART_MARKER + boundary.encode(self.encoding)

        # Split content by delimiter
        # Skip first part (preamble) and process remaining parts
        for part in content.split(delimiter)[1:]:
            # Skip empty parts or end markers
            if not part.strip() or part.strip() == MULTIPART_MARKER:
                continue

            # Extract data from part (headers and body part)
            part_data = self._parse_part(part)
            if part_data:
                yield part_data

    def _parse_part(self, part: bytes) -> Optional[Tuple[bytes, bytes]]:
        """
        Parse individual multipart body part and extract headers and body.

        Args:
            part (bytes): Raw body part content including headers and body

        Returns:
            Optional[Tuple[bytes, bytes]]: Extracted headers and body, or None if parsing fails
        """
        # Determine line ending style
        if WIN_LINE_END in part:
            line_ending = WIN_LINE_END
        elif UNIX_LINE_END in part:
            line_ending = UNIX_LINE_END
        else:
            # No valid line ending found
            logger.warning(
                "Multipart sections must include either Windows-style line endings %s "
                "or Unix-style line endings %s. The part has ending: %s",
                WIN_LINE_END,
                UNIX_LINE_END,
                part or "",
            )
            return None

        # Split headers from body
        try:
            # Headers and body are separated by double line ending
            sections = part.split(line_ending, 1)

            # If splitting failed, then part is malformed
            if not sections or len(sections) == 1:
                logger.exception(
                    "Malformed part: expected header/body split\n%s",
                    part.decode(self.encoding, errors="replace"),
                )
                return None

            headers, body = sections
            return headers, body

        except (ValueError, IndexError):
            # Failed to parse part
            logger.exception(
                "Could not parse part by splitting headers from body \n%s",
                str(part),
            )
            return None

    def _parse_content_streaming(
        self, response: Response, boundary: str
    ) -> Iterator[Tuple[bytes, BodyStreamReader]]:
        """
        Use a streaming parser to start decoding incoming multipart stream.
        Only extracts headers in memory and streams the body content per part.
        """
        start_boundary = MULTIPART_MARKER + boundary.encode(self.encoding)

        # Create stateful parser using boundaries and chunk size
        state_parser = StatefulStreamingParser(
            response.iter_content(chunk_size=self.chunk_size),
            start_boundary,
            self.max_buffer_size,
        )

        # Yield each part as we encounter it
        while True:
            part_result = state_parser.get_next_part()
            if part_result is None:
                break

            # headers and body stream
            yield part_result
