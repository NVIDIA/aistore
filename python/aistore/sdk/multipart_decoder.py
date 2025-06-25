#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Iterator, Tuple
from re import search, IGNORECASE
from requests.models import Response

from aistore.sdk.utils import get_logger
from aistore.sdk.const import (
    MULTIPART_MARKER,
    WIN_LINE_END,
    UNIX_LINE_END,
    UTF_ENCODING,
)


logger = get_logger(f"{__name__}")


# pylint: disable=too-few-public-methods
class MultipartDecoder:
    """
    A multipart decoder for parsing multipart HTTP responses.

    Handles boundary extraction, content parsing, and data extraction from individual parts.
    The decoder supports both Windows (\\r\\n) and Unix (\\n) line endings and gracefully
    handles malformed content by skipping invalid parts.

    Args:
        encoding (str, optional): Character encoding to use. Defaults to "utf-8".

    Example:
        >>> decoder = MultipartDecoder()
        >>> response = requests.get("https://example.com/multipart-endpoint")
        >>> parts_iter = decoder.decode_multipart(response)
        >>> for headers, body in parts_iter:
        ...     print(f"Headers: {headers}, Body size: {len(body)} bytes")
    """

    def __init__(self, encoding: str = UTF_ENCODING):
        """
        Initialize MultipartDecoder with specified encoding.

        Args:
            encoding (str, optional): Character encoding to use. Defaults to "utf-8".
        """
        self.encoding = encoding

    def decode_multipart(self, response: Response) -> Iterator[Tuple[bytes, bytes]]:
        """
        Decode contents of a multipart HTTP response.

        Args:
            response (Response): HTTP response object containing multipart data

        Returns:
            Iterator[Tuple[bytes, bytes]]: Iterator over extracted body parts containing headers and body
        """
        # Validate content type
        content_type = response.headers.get("Content-Type", "")
        if "multipart" not in content_type.lower():
            raise ValueError("Response is not of multipart content type")

        # Extract boundary from content type header
        boundary = self._extract_boundary(content_type)

        yield from self._parse_multipart_content(boundary, response.content)

    def _extract_boundary(self, content_type: str) -> Optional[str]:
        """
        Extract the multipart boundary from the Content-Type header.

        Args:
            content_type (str): Content-Type header value

        Returns:
            str: Boundary identifier separating multipart sections

        Raises:
            ValueError: If boundary not found in Content-Type header
        """
        # Match boundary format: boundary=<BOUNDARY-ID>
        match = search(r"boundary=([^;,\s]+)", content_type, IGNORECASE)

        if match is None:
            raise ValueError("Boundary not found in Content-Type header")

        # Remove quotes if present
        return match.group(1).strip("\"'")

    def _parse_multipart_content(
        self, boundary: str, content: bytes
    ) -> Iterator[Tuple[bytes, bytes]]:
        """
        Parse multipart content and extract data from each body part.

        Args:
            boundary (str): Boundary identifier used to separate body parts
            content (bytes): Raw multipart content to parse

        Returns:
            Iterator[Tuple[bytes, bytes]]: Iterator over extracted body part header and data
        """
        # Create delimiter: -- + boundary
        boundary_encoded = boundary.encode(self.encoding)
        delimiter = MULTIPART_MARKER + boundary_encoded

        # Split content by delimiter
        split_content = content.split(delimiter)

        # Skip first part (preamble) and process remaining parts
        for part in split_content[1:]:
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
                "or Unix-style line endings %s.",
                WIN_LINE_END,
                UNIX_LINE_END,
            )
            return None

        # Split headers from body
        try:

            # Headers and body are separated by double line ending
            sections = part.split(line_ending, 1)

            if len(sections) != 2:
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
