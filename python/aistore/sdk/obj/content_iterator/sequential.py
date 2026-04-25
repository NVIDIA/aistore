#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

from typing import Generator

from aistore.sdk.const import HEADER_CONTENT_ENCODING, HEADER_CONTENT_LENGTH
from aistore.sdk.obj.content_iterator.base import BaseContentIterProvider


# pylint: disable=too-few-public-methods
class ContentIterProvider(BaseContentIterProvider):
    """
    Provide an iterator to open an HTTP response stream and read chunks of object content.

    Args:
        client (ObjectClient): Client for accessing contents of an individual object.
        chunk_size (int): Size of each chunk of data yielded from the response stream.
    """

    def read_all(self) -> bytes:
        """Read all object content in a single non-streamed request."""
        return self._client.get(stream=False).content

    def create_iter(self, offset: int = 0) -> Generator[bytes, None, None]:
        """
        Create an iterator over the object content, applying an optional offset.

        Args:
            offset (int, optional): The offset in bytes to apply. If not provided, no offset
                                    is applied.

        Yields:
            bytes: Chunks of the object's content.
        """
        self._expected_end_position = None
        stream = self._client.get(stream=True, offset=offset)
        # Per RFC 9110 §8.4 (https://www.rfc-editor.org/rfc/rfc9110.html#section-8.4),
        # when Content-Encoding is non-identity the representation metadata
        # (Content-Length included) describes the *coded* form, while
        # iter_content yields decoded bytes — so the two would not match
        # and every successful read would look like a short EOF. Skip
        # tracking in that case.
        encoding = stream.headers.get(HEADER_CONTENT_ENCODING, "identity").lower()
        if encoding in ("", "identity"):
            content_length = stream.headers.get(HEADER_CONTENT_LENGTH)
            if content_length is not None:
                try:
                    self._expected_end_position = offset + int(content_length)
                except (TypeError, ValueError):
                    self._expected_end_position = None
        try:
            yield from stream.iter_content(chunk_size=self._chunk_size)
        finally:
            stream.close()
