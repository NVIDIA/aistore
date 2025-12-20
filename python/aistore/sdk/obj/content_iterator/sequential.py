#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Generator

from aistore.sdk.obj.content_iterator.base import BaseContentIterProvider


# pylint: disable=too-few-public-methods
class ContentIterProvider(BaseContentIterProvider):
    """
    Provide an iterator to open an HTTP response stream and read chunks of object content.

    Args:
        client (ObjectClient): Client for accessing contents of an individual object.
        chunk_size (int): Size of each chunk of data yielded from the response stream.
    """

    def create_iter(self, offset: int = 0) -> Generator[bytes, None, None]:
        """
        Create an iterator over the object content, applying an optional offset.

        Args:
            offset (int, optional): The offset in bytes to apply. If not provided, no offset
                                    is applied.

        Yields:
            bytes: Chunks of the object's content.
        """
        stream = self._client.get(stream=True, offset=offset)
        try:
            yield from stream.iter_content(chunk_size=self._chunk_size)
        finally:
            stream.close()
