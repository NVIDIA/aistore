#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Generator

from aistore.sdk.obj.object_client import ObjectClient


# pylint: disable=too-few-public-methods
class ContentIterProvider:
    """
    Provide an iterator to open an HTTP response stream and read chunks of object content.

    Args:
        client (ObjectClient): Client for accessing contents of an individual object.
        chunk_size (int): Size of each chunk of data yielded from the response stream.
    """

    def __init__(self, client: ObjectClient, chunk_size: int):
        self._client = client
        self._chunk_size = chunk_size

    @property
    def client(self) -> ObjectClient:
        """
        Get the client associated with this content iterator.

        Returns:
            ObjectClient: The client used to access object content.
        """
        return self._client

    def create_iter(self, offset: int = 0) -> Generator[bytes, None, None]:
        """
        Create an iterator over the object content, applying an optional offset.

        Args:
            offset (int, optional): The offset in bytes to apply. If not provided, no offset
                                    is applied.

        Returns:
            Generator[bytes, None, None]: An iterator over chunks of the object's content.
        """
        stream = self._client.get(stream=True, offset=offset)
        try:
            yield from stream.iter_content(chunk_size=self._chunk_size)
        finally:
            stream.close()
