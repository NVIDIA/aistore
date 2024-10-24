#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Iterator

from aistore.sdk.obj.object_client import ObjectClient


# pylint: disable=too-few-public-methods
class ContentIterator:
    """
    Provide an iterator to open an HTTP response stream and read chunks of object content.

    Args:
        client (ObjectClient): Client for accessing contents of an individual object.
        chunk_size (int): Size of each chunk of data yielded from the response stream.
    """

    def __init__(self, client: ObjectClient, chunk_size: int):
        self._client = client
        self._chunk_size = chunk_size

    def iter_from_position(self, start_position: int = 0) -> Iterator[bytes]:
        """
        Make a request to get a stream from the provided object starting at a specific byte position
        and yield chunks of the stream content.

        Args:
            start_position (int): The byte position from which to start reading. Defaults to 0.

        Returns:
            Iterator[bytes]: An iterator over each chunk of bytes in the object starting from the specific position
        """
        stream = self._client.get(stream=True, start_position=start_position)
        try:
            yield from stream.iter_content(chunk_size=self._chunk_size)
        finally:
            stream.close()
