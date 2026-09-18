#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generator, Optional, Union

from aistore.sdk.obj.content_iterator.buffer import ParallelBuffer
from aistore.sdk.obj.object_client import ObjectClient


@dataclass
class StreamBounds:
    """
    Byte positions of a single stream. A provider fills these in as it opens the stream,
    so they are meaningful only once that stream yields or ends.

    Attributes:
        start (int): Logical byte position at which the stream begins.
        expected_end (int, optional): Logical byte position at EOF, when response metadata
            makes it known.
    """

    start: int = 0
    expected_end: Optional[int] = None


class BaseContentIterProvider(ABC):
    """
    Abstract base class for content iterator providers.

    Args:
        client (ObjectClient): Client for accessing contents of an individual object.
        chunk_size (int): Size of each chunk of data yielded.
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

    @abstractmethod
    def read_all(self) -> Union[bytes, ParallelBuffer]:
        """Read all object content into memory and return it."""

    @abstractmethod
    def create_iter(
        self, offset: int = 0, bounds: Optional[StreamBounds] = None
    ) -> Generator[bytes, None, None]:
        """
        Create an iterator over the object content.

        Args:
            offset (int, optional): The offset in bytes to apply. Defaults to 0.
            bounds (StreamBounds, optional): Receives the byte positions of the created
                stream. One provider can back several concurrent streams, so each caller
                that tracks positions must pass its own.

        Yields:
            bytes: Chunks of the object's content.
        """
