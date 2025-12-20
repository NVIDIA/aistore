#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from abc import ABC, abstractmethod
from typing import Generator

from aistore.sdk.obj.object_client import ObjectClient


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
    def create_iter(self, offset: int = 0) -> Generator[bytes, None, None]:
        """
        Create an iterator over the object content.

        Args:
            offset (int, optional): The offset in bytes to apply. Defaults to 0.

        Yields:
            bytes: Chunks of the object's content.
        """
