#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Generator, Tuple, Union, Any, Optional
from io import BytesIO
from abc import ABC, abstractmethod
from requests import Response

from aistore.sdk.batch.batch_response import (
    BatchResponse,
    BatchResponseItem,
)
from aistore.sdk.batch.batch_request import BatchRequest


class ArchiveStreamExtractor(ABC):
    """
    Parent class for extracting batch archive streams from AIStore.

    Integrates with BatchRequest/BatchResponse objects to provide proper metadata mapping.
    """

    _supported_fmts = tuple()

    @abstractmethod
    def extract(
        self,
        response: Response,
        data_stream: Union[BytesIO, Any],
        batch_request: BatchRequest,
        batch_response: Optional[BatchResponse] = None,
    ) -> Generator[Tuple[BatchResponseItem, bytes], None, None]:
        """
        Extract from archive stream and yield individual file contents.

        Sequentially streams the archive to avoid memory-intensive buffering.

        Args:
            response (Response): HTTP response object containing connection for stream
            data_stream (Union[BytesIO, Any]): Archive data stream or bytes
            batch_request (BatchRequest): Request that fetched the archive
            batch_response (Optional[BatchResponse]): Provided if request is not streaming

        Yields:
            Tuple[BatchResponseItem, bytes]: Response and file content in bytes

        Raises:
            RuntimeError: If stream extraction fails
        """

    def get_supported_formats(self) -> Tuple[str, ...]:
        """
        Get formats supported by this extractor.

        Returns:
            Tuple[str]: Tuple of support formats
        """
        return self._supported_fmts
