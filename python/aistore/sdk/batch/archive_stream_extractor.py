#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import tarfile
from typing import Generator, Tuple, Union, Any, Optional
from io import BytesIO

from aistore.sdk.batch.batch_response import (
    BatchResponse,
    BatchResponseItem,
)
from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.const import MISSING_DATA_PREFIX, EXT_TARGZ, EXT_TGZ, EXT_TAR
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


class ArchiveStreamExtractor:
    """
    Class for extracting batch archive streams from AIStore.

    Handles .tar, .tar.gz and .tgz archive formats and integrates with BatchRequest/BatchResponse
    objects to provide proper metadata mapping.
    """

    def __init__(self):
        super().__init__()
        self.supported_formats = (EXT_TARGZ, EXT_TGZ, EXT_TAR)

    def extract(
        self,
        data_stream: Union[BytesIO, Any],
        batch_request: BatchRequest,
        batch_response: Optional[BatchResponse] = None,
    ) -> Generator[Tuple[BatchResponseItem, bytes], None, None]:
        """
        Extract from archive stream and yield individual file contents.

        Sequentially streams the archive to avoid memory-intensive buffering.

        Args:
            data_stream (Union[BytesIO, Any]): Tar archive data stream or bytes
            batch_request (BatchRequest): Request that fetched the tar
            batch_response (Optional[BatchResponse]): Provided if request is not streaming

        Yields:
            Tuple[BatchResponseItem, bytes]: Response and file content in bytes

        Raises:
            RuntimeError: If stream extraction fails
        """
        # Decode based on output fmt type
        if self.supports_format(batch_request.output_format):
            return ArchiveStreamExtractor._extract_tar_stream(
                data_stream, batch_request, batch_response
            )

        # Default is tar output fmt so cannot enter this state
        raise ValueError(
            f"Unsupported output format type {batch_request.output_format}"
        )

    @staticmethod
    def _extract_tar_stream(
        data_stream: Union[BytesIO, Any],
        batch_request: BatchRequest,
        batch_response: Optional[BatchResponse] = None,
    ) -> Generator[Tuple[BatchResponseItem, bytes], None, None]:
        """
        Extract from tar archive stream and yield individual file contents.

        Sequentially reads the tar archive to avoid memory-intensive buffering.
        See https://docs.python.org/3/library/tarfile.html#tarfile.open

        Args:
            data_stream (Union[BytesIO, Any]): Tar archive data stream or bytes
            batch_request (BatchRequest): Request that fetched the tar
            batch_response (Optional[BatchResponse]): Provided if request is not streaming

        Yields:
            Tuple[BatchResponseItem, bytes]: Response and file content in bytes

        Raises:
            RuntimeError: If tar stream extraction fails
        """
        resp_index = 0
        try:
            with tarfile.open(fileobj=data_stream, mode="r|*") as tar_file:
                for tarinfo in tar_file:

                    if not tarinfo.isfile():
                        continue

                    try:
                        with tar_file.extractfile(tarinfo) as file:

                            if batch_request.streaming:
                                # If we are in streaming mode, then server does not return response
                                # We must construct a response using the original request data
                                response_item = BatchResponseItem.from_batch_request(
                                    batch_request, resp_index
                                )
                            else:
                                # In non streaming mode, the first multipart body part is the response metadata
                                # We must keep track of index to obtain the right batch request
                                response_item = batch_response.responses[resp_index]

                            resp_index += 1

                            # Missing files will be all in one place: under __404__/<Bucket>/<ObjName>
                            missing_path = f"{MISSING_DATA_PREFIX}/{response_item.bucket}/{response_item.obj_name}"

                            if tarinfo.name == missing_path:
                                response_item.is_missing = True

                            # Finally, yield response and file contents
                            yield response_item, file.read()

                    except tarfile.TarError as exception:

                        if batch_request.continue_on_err:
                            logger.error(
                                "Failed to extract %s: %s", tarinfo.name, exception
                            )
                            continue

                        raise exception

        except tarfile.TarError as e:
            raise RuntimeError(f"Failed to extract tar stream: {e}") from e

    def supports_format(self, output_format: str) -> bool:
        """
        Check if this decoder supports the given format type.

        Args:
            output_format (str): Format type to check

        Returns:
            bool: True if format is supported, False otherwise
        """
        return output_format.lower() in self.supported_formats
