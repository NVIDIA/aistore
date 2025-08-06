#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import tarfile
from typing import Generator, Tuple, Union, Any, Optional
from io import BytesIO
from requests import Response
from overrides import override

from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor
from aistore.sdk.batch.batch_response import (
    BatchResponse,
    BatchResponseItem,
)
from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.const import EXT_TARGZ, EXT_TGZ, EXT_TAR
from aistore.sdk.utils import get_logger


logger = get_logger(__name__)


class TarStreamExtractor(ArchiveStreamExtractor):
    """
    Class for extracting batch .tar, .tar.gz, and .tgz streams from AIStore.

    Integrates with BatchRequest/BatchResponse objects to provide proper metadata mapping.
    """

    _supported_fmts = (EXT_TARGZ, EXT_TGZ, EXT_TAR)

    @override
    def extract(
        self,
        response: Response,
        data_stream: Union[BytesIO, Any],
        batch_request: BatchRequest,
        batch_response: Optional[BatchResponse] = None,
    ) -> Generator[Tuple[BatchResponseItem, bytes], None, None]:
        """
        Extract from tar archive stream and yield individual file contents.

        Sequentially reads the tar archive to avoid memory-intensive buffering.
        See https://docs.python.org/3/library/tarfile.html#tarfile.open

        Args:
            response (Response): HTTP response object containing connection for stream
            data_stream (Union[BytesIO, Any]): Tar archive data stream or bytes
            batch_request (BatchRequest): Request that fetched the tar
            batch_response (Optional[BatchResponse]): Provided if request is not streaming

        Yields:
            Tuple[BatchResponseItem, bytes]: Response and file content in bytes

        Raises:
            RuntimeError: If tar stream extraction or file extraction fails
        """
        resp_index = 0
        try:
            with tarfile.open(fileobj=data_stream, mode="r|*") as tar_file:
                for tarinfo in tar_file:

                    if not tarinfo.isfile():
                        continue

                    try:
                        with tar_file.extractfile(tarinfo) as file:

                            # Get response item (metadata)
                            response_item = BatchResponseItem.from_request_or_response(
                                index=resp_index,
                                batch_request=batch_request,
                                batch_response=batch_response,
                            )

                            # Check for missing files
                            response_item.is_missing = response_item.is_file_missing(
                                filename=tarinfo.name
                            )

                            # Update item index in batch
                            resp_index += 1

                            # Finally, yield response and file contents
                            yield response_item, file.read()

                    except tarfile.TarError as exception:

                        if batch_request.continue_on_err:
                            logger.error(
                                "Failed to extract %s: %s", tarinfo.name, exception
                            )
                            continue

                        raise RuntimeError(
                            f"Failed to extract file {tarinfo.name}: {exception}"
                        ) from exception

        except tarfile.TarError as e:
            raise RuntimeError(f"Failed to extract tar stream: {e}") from e

        finally:
            # Finally, if we are done yielding all items or error, close connection
            response.close()
