#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import zipfile
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
from aistore.sdk.const import EXT_ZIP
from aistore.sdk.utils import get_logger


logger = get_logger(__name__)


class ZipStreamExtractor(ArchiveStreamExtractor):
    """
    Class for extracting batch .zip streams from AIStore.

    Integrates with BatchRequest/BatchResponse objects to provide proper metadata mapping.
    """

    _supported_fmts = (EXT_ZIP,)

    @override
    def extract(
        self,
        response: Response,
        data_stream: Union[BytesIO, Any],
        batch_request: BatchRequest,
        batch_response: Optional[BatchResponse] = None,
    ) -> Generator[Tuple[BatchResponseItem, bytes], None, None]:
        """
        Extract from zip archive stream and yield individual file contents.

        Uses regular zipfile library which requires entire zip to be loaded in memory.

        Args:
            response (Response): HTTP response object containing connection for stream
            data_stream (Union[BytesIO, Any]): Zip archive data stream with read() method
            batch_request (BatchRequest): Request that fetched the archive
            batch_response (Optional[BatchResponse]): Provided if request is not streaming

        Yields:
            Tuple[BatchResponseItem, bytes]: Response and file content in bytes

        Raises:
            RuntimeError: If zip stream extraction or file extraction fails
        """
        resp_index = 0

        # If not file-like stream, load into mem first
        if not hasattr(data_stream, "read"):
            data_stream = BytesIO(data_stream)
        # If streaming, then raw http stream needs to be loaded
        elif batch_request.streaming:
            data_stream = BytesIO(data_stream.read())

        try:
            # Read entire stream into memory for zipfile
            with zipfile.ZipFile(data_stream, "r") as zip_file:
                for file_info in zip_file.infolist():
                    if file_info.is_dir():
                        continue

                    try:
                        # Get response item (metadata)
                        response_item = BatchResponseItem.from_request_or_response(
                            index=resp_index,
                            batch_request=batch_request,
                            batch_response=batch_response,
                        )

                        # Check for missing files
                        response_item.is_missing = response_item.is_file_missing(
                            filename=file_info.filename
                        )

                        # Update item index in batch
                        resp_index += 1

                        # Extract and yield metadata + file content
                        yield response_item, zip_file.read(file_info.filename)

                    except (
                        zipfile.BadZipFile,
                        zipfile.LargeZipFile,
                        KeyError,
                    ) as exception:
                        if batch_request.continue_on_err:
                            logger.error(
                                "Failed to extract %s: %s",
                                file_info.filename,
                                exception,
                            )
                            continue

                        raise RuntimeError(
                            f"Failed to extract file {file_info.filename}: {exception}"
                        ) from exception

        except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError) as e:
            raise RuntimeError(f"Failed to extract ZIP stream: {e}") from e

        finally:
            # Finally, if we are done yielding all items or error, close connection
            response.close()
