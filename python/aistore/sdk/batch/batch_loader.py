#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, Generator, Tuple, Union, Any
from io import BytesIO

from aistore.sdk.request_client import RequestClient
from aistore.sdk.batch.batch_response import (
    BatchResponse,
    BatchResponseItem,
)
from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.batch.archive_stream_extractor import ArchiveStreamExtractor
from aistore.sdk.batch.multipart_decoder import MultipartDecoder
from aistore.sdk.const import (
    HTTP_METHOD_GET,
    JSON_CONTENT_TYPE,
    HEADER_CONTENT_TYPE,
    URL_PATH_GB,
    QPARAM_PROVIDER,
)
from aistore.sdk.utils import get_logger


logger = get_logger(__name__)


# pylint: disable=too-few-public-methods
class BatchLoader:
    """
    A high-performance batch data loader for retrieving multiple objects from AIStore clusters.

    BatchLoader enables efficient downloading and processing of data stored across multiple
    objects, archives, buckets, and providers within an AIStore cluster. It supports both
    streaming and non-streaming modes as well as automatic archive extraction.

    Note that the `BatchLoader` is in development, not fully tested, and subject to change.
    """

    def __init__(self, request_client: RequestClient):
        """
        Initialize BatchLoader with AIStore client.

        Args:
            request_client (RequestClient): RequestClient instance for making requests
        """
        self._request_client = request_client

    def get_batch(
        self,
        batch_request: BatchRequest,
        extractor: Optional[ArchiveStreamExtractor] = ArchiveStreamExtractor(),
        decoder: Optional[MultipartDecoder] = MultipartDecoder(parse_as_stream=False),
    ) -> Union[
        Generator[Tuple[BatchResponseItem, bytes], None, None], Union[BytesIO, Any]
    ]:
        """
        Execute a batch request and yield (filename, content) tuples.

        If an extractor is provided, then this function returns a generator. Generators include
        3 types: the yield type, send type, and return type. We don't send or return, so the
        values are None, None. `ArchiveStreamExtractor` is provided by default.

        If the extractor is set to None, then this function returns the raw archive stream
        from the batch request.

        For the decoder, `MultipartDecoder` is provided by default. If BatchRequest.streaming = False,
        then the cluster will return a multipart response with the below format:

            GetBatch Response (Multipart HTTP Response)
            ├── HTTP Headers
            │   ├── Content-Type: multipart/mixed; boundary=<boundary>
            │   └── Other standard HTTP headers
            │
            ├── Part 1: Metadata
            │   ├── Headers:
            │   │   ├── Content-Type: application/json
            │   │   ├── Content-Length: <size>
            │   │   └── Additional part-specific headers
            │   └── Content:
            │       └── JSON metadata (BatchResponse with object info)
            │
            └── Part 2: Archive Data
                ├── Headers:
                │   ├── Content-Type: application/octet-stream
                │   ├── Content-Length: <size>
                │   └── Additional part-specific headers
                └── Content:
                    └── Binary file contents (Archive)

        If the decoder is set to None, then this function returns the raw multipart response
        from the batch request. Note that the decoder `parse_as_stream` functionality is in
        development, not fully tested, and subject to change.

        Args:
            batch_request (BatchRequest): Batch request detailing which objects to load
                and how to load them.
            extractor (Optional[ArchiveStreamExtractor]): Extractor that handles file
                stream response. If None, raw stream is returned.
            decoder (Optional[MultipartDecoder]): Decoder that handles parsing the
                multipart response into body parts. If None and streaming is False,
                raw multipart response is returned.

        Yields:
            Tuple[BatchResponseItem, bytes]: Object name and corresponding data in bytes

        Returns:
            Union[Generator[Tuple[BatchResponseItem, bytes], None, None], Union[BytesIO, Any]]:
                Generator of response items and file content, or raw stream data

        Raises:
            ValueError: If BatchRequest is None or empty
        """
        if not batch_request or batch_request.is_empty():
            raise ValueError("Empty or missing BatchRequest")

        # Extract bucket name from the first request item
        # If this item is not present, then request will fail anyway
        # So we can always use the first item
        bucket_name, provider = batch_request.get_request_bck_name_prov()

        # AIS client has retry support
        response = self._request_client.request(
            method=HTTP_METHOD_GET,
            path=f"{URL_PATH_GB}/{bucket_name}",
            params={QPARAM_PROVIDER: provider},
            headers={HEADER_CONTENT_TYPE: JSON_CONTENT_TYPE},
            stream=True,
            json=batch_request.to_dict(),
        )

        data_stream = None

        if batch_request.streaming:
            # Streaming mode: process response as stream
            data_stream = response.raw
            batch_response = None
        else:

            # If decoder is None, then return raw multipart response
            if not decoder:
                return response.raw

            # Non-streaming mode: expect multipart response
            try:
                parts_iter = decoder.decode(response)

                if decoder.parse_as_stream:
                    # Get metadata json (part 1)
                    metadata_str = next(parts_iter)[1].read().decode(decoder.encoding)
                    batch_response = BatchResponse.from_json(metadata_str)

                    # Load archive (part 2) as file-like iterator
                    data_stream = next(parts_iter)[1]
                else:
                    # Get metadata json (part 1)
                    batch_response = BatchResponse.from_json(
                        next(parts_iter)[1].decode(decoder.encoding)
                    )

                    # Load archive (part 2) into memory buffer for non-streaming mode
                    data_stream = BytesIO(next(parts_iter)[1])
            finally:
                # Need to close in load into memory case
                if not decoder.parse_as_stream:
                    response.close()
                # Otherwise, either response will close when iterator is exhausted
                # or manually closed by user

        if extractor:
            return extractor.extract(data_stream, batch_request, batch_response)

        return data_stream
