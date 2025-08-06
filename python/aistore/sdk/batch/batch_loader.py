#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Generator, Tuple, Union, Any
from io import BytesIO

from aistore.sdk.request_client import RequestClient
from aistore.sdk.batch.batch_response import (
    BatchResponse,
    BatchResponseItem,
)
from aistore.sdk.batch.batch_request import BatchRequest
from aistore.sdk.batch.extractor.extractor_manager import ExtractorManager
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
        self._extractor_manager = ExtractorManager()

    def get_batch(
        self,
        batch_request: BatchRequest,
        return_raw: bool = False,
        decode_as_stream: bool = False,
    ) -> Union[
        Generator[Tuple[BatchResponseItem, bytes], None, None], Union[BytesIO, Any]
    ]:
        """
        Execute a batch request and yield (filename, content) tuples.

        If an extractor is provided, then this function returns a generator. Generators include
        3 types: the yield type, send type, and return type. We don't send or return, so the
        values are None, None. `ArchiveStreamExtractor` is provided by default.

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

        Args:
            batch_request (BatchRequest): Batch request detailing which objects to load
                and how to load them.
            return_raw (bool): If True, then raw HTTPResponse stream containing batch contents
                is returned. Else, stream is decoded/extracted and batch items are yielded.
                Defaults to False.
            decode_as_stream (bool): If True and `BatchRequest.streaming=False`, then
                the corresponding multipart response is decoded on the fly rather than loaded
                 into memory. Defaults to False.

        Returns:
            Union[Generator[Tuple[BatchResponseItem, bytes], None, None], Union[BytesIO, Any]]:
                If return_raw=True, returns raw HTTPResponse stream.
                Otherwise, returns a Generator that yields (BatchResponseItem, bytes) tuples.

        Raises:
            ValueError: If BatchRequest is None or empty
            Exception: If decoding the multipart batch response fails
        """
        if not batch_request or batch_request.is_empty():
            raise ValueError("Batch request must not be empty")

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

        # Returns raw batch stream, user must close
        if return_raw:
            return response.raw

        extractor = self._extractor_manager.get_extractor(batch_request.output_format)

        if batch_request.streaming:
            # Streaming mode: process response as stream
            # Streaming mode does not yield response metadata, pass None
            # We will close response after extracting in extractor
            return extractor.extract(response, response.raw, batch_request, None)

        # Non-streaming mode: expect multipart response
        try:
            decoder = MultipartDecoder(parse_as_stream=decode_as_stream)

            parts_iter = decoder.decode(response)

            if decode_as_stream:
                # BodyStreamIter, need to read fully to decode
                metadata_body = next(parts_iter)[1].read()
            else:
                metadata_body = next(parts_iter)[1]

            # Get metadata json (part 1)
            batch_response = BatchResponse.from_json(
                metadata_body.decode(decoder.encoding)
            )

            if decode_as_stream:
                # Archive (part 2) is BodyStreamIter now
                data_stream = next(parts_iter)[1]
            else:
                # Load archive (part 2) into memory buffer for non-streaming mode
                data_stream = BytesIO(next(parts_iter)[1])

        except Exception as e:
            # Log that we failed to decode
            logger.error("Failed to decode multipart batch response: %s", str(e))

            # We can close connection if we fail at point
            response.close()
            raise e

        # # Else, we'll close after extracting in the extractor
        return extractor.extract(response, data_stream, batch_request, batch_response)
