#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import base64
from typing import Type, Tuple
from urllib.parse import urlparse, urlunparse

import cloudpickle
from aistore.sdk.etl.webserver.base_etl_server import ETLServer
from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.errors import InvalidPipelineError


def serialize_class(cls: Type[ETLServer], encoding: str = UTF_ENCODING) -> str:
    """
    Pickle and base64-encode a user-provided ETLServer subclass for transmission.

    Args:
        cls: A subclass of ETLServer to serialize.
        encoding: The string encoding for the Base64 payload.

    Returns:
        A Base64 string containing the pickled class.

    Raises:
        TypeError: If `cls` is not a subclass of ETLServer.
    """
    if not isinstance(cls, type) or not issubclass(cls, ETLServer):
        raise TypeError(f"{cls!r} is not a subclass of ETLServer")
    pickled = cloudpickle.dumps(cls)
    return base64.b64encode(pickled).decode(encoding)


def compose_etl_direct_put_url(direct_put_url: str, host_target: str) -> str:
    """
    Compose a direct PUT URL by taking the scheme and netloc from `direct_put_url`
    and the path from `host_target`.

    Args:
        direct_put_url (str): The destination node's direct PUT URL, including path and query.
        host_target (str): The base AIS target URL used as the scheme and path base.

    Returns:
        str: A complete direct PUT URL targeting the appropriate AIS node.
    """
    parsed_target = urlparse(direct_put_url)
    parsed_host = urlparse(host_target)
    return urlunparse(
        parsed_host._replace(
            netloc=parsed_target.netloc,
            path=parsed_host.path + parsed_target.path,
            query=parsed_target.query,  # pass xid on direct put for statistics
        )
    )


def parse_etl_pipeline(pipeline_header: str) -> Tuple[str, str]:
    """
    Parse ETL pipeline from header value with validation.

    Args:
        pipeline_header: Comma-separated pipeline URLs

    Returns:
        Tuple of (first_url, remaining_pipeline_header)
        where remaining_pipeline_header is comma-joined remaining URLs
        or empty string if no remaining stages

    Raises:
        InvalidPipelineError: If pipeline header is malformed
    """
    if not pipeline_header or not pipeline_header.strip():
        return "", ""

    # Validate basic format - check for empty entries
    pipeline_header = pipeline_header.strip()
    entries = [entry.strip() for entry in pipeline_header.split(",")]

    for entry in entries:
        if not entry:
            raise InvalidPipelineError("Pipeline header contains empty entry")

    # Find the first comma efficiently (after validation)
    comma_index = pipeline_header.find(",")

    if comma_index == -1:
        # No comma found, only one URL (already validated)
        return pipeline_header.strip(), ""

    # Extract first URL and remaining pipeline
    first_url = pipeline_header[:comma_index].strip()
    remaining_pipeline = pipeline_header[comma_index + 1 :].strip()

    return first_url, remaining_pipeline
