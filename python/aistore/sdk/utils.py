#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

import logging
from pathlib import Path
from typing import Callable, Iterator, Tuple, Type, TypeVar
from urllib.parse import urlparse

import braceexpand
import humanize

from msgspec import msgpack
import pydantic.tools
import requests
from pydantic import BaseModel, parse_raw_as
from requests import Response

from aistore.sdk.const import (
    UTF_ENCODING,
    HEADER_CONTENT_TYPE,
    MSGPACK_CONTENT_TYPE,
    DEFAULT_LOG_FORMAT,
)
from aistore.sdk.errors import (
    AISError,
    ErrBckNotFound,
    ErrRemoteBckNotFound,
    ErrBckAlreadyExists,
    ErrETLAlreadyExists,
)

T = TypeVar("T")


class HttpError(BaseModel):
    """
    Represents an error returned by the API.
    """

    status: int
    message: str = ""
    method: str = ""
    url_path: str = ""
    remote_addr: str = ""
    caller: str = ""
    node: str = ""


def raise_ais_error(text: str) -> None:
    """
    Raise an AIS error based on the response text.

    Args:
        text (str): The raw text of the API response containing error details.

    Raises:
        AISError: If the error doesn't match any specific conditions.
        ErrBckNotFound: If the error message indicates a missing bucket.
        ErrRemoteBckNotFound: If the error message indicates a missing remote bucket.
        ErrBckAlreadyExists: If the error message indicates a bucket already exists.
        ErrETLAlreadyExists: If the error message indicates an ETL already exists
    """
    err = pydantic.tools.parse_raw_as(HttpError, text)
    if 400 <= err.status < 500:
        if "does not exist" in err.message:
            if "cloud bucket" in err.message or "remote bucket" in err.message:
                raise ErrRemoteBckNotFound(err.status, err.message)
            if "bucket" in err.message:
                raise ErrBckNotFound(err.status, err.message)
        elif "already exists" in err.message:
            if "bucket" in err.message:
                raise ErrBckAlreadyExists(err.status, err.message)
            if "etl" in err.message:
                raise ErrETLAlreadyExists(err.status, err.message)

    raise AISError(err.status, err.message)


# pylint: disable=unused-variable
def handle_errors(
    resp: requests.Response, raise_error_fn: Callable[[str], None] = raise_ais_error
) -> None:
    """
    Error handling for requests made to the AIS Client

    Args:
        resp: requests.Response = Response received from the request
        raise_error_fn: Function that processes error text and raises appropriate exceptions.
    """
    error_text = resp.text
    if isinstance(resp.text, bytes):
        try:
            error_text = error_text.decode(UTF_ENCODING)
        except UnicodeDecodeError:
            error_text = error_text.decode("iso-8859-1")
    if error_text != "":
        raise_error_fn(error_text)
    resp.raise_for_status()


def probing_frequency(dur: int) -> float:
    """
    Given a timeout, return an interval to wait between retries

    Args:
        dur: Duration of timeout

    Returns:
        Frequency to probe
    """
    freq = min(dur / 8.0, 1.0)
    freq = max(dur / 64.0, freq)
    return max(freq, 0.1)


def read_file_bytes(filepath: str) -> bytes:
    """
    Given a filepath, read the content as bytes
    Args:
        filepath: Existing local filepath

    Returns: Raw bytes
    """
    with open(filepath, "rb") as reader:
        return reader.read()


def _check_path_exists(path: Path) -> None:
    if not path.exists():
        raise ValueError(f"Path: {path} does not exist")


def validate_file(path: str or Path) -> None:
    """
    Validate that a file exists and is a file
    Args:
        path (str or Path): Path to validate
    Raises:
        ValueError: If path does not exist or is not a file
    """
    if isinstance(path, str):
        path = Path(path)
    _check_path_exists(path)
    if not path.exists():
        raise ValueError(f"Path: {path} does not exist")
    if not path.is_file():
        raise ValueError(f"Path: {path} is a directory, not a file")


def validate_directory(path: str or Path) -> None:
    """
    Validate that a directory exists and is a directory
    Args:
        path (str or Path): Path to validate
    Raises:
        ValueError: If path does not exist or is not a directory
    """
    if isinstance(path, str):
        path = Path(path)
    _check_path_exists(path)
    if not path.is_dir():
        raise ValueError(f"Path: {path} is a file, not a directory")


def get_file_size(file: Path) -> str:
    """
    Get the size of a file and return it in human-readable format
    Args:
        file: File to read

    Returns:
        Size of file as human-readable string

    """
    return (
        humanize.naturalsize(file.stat().st_size) if file.stat().st_size else "unknown"
    )


def expand_braces(template: str) -> Iterator[str]:
    """
    Given a string template, apply bash-style brace expansion to return a list of strings
    Args:
        template: Valid brace expansion input, e.g. prefix-{0..10..2}-gap-{11..15}-suffix

    Returns:
        Iterator of brace expansion output

    """
    # pylint: disable = fixme
    # TODO Build custom expansion to validate consistent with cmn/cos/template.go TemplateRange
    return braceexpand.braceexpand(template)


def decode_response(
    res_model: Type[T],
    resp: Response,
) -> T:
    """
    Parse response content from the cluster into a Python class,
     decoding with msgpack depending on content type in header

    Args:
        res_model (Type[T]): Resulting type to which the response should be deserialized
        resp (Response): Response from the AIS cluster

    """
    if resp.headers.get(HEADER_CONTENT_TYPE) == MSGPACK_CONTENT_TYPE:
        return msgpack.decode(resp.content, type=res_model)
    return parse_raw_as(res_model, resp.text)


def parse_url(url: str) -> Tuple[str, str, str]:
    """
    Parse AIS URLs for bucket and object names.

    Args:
        url (str): Complete URL of the object (e.g., "ais://bucket1/file.txt")

    Returns:
        Tuple[str, str, str]: Provider, bucket name, and object name
    """
    parsed_url = urlparse(url)
    path = parsed_url.path.lstrip("/")
    return parsed_url.scheme, parsed_url.netloc, path


def get_logger(name: str, log_format: str = DEFAULT_LOG_FORMAT):
    """
    Create or retrieve a logger with the specified configuration.

    Args:
        name (str): The name of the logger.
        log_format (str, optional): Logging format.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(log_format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    return logger
