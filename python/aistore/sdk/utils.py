#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#

import logging
import re
from pathlib import Path
from typing import Callable, Iterator, Optional, Tuple, Type, TypeVar
from urllib.parse import urlparse

import braceexpand
import humanize
import requests
import xxhash

from msgspec import msgpack
from pydantic.v1 import BaseModel, parse_raw_as

from aistore.sdk.const import (
    UTF_ENCODING,
    HEADER_CONTENT_TYPE,
    MSGPACK_CONTENT_TYPE,
    DEFAULT_LOG_FORMAT,
    XX_HASH_SEED,
)
from aistore.sdk.errors import (
    AISError,
    ErrBckNotFound,
    ErrRemoteBckNotFound,
    ErrBckAlreadyExists,
    ErrObjNotFound,
    ErrETLNotFound,
    ErrETLAlreadyExists,
    InvalidBckProvider,
)
from aistore.sdk.provider import Provider

T = TypeVar("T")
MASK = 0xFFFFFFFFFFFFFFFF  # 64-bit mask
# fmt: off
GOLDEN_RATIO = 0x9e3779b97f4a7c15
CONST1 = 0xbf58476d1ce4e5b9
CONST2 = 0x94d049bb133111eb
# fmt: on
ROTATION_BITS = 7


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


def decode_response_text(resp: requests.Response) -> str:
    """
    Convert the response body to a decoded string if it is bytes. Otherwise, return as is.

    Args:
        resp (requests.Response): The response object.

    Returns:
        str: The decoded response text (UTF-8 or ISO-8859-1).
    """
    error_text = resp.text
    if isinstance(error_text, bytes):
        try:
            error_text = error_text.decode(UTF_ENCODING)
        except UnicodeDecodeError:
            error_text = error_text.decode("iso-8859-1")
    return error_text


def parse_http_error_or_raise(
    resp: requests.Response, exc_class: Type[Exception]
) -> HttpError:
    """
    Parse the response body as a HttpError object or raise a custom exception on failure.

    Args:
        resp (requests.Response): The response to parse.
        exc_class (Type[Exception]): The exception class to raise if parsing fails.

    Returns:
        HttpError: The parsed HttpError model.

    Raises:
        exc_class: If the response text cannot be parsed into HttpError.
    """
    error_text = decode_response_text(resp)
    try:
        return parse_raw_as(HttpError, error_text)
    except Exception as exc:
        raise exc_class(resp.status_code, error_text) from exc


def parse_ais_error(resp: requests.Response) -> AISError:
    """
    Parse raw text into an appropriate AISError object.

    Args:
        resp (requests.Response): The response from the AIS cluster.

    Returns:
        AISError: If the error doesn't match any specific conditions.
        ErrBckNotFound: If the error message indicates a missing bucket.
        ErrRemoteBckNotFound: If the error message indicates a missing remote bucket.
        ErrObjNotFound: If the error message indicates a missing object.
        ErrBckAlreadyExists: If the error message indicates a bucket already exists.
        ErrETLAlreadyExists: If the error message indicates an ETL already exists
        ErrETLNotFound: If the error message indicates a missing ETL.
    """
    exc_class = AISError
    err = parse_http_error_or_raise(resp, exc_class)
    status, message = err.status, err.message
    prov, bck, obj = _extract_and_parse_url(message) or (None, None, None)
    if prov:
        try:
            prov = Provider.parse(prov)
        except InvalidBckProvider:
            prov = None

    if status == 404:
        if "etl job" in message:
            exc_class = ErrETLNotFound
        elif prov:
            if obj:
                exc_class = ErrObjNotFound
            elif prov.is_remote() or "@" in bck:
                exc_class = ErrRemoteBckNotFound
            else:
                exc_class = ErrBckNotFound
    elif status == 409:
        if "etl job" in message:
            exc_class = ErrETLAlreadyExists
        elif prov and not obj:
            exc_class = ErrBckAlreadyExists

    return exc_class(status, message)


# pylint: disable=unused-variable
def handle_errors(
    resp: requests.Response,
    parse_error: Callable[[requests.Response], Exception],
) -> None:
    """
    Error handling for requests made to the AIS Client

    Args:
        resp: requests.Response = Response received from the request
        parse_error_fn: Function that processes error text and returns appropriate exceptions.
    """
    try:
        # Will raise HTTPError if status code is 4xx or 5xx
        resp.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        raise parse_error(resp) from http_err

    # Final status code (post-redirection) wasn't 4xx/5xx. Could be 1xx or 3xx.
    raise parse_error(resp)


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
    resp: requests.Response,
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


def _extract_and_parse_url(msg: str) -> Optional[Tuple[str, str, str]]:
    url_pattern = r"[a-z0-9]+://[A-Za-z0-9@._/-]+"
    match = re.search(url_pattern, msg)
    if match:
        return parse_url(match.group())
    return None


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
        handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


# Translated from:
# http://xoshiro.di.unimi.it/xoshiro256starstar.c
# Scrambled Linear Pseudorandom Number Generators
# David Blackman, Sebastiano Vigna
# https://arxiv.org/abs/1805.01407
# http://www.pcg-random.org/posts/a-quick-look-at-xoshiro256.html
def xoshiro256_hash(seed: int) -> int:
    """
    Xoshiro256-inspired hash function with 64-bit overflow behavior.
    """
    z = (seed + GOLDEN_RATIO) & MASK
    z = (z ^ (z >> 30)) * CONST1 & MASK
    z = (z ^ (z >> 27)) * CONST2 & MASK
    z = (z ^ (z >> 31)) + GOLDEN_RATIO & MASK
    z = (z ^ (z >> 30)) * CONST1 & MASK
    z = (z ^ (z >> 27)) * CONST2 & MASK
    z = (z ^ (z >> 31)) * 5 & MASK
    rotated = ((z << ROTATION_BITS) | (z >> (64 - ROTATION_BITS))) & MASK
    return (rotated * 9) & MASK


def get_digest(name: str) -> int:
    """
    Get the xxhash digest of a given string.
    """
    return xxhash.xxh64(seed=XX_HASH_SEED, input=name.encode("utf-8")).intdigest()
