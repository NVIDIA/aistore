#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#

import logging
import re
from pathlib import Path
from typing import Iterator, Optional, Tuple, Type, TypeVar, Union
from urllib.parse import urlparse, urlunparse

import braceexpand
import humanize
import requests
import xxhash

from msgspec import msgpack
from pydantic.v1 import BaseModel, parse_raw_as

from aistore.sdk.const import (
    HEADER_CONTENT_TYPE,
    MSGPACK_CONTENT_TYPE,
    DEFAULT_LOG_FORMAT,
    XX_HASH_SEED,
)

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

    status_code: int
    message: str = ""
    method: str = ""
    url_path: str = ""
    remote_addr: str = ""
    caller: str = ""
    node: str = ""


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


def extract_and_parse_url(msg: str) -> Optional[Tuple[str, str, bool]]:
    """
    Extract provider, bucket, and whether an object is present.

    Args:
        msg (str): Any string that may contain an AIS FQN.

    Returns:
        Optional[Tuple[str, str, bool]]: (prov, bck, has_obj) if a FQN is found, otherwise None.
    """
    pattern = r"([a-z0-9]+)://([A-Za-z0-9@._-]+)(/.*)?"
    match = re.search(pattern, msg)

    if not match:
        return None

    prov = match.group(1)
    bck = match.group(2)
    has_obj = match.group(3) is not None  # true if `/` after bucket

    return prov, bck, has_obj


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


def convert_to_seconds(time_val: Union[str, int]) -> int:
    """
    Converts a time value (e.g., '5s', '10m', '2h', '3d', or 10) to seconds.
    If no unit is provided (e.g., '10' or 10), seconds are assumed.

    Args:
        time_val (Union[str, int]): The time value to convert.

    Returns:
        int: The equivalent time in seconds.

    Raises:
        ValueError: If the format or unit is invalid.
    """
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}

    if isinstance(time_val, int):
        return time_val

    if not isinstance(time_val, str) or not time_val.strip():
        raise ValueError("Time value must be a non-empty string or integer.")

    time_val = time_val.strip()

    if time_val.isdigit():
        return int(time_val)

    num, unit = time_val[:-1], time_val[-1]

    if unit not in multipliers:
        raise ValueError(f"Unsupported time unit: '{unit}'. Use 's', 'm', 'h', or 'd'.")

    if not num.isdigit():
        raise ValueError(f"Invalid numeric value in time: '{num}'.")

    return int(num) * multipliers[unit]


def compose_etl_direct_put_url(direct_put_url: str, host_target: str) -> str:
    """
    Composes the final direct PUT URL by merging the AIS target base URL (`host_target`)
    with the destination node address and object path from `direct_put_url`.

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
