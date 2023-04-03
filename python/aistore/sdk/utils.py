#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
from pathlib import Path
from typing import Iterator

import braceexpand
import humanize
import pydantic.tools
import requests
from pydantic import BaseModel

from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.errors import (
    AISError,
    ErrBckNotFound,
    ErrRemoteBckNotFound,
    ErrBckAlreadyExists,
    ErrETLAlreadyExists,
)


class HttpError(BaseModel):
    """
    Represents the errors returned by the API
    """

    status: int
    message: str = ""
    method: str = ""
    url_path: str = ""
    remote_addr: str = ""
    caller: str = ""
    node: str = ""


def _raise_error(text: str):
    err = pydantic.tools.parse_raw_as(HttpError, text)
    if 400 <= err.status < 500:
        err = pydantic.tools.parse_raw_as(HttpError, text)
        if "does not exist" in err.message:
            if "cloud bucket" in err.message or "remote bucket" in err.message:
                raise ErrRemoteBckNotFound(err.status, err.message)
            if "bucket" in err.message:
                raise ErrBckNotFound(err.status, err.message)
        if "already exists" in err.message:
            if "bucket" in err.message:
                raise ErrBckAlreadyExists(err.status, err.message)
            if "etl" in err.message:
                raise ErrETLAlreadyExists(err.status, err.message)
    raise AISError(err.status, err.message)


# pylint: disable=unused-variable
def handle_errors(resp: requests.Response):
    """
    Error handling for requests made to the AIS Client

    Args:
        resp: requests.Response = Response received from the request
    """
    error_text = resp.text
    if isinstance(resp.text, bytes):
        try:
            error_text = error_text.decode(UTF_ENCODING)
        except UnicodeDecodeError:
            error_text = error_text.decode("iso-8859-1")
    if error_text != "":
        _raise_error(error_text)
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


def read_file_bytes(filepath: str):
    """
    Given a filepath, read the content as bytes
    Args:
        filepath: Existing local filepath

    Returns: Raw bytes
    """
    with open(filepath, "rb") as reader:
        return reader.read()


def _check_path_exists(path: str):
    if not Path(path).exists():
        raise ValueError(f"Path: {path} does not exist")


def validate_file(path: str):
    """
    Validate that a file exists and is a file
    Args:
        path: Path to validate
    Raises:
        ValueError: If path does not exist or is not a file
    """
    _check_path_exists(path)
    if not Path(path).is_file():
        raise ValueError(f"Path: {path} is a directory, not a file")


def validate_directory(path: str):
    """
    Validate that a directory exists and is a directory
    Args:
        path: Path to validate
    Raises:
        ValueError: If path does not exist or is not a directory
    """
    _check_path_exists(path)
    if not Path(path).is_dir():
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
