#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
from pathlib import Path

import pydantic.tools
import requests

from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.errors import AISError, ErrBckNotFound, ErrRemoteBckNotFound
from aistore.sdk.types import HttpError


def _raise_error(text: str):
    err = pydantic.tools.parse_raw_as(HttpError, text)
    if 400 <= err.status < 500:
        err = pydantic.tools.parse_raw_as(HttpError, text)
        if "does not exist" in err.message:
            if "cloud bucket" in err.message or "remote bucket" in err.message:
                raise ErrRemoteBckNotFound(err.status, err.message)
            if "bucket" in err.message:
                raise ErrBckNotFound(err.status, err.message)
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
        raise ValueError("Provided path does not exist")


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
        raise ValueError("Provided path is a directory, not a file")


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
        raise ValueError("Provided path is a file, not a directory")
