#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#
from pydantic import parse_raw_as
import requests

from aistore.client.errors import AISError, ErrBckNotFound, ErrRemoteBckNotFound
from aistore.client.types import HttpError


def _raise_error(text: str):
    err = parse_raw_as(HttpError, text)
    if 400 <= err.status < 500:
        err = parse_raw_as(HttpError, text)
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
            error_text = error_text.decode('utf-8')
        except UnicodeDecodeError:
            error_text = error_text.decode('iso-8859-1')
    if error_text != "":
        _raise_error(error_text)
    resp.raise_for_status()


def probing_frequency(dur: int) -> float:
    freq = min(dur / 8.0, 1.0)
    freq = max(dur / 64.0, freq)
    return max(freq, 0.1)
