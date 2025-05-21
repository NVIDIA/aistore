#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import base64
from typing import Type

import cloudpickle
from aistore.sdk.etl.webserver.base_etl_server import ETLServer
from aistore.sdk.const import UTF_ENCODING


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


def deserialize_class(payload: str, encoding: str = UTF_ENCODING) -> Type[ETLServer]:
    """
    Decode a Base64 payload and unpickle it back into an ETLServer subclass.

    Args:
        payload: Base64-encoded pickled data.
        encoding: The string encoding used to decode the payload.

    Returns:
        The ETLServer subclass.

    Raises:
        TypeError: If the unpickled object is not a subclass of ETLServer.
    """
    raw = base64.b64decode(payload.encode(encoding))
    cls = cloudpickle.loads(raw)

    if not isinstance(cls, type) or not issubclass(cls, ETLServer):
        raise TypeError(f"Deserialized object {cls!r} is not an ETLServer subclass")
    return cls
