#
# Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
#
from typing import Any

from pydantic import BaseModel
from .const import ProviderAIS


class Namespace(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    uuid: str = ""
    name: str = ""


class Bck(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    name: str
    provider: str = ProviderAIS
    ns: Namespace = None


class ActionMsg(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    action: str
    name: str = ""
    value: Any = None
