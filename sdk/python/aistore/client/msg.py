#
# Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
#
from typing import Any, Mapping

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


class NetInfo(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    node_hostname: str
    daemon_port: str
    direct_url: str


class Snode(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    daemon_id: str
    daemon_type: str
    public_net: NetInfo = None
    intra_control_net: NetInfo = None
    intra_data_net: NetInfo = None
    flags: int


class Smap(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    tmap: Mapping[str, Snode]
    pmap: Mapping[str, Snode]
    Primary: Snode
    version: int
    uuid: str
    creation_time: str
