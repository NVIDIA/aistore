#
# Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable

from typing import Any, Mapping, List, Iterator, Optional

from pydantic import BaseModel, Field, StrictInt, StrictStr, validator
import requests
from aistore.client.const import DEFAULT_CHUNK_SIZE, ProviderAIS

# pylint: disable=too-few-public-methods,unused-variable


class Namespace(BaseModel):
    uuid: str = ""
    name: str = ""


class Bck(BaseModel):
    name: str
    provider: str = ProviderAIS
    ns: Namespace = None


class ActionMsg(BaseModel):
    action: str
    name: str = ""
    value: Any = None


class NetInfo(BaseModel):
    node_hostname: str = ""
    daemon_port: str = ""
    direct_url: str = ""


class Snode(BaseModel):
    daemon_id: str
    daemon_type: str
    public_net: NetInfo = None
    intra_control_net: NetInfo = None
    intra_data_net: NetInfo = None
    flags: int = 0


class Smap(BaseModel):
    tmap: Mapping[str, Snode]
    pmap: Mapping[str, Snode]
    proxy_si: Snode
    version: int = 0
    uuid: str = ""
    creation_time: str = ""


class BucketEntry(BaseModel):
    name: str
    size: int = 0
    checksum: str = ""
    atime: str = ""
    version: str = ""
    target_url: str = ""
    copies: int = 0
    flags: int = 0

    def is_cached(self):
        return (self.flags & (1 << 6)) != 0

    def is_ok(self):
        return (self.flags & ((1 << 5) - 1)) == 0


class BucketList(BaseModel):
    uuid: str
    entries: Optional[List[BucketEntry]] = []
    continuation_token: str
    flags: int

    def get_entries(self):
        return self.entries

    @validator('entries')
    def set_entries(cls, entries):  # pylint: disable=no-self-argument
        if entries is None:
            entries = []
        return entries


class XactStatus(BaseModel):
    uuid: str = ""
    err: str = ""
    end_time: int = 0
    aborted: bool = False


class ObjStream(BaseModel):
    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    content_length: StrictInt = Field(default=-1, allow_mutation=False)
    chunk_size: StrictInt = Field(default=DEFAULT_CHUNK_SIZE, allow_mutation=False)
    e_tag: StrictStr = Field(..., allow_mutation=False)
    e_tag_type: StrictStr = Field(..., allow_mutation=False)
    stream: requests.Response

    # read_all uses a bytes cast which makes it slightly slower
    def read_all(self) -> bytes:
        obj_arr = bytearray()
        for chunk in self:
            obj_arr.extend(chunk)
        return bytes(obj_arr)

    def raw(self) -> bytes:
        return self.stream.raw

    def __iter__(self) -> Iterator[bytes]:
        try:
            for chunk in self.stream.iter_content(chunk_size=self.chunk_size):
                yield chunk
        finally:
            self.stream.close()


class HttpError(BaseModel):
    status: int
    message: str = ""
    method: str = ""
    url_path: str = ""
    remote_addr: str = ""
    caller: str = ""
    node: str = ""


class BucketLister:
    _fetched: Optional[List[BucketEntry]] = []
    _token: str = ""
    _uuid: str = ""
    _prefix: str = ""
    _props: str = ""
    _provider: str = ProviderAIS
    _bck_name: str = ""
    _client: 'Client'

    def __init__(self, client: 'Client', bck_name: str = "", provider: str = ProviderAIS, prefix: str = "", props: str = "", page_size: int = 0):
        self._client = client
        self._prefix = prefix
        self._props = props
        self._bck_name = bck_name
        self._provider = provider
        self._page_size = page_size

    def __iter__(self):
        return self

    def __next__(self) -> Iterator[BucketEntry]:
        # Iterator is exhausted.
        if len(self._fetched) == 0 and self._token == "" and self._uuid != "":
            raise StopIteration
        # Read the next page of objects.
        if len(self._fetched) == 0:
            value = {
                "prefix": self._prefix,
                "uuid": self._uuid,
                "props": self._props,
                "continuation_token": self._token,
                "pagesize": self._page_size,
            }
            resp = self._client.bucket(self._bck_name, self._provider).list_objects(
                prefix=self._prefix, props=self._props, uuid=self._uuid, continuation_token=self._token, page_size=self._page_size
            )
            self._fetched = resp.get_entries()
            self._uuid = resp.uuid
            self._token = resp.continuation_token
            # Empty page and token mean no more objects left.
            if len(self._fetched) == 0 and self._token == "":
                raise StopIteration
        return self._fetched.pop(0)
