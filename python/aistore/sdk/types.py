#
# Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
import base64

from typing import Any, Mapping, List, Iterator, Optional

from pydantic import BaseModel, Field, StrictInt, StrictStr, validator
import requests

from aistore.sdk.const import DEFAULT_CHUNK_SIZE, ProviderAIS


# pylint: disable=too-few-public-methods,unused-variable


class Namespace(BaseModel):
    uuid: str = ""
    name: str = ""


class ActionMsg(BaseModel):
    action: str
    name: str = ""
    value: Any = None


class HttpError(BaseModel):
    status: int
    message: str = ""
    method: str = ""
    url_path: str = ""
    remote_addr: str = ""
    caller: str = ""
    node: str = ""


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

    @validator("entries")
    def set_entries(cls, entries):  # pylint: disable=no-self-argument
        if entries is None:
            entries = []
        return entries


class BucketModel(BaseModel):
    name: str
    provider: str = ProviderAIS
    ns: Namespace = None


class JobArgs(BaseModel):
    id: str = ""
    kind: str = ""
    daemon_id: str = ""
    buckets: List[BucketModel] = None
    only_running: bool = False

    def get_json(self):
        return {
            "ID": self.id,
            "Kind": self.kind,
            "DaemonID": self.daemon_id,
            "Buckets": self.buckets,
            "OnlyRunning": self.only_running,
        }


class JobStatus(BaseModel):
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


class ObjectRange(BaseModel):
    prefix: str
    min_index: int
    max_index: int
    pad_width: int = 0
    step: int = 1
    suffix: str = ""


class ETL(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    id: str = ""
    obj_count: int = 0
    in_bytes: int = 0
    out_bytes: int = 0


class ETLDetails(BaseModel):
    id: str
    communication: str
    timeout: str
    code: Optional[bytes]
    spec: Optional[str]
    dependencies: Optional[str]
    runtime: Optional[str]  # see ext/etl/runtime/all.go
    chunk_size: int = 0

    @validator("code")
    def set_code(cls, code):  # pylint: disable=no-self-argument
        if code is not None:
            code = base64.b64decode(code)
        return code

    @validator("spec")
    def set_spec(cls, spec):  # pylint: disable=no-self-argument
        if spec is not None:
            spec = base64.b64decode(spec)
        return spec
