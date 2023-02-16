#
# Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
import base64

from typing import Any, Mapping, List, Iterator, Optional, Dict

from pydantic import BaseModel, Field, StrictInt, validator
import requests
from requests.structures import CaseInsensitiveDict

from aistore.sdk.const import (
    DEFAULT_CHUNK_SIZE,
    ProviderAIS,
    CONTENT_LENGTH,
    AIS_CHECKSUM_TYPE,
    AIS_CHECKSUM_VALUE,
    AIS_VERSION,
    AIS_ACCESS_TIME,
    AIS_CUSTOM_MD,
)


# pylint: disable=too-few-public-methods,unused-variable,missing-function-docstring


class Namespace(BaseModel):
    """
    A bucket namespace
    """

    uuid: str = ""
    name: str = ""


class ActionMsg(BaseModel):
    """
    Represents the action message passed by the client via json
    """

    action: str
    name: str = ""
    value: Any = None


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


class NetInfo(BaseModel):
    """
    Represents a set of network-related info
    """

    node_hostname: str = ""
    daemon_port: str = ""
    direct_url: str = ""


class Snode(BaseModel):
    """
    Represents a system node
    """

    daemon_id: str
    daemon_type: str
    public_net: NetInfo = None
    intra_control_net: NetInfo = None
    intra_data_net: NetInfo = None
    flags: int = 0


class Smap(BaseModel):
    """
    Represents a system map
    """

    tmap: Mapping[str, Snode]
    pmap: Mapping[str, Snode]
    proxy_si: Snode
    version: int = 0
    uuid: str = ""
    creation_time: str = ""


class BucketEntry(BaseModel):
    """
    Represents a single entry in a bucket -- an object
    """

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
    """
    Represents the response when getting a list of bucket items, containing a list of BucketEntry objects
    """

    uuid: str
    entries: Optional[List[BucketEntry]] = []
    continuation_token: str
    flags: int

    def get_entries(self):
        return self.entries

    # pylint: disable=no-self-argument
    @validator("entries")
    def set_entries(cls, entries):
        if entries is None:
            entries = []
        return entries


class BucketModel(BaseModel):
    """
    Represents the response from the API containing bucket info
    """

    name: str
    provider: str = ProviderAIS
    ns: Namespace = None


class JobArgs(BaseModel):
    """
    Represents the set of args to pass when making a job-related request
    """

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
    """
    Represents the response of an API query to fetch job status
    """

    uuid: str = ""
    err: str = ""
    end_time: int = 0
    aborted: bool = False


class ObjAttr(BaseModel):
    """
    Represents the attributes parsed from the response headers returned from an API call to get an object

    Args:
        response_headers (CaseInsensitiveDict): Response header dict containing object attributes
    """

    size: int = 0
    checksum_type: str = ""
    access_time: str = ""
    obj_version: str = ""
    checksum_value: str = ""
    custom_metadata: Dict = {}

    def __init__(self, response_headers: CaseInsensitiveDict):
        super().__init__()
        self.size = int(response_headers.get(CONTENT_LENGTH, 0))
        self.checksum_type = response_headers.get(AIS_CHECKSUM_TYPE, "")
        self.checksum_value = response_headers.get(AIS_CHECKSUM_VALUE, "")
        self.access_time = response_headers.get(AIS_ACCESS_TIME, "")
        self.obj_version = response_headers.get(AIS_VERSION, "")
        custom_md_header = response_headers.get(AIS_CUSTOM_MD, "")
        if len(custom_md_header) > 0:
            self._parse_custom(custom_md_header)

    def _parse_custom(self, custom_md_header):
        self.custom_metadata = {}
        for entry in custom_md_header.split(","):
            try:
                assert isinstance(entry, str)
                entry_list = entry.strip().split("=")
                assert len(entry_list) == 2
                self.custom_metadata[entry_list[0]] = entry_list[1]
            except AssertionError:
                continue


class ObjStream(BaseModel):
    """
    Represents the stream of data returned by the API when getting an object
    """

    class Config:
        """
        Pydantic config
        """

        validate_assignment = True
        arbitrary_types_allowed = True

    chunk_size: StrictInt = Field(default=DEFAULT_CHUNK_SIZE, allow_mutation=False)
    response_headers: CaseInsensitiveDict
    stream: requests.Response

    @property
    def attributes(self):
        # Lazy evaluation of ObjAttr parsing based on response headers
        attr = self.__dict__.get("_attributes")
        if attr is None:
            attr = ObjAttr(self.response_headers)
            self.__dict__["_attributes"] = attr
        return attr

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


class ETL(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    """
    Represents the API response when querying an ETL
    """

    id: str = ""
    obj_count: int = 0
    in_bytes: int = 0
    out_bytes: int = 0


class ETLDetails(BaseModel):
    """
    Represents the API response of queries on single ETL details
    """

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


class PromoteAPIArgs(BaseModel):
    """
    Represents the set of args the sdk will pass to AIStore when making a promote request and
    provides conversion to the expected json format
    """

    target_id: str = ""
    source_path: str = ""
    object_name: str = ""
    recursive: bool = False
    overwrite_dest: bool = False
    delete_source: bool = False
    src_not_file_share: bool = False

    def get_json(self):
        return {
            "tid": self.target_id,
            "src": self.source_path,
            "obj": self.object_name,
            "rcr": self.recursive,
            "ovw": self.overwrite_dest,
            "dls": self.delete_source,
            "notshr": self.src_not_file_share,
        }
