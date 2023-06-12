#
# Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
import base64

from typing import Any, Mapping, List, Optional, Dict

import msgspec
from pydantic import BaseModel, validator

from aistore.sdk.namespace import Namespace
from aistore.sdk.const import PROVIDER_AIS
from aistore.sdk.list_object_flag import ListObjectFlag


# pylint: disable=too-few-public-methods,unused-variable,missing-function-docstring


class ActionMsg(BaseModel):
    """
    Represents the action message passed by the client via json
    """

    action: str
    name: str = ""
    value: Any = None


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


class BucketEntry(msgspec.Struct):
    """
    Represents a single entry in a bucket -- an object
    See cmn/objlist.go/LsoEntry
    """

    n: str
    cs: str = ""
    a: str = ""
    v: str = ""
    t: str = ""
    s: int = 0
    c: int = 0
    f: int = 0
    object: Any = None

    @property
    def name(self):
        return self.n

    @property
    def checksum(self):
        return self.cs

    @property
    def atime(self):
        return self.a

    @property
    def version(self):
        return self.v

    @property
    def location(self):
        return self.t

    @property
    def size(self):
        return self.s

    @property
    def copies(self):
        return self.c

    @property
    def flags(self):
        return self.f

    def is_cached(self):
        return (self.flags & (1 << 6)) != 0

    def is_ok(self):
        return (self.flags & ((1 << 5) - 1)) == 0


class BucketList(msgspec.Struct):
    """
    Represents the response when getting a list of bucket items, containing a list of BucketEntry objects
    """

    UUID: str
    ContinuationToken: str
    Flags: int
    Entries: List[BucketEntry] = None

    @property
    def uuid(self):
        return self.UUID

    @property
    def continuation_token(self):
        return self.ContinuationToken

    @property
    def flags(self):
        return self.Flags

    @property
    def entries(self):
        return [] if self.Entries is None else self.Entries

    def get_entries(self):
        """
        Deprecated -- use entries property
        """
        return self.entries


class BucketModel(BaseModel):
    """
    Represents the response from the API containing bucket info
    """

    name: str
    provider: str = PROVIDER_AIS
    namespace: Namespace = None

    def as_dict(self):
        dict_rep = {"name": self.name, "provider": self.provider}
        if self.namespace:
            dict_rep["namespace"] = self.namespace
        return dict_rep


class BsummCtrlMsg(BaseModel):
    """
    Represents the bucket summary control message
    """

    uuid: str
    prefix: str
    fast: bool
    cached: bool
    present: bool


class JobArgs(BaseModel):
    """
    Represents the set of args to pass when making a job-related request
    """

    id: str = ""
    kind: str = ""
    daemon_id: str = ""
    bucket: BucketModel = None
    buckets: List[BucketModel] = None

    def as_dict(self):
        return {
            "ID": self.id,
            "Kind": self.kind,
            "DaemonID": self.daemon_id,
            "Bck": self.bucket,
            "Buckets": self.buckets,
        }


class JobQuery(BaseModel):
    """
    Structure to send the API when querying the cluster for multiple jobs
    """

    active: bool = False
    kind: str = ""
    target: str = ""

    def as_dict(self):
        return {
            "kind": self.kind,
            "node": self.target,
            "show_active": self.active,
        }


class JobStatus(BaseModel):
    """
    Represents the response of an API query to fetch job status
    """

    uuid: str = ""
    err: str = ""
    end_time: int = 0
    aborted: bool = False


class ETLInfo(BaseModel):  # pylint: disable=too-few-public-methods,unused-variable
    """
    Represents the API response when querying an ETL
    """

    id: str = ""
    xaction_id: str = ""
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


class InitETLArgs(BaseModel):
    """
    Represents the args shared by ETL initialization with code or spec
    """

    etl_name: str
    communication_type: str
    timeout: str


class InitSpecETLArgs(InitETLArgs):
    """
    Represents the set of args the sdk will pass to AIStore when making a request to initialize an ETL with a spec
    """

    spec: str

    def as_dict(self):
        return {
            "id": self.etl_name,
            "timeout": self.timeout,
            "communication": f"{self.communication_type}://",
            "spec": self.spec,
        }


class InitCodeETLArgs(InitETLArgs):
    """
    Represents the set of args the sdk will pass to AIStore when making a request to initialize an ETL with code
    """

    runtime: str
    dependencies: str
    functions: Dict[str, str]
    code: str
    chunk_size: int = None
    transform_url: bool = False

    def as_dict(self):
        dict_rep = {
            "id": self.etl_name,
            "runtime": self.runtime,
            "communication": f"{self.communication_type}://",
            "timeout": self.timeout,
            "funcs": self.functions,
            "code": self.code,
            "dependencies": self.dependencies,
            "transform_url": self.transform_url,
        }
        if self.chunk_size:
            dict_rep["chunk_size"] = self.chunk_size
        return dict_rep


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

    def as_dict(self):
        return {
            "tid": self.target_id,
            "src": self.source_path,
            "obj": self.object_name,
            "rcr": self.recursive,
            "ovw": self.overwrite_dest,
            "dls": self.delete_source,
            "notshr": self.src_not_file_share,
        }


class JobStats(BaseModel):
    """
    Structure for job statistics
    """

    objects: int = 0
    bytes: int = 0
    out_objects: int = 0
    out_bytes: int = 0
    in_objects: int = 0
    in_bytes: int = 0


class JobSnapshot(BaseModel):
    """
    Structure for the data returned when querying a single job on a single target node
    """

    id: str = ""
    kind: str = ""
    start_time: str = ""
    end_time: str = ""
    bucket: BucketModel = None
    source_bck: str = ""
    dest_bck: str = ""
    rebalance_id: str = ""
    stats: JobStats = None
    aborted: bool = False
    is_idle: bool = False


class CopyBckMsg(BaseModel):
    """
    API message structure for copying a bucket
    """

    prefix: str = ""
    prepend: str
    dry_run: bool
    force: bool

    def as_dict(self):
        return {
            "prefix": self.prefix,
            "prepend": self.prepend,
            "dry_run": self.dry_run,
            "force": self.force,
        }


class ListObjectsMsg(BaseModel):
    """
    API message structure for listing objects in a bucket
    """

    prefix: str
    page_size: int
    uuid: str
    props: str
    continuation_token: str
    flags: List[ListObjectFlag]
    target: str

    def as_dict(self):
        return {
            "prefix": self.prefix,
            "pagesize": self.page_size,
            "uuid": self.uuid,
            "props": self.props,
            "continuation_token": self.continuation_token,
            "flags": str(ListObjectFlag.join_flags(self.flags)),
            "target": self.target,
        }


class TransformBckMsg(BaseModel):
    """
    API message structure for requesting an etl transform on a bucket
    """

    etl_name: str
    timeout: str

    def as_dict(self):
        return {"id": self.etl_name, "request_timeout": self.timeout}


class TCBckMsg(BaseModel):
    """
    API message structure for transforming or copying between buckets.
    Can be used on its own for an entire bucket or encapsulated in TCMultiObj to apply only to a selection of objects
    """

    ext: Dict[str, str] = None
    copy_msg: CopyBckMsg = None
    transform_msg: TransformBckMsg = None

    def as_dict(self):
        dict_rep = {}
        if self.ext:
            dict_rep["ext"] = self.ext
        if self.copy_msg:
            for key, val in self.copy_msg.as_dict().items():
                dict_rep[key] = val
        if self.transform_msg:
            for key, val in self.transform_msg.as_dict().items():
                dict_rep[key] = val
        return dict_rep


class TCMultiObj(BaseModel):
    """
    API message structure for transforming or copying multiple objects between buckets
    """

    to_bck: BucketModel
    tc_msg: TCBckMsg = None
    continue_on_err: bool
    object_selection: dict

    def as_dict(self):
        dict_rep = self.object_selection
        if self.tc_msg:
            for key, val in self.tc_msg.as_dict().items():
                dict_rep[key] = val
        dict_rep["tobck"] = self.to_bck.as_dict()
        dict_rep["coer"] = self.continue_on_err
        return dict_rep


class ArchiveMultiObj(BaseModel):
    """
    API message structure for multi-object archive requests
    """

    archive_name: str
    to_bck: BucketModel
    mime: str = None
    include_source_name = False
    allow_append = False
    continue_on_err = False
    object_selection: dict

    def as_dict(self):
        dict_rep = self.object_selection
        dict_rep["archname"] = self.archive_name
        dict_rep["isbn"] = self.include_source_name
        dict_rep["aate"] = self.allow_append
        dict_rep["coer"] = self.continue_on_err
        if self.mime:
            dict_rep["mime"] = self.mime
        if self.to_bck:
            dict_rep["tobck"] = self.to_bck.as_dict()
        return dict_rep
