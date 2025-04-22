#
# Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
#
from __future__ import annotations
import base64
from typing import Any, Mapping, List, Optional, Dict
import msgspec
from pydantic.v1 import BaseModel, Field, validator
from requests.structures import CaseInsensitiveDict

from aistore.sdk.errors import NoTargetError
from aistore.sdk.namespace import Namespace
from aistore.sdk.list_object_flag import ListObjectFlag
from aistore.sdk.obj.object_props import ObjectProps
from aistore.sdk.const import (
    HEADER_CONTENT_LENGTH,
    AIS_CHECKSUM_VALUE,
    AIS_ACCESS_TIME,
    AIS_VERSION,
    AIS_OBJ_NAME,
    AIS_LOCATION,
    AIS_MIRROR_COPIES,
    JOGGER_COUNT_MASK,
    WORKER_COUNT_MASK,
    WORKER_COUNT_SHIFT,
    CHANNEL_COUNT_MASK,
    CHANNEL_COUNT_SHIFT,
)
from aistore.sdk.utils import get_digest, xoshiro256_hash


# pylint: disable=too-few-public-methods,unused-variable,missing-function-docstring,too-many-lines
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
    id_digest: int = 0

    def in_maint_or_decomm(self) -> bool:
        return (self.flags & (1 << 2 | 1 << 3)) != 0


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

    def get_target_for_object(self, uname: str) -> Snode:
        """
        Determine the target node responsible for an object based on its bucket path and name.

        Args:
            uname (str): Fully qualified (namespaced) object name (e.g., f"{bck.get_path()}{obj.name}").

        Returns:
            Snode: The assigned target node.

        Raises:
            AISError: If no suitable target node is found.
        """
        digest = get_digest(uname)

        selected_node, max_hash = None, -1

        for tsi in self.tmap.values():
            if tsi.in_maint_or_decomm():
                continue  # Skip nodes in maintenance or decommissioned mode

            # Compute hash using Xoshiro256
            cs = xoshiro256_hash(tsi.id_digest ^ digest)

            if cs > max_hash:
                max_hash, selected_node = cs, tsi

        if selected_node is None:
            raise NoTargetError(len(self.tmap))

        return selected_node


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

    def generate_object_props(self) -> ObjectProps:
        """
        Convert bucket entry data into Object Props.

        NOTE: Bucket entry data and object props are not a one-to-one mapping.

        Returns:
            ObjectProps with object data
        """
        headers = CaseInsensitiveDict(
            {
                AIS_OBJ_NAME: self.name,
                AIS_CHECKSUM_VALUE: self.checksum,
                AIS_ACCESS_TIME: self.atime,
                AIS_VERSION: self.version,
                AIS_LOCATION: self.location,
                HEADER_CONTENT_LENGTH: self.size,
                AIS_MIRROR_COPIES: self.copies,
            }
        )
        return ObjectProps(headers)

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
    provider: str
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
    stage: str = ""
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
    argument: str = ""

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
    arg_type: str = ""


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
            "argument": self.arg_type,
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

    def as_dict(self):
        dict_rep = {
            "id": self.etl_name,
            "runtime": self.runtime,
            "communication": f"{self.communication_type}://",
            "timeout": self.timeout,
            "funcs": self.functions,
            "code": self.code,
            "dependencies": self.dependencies,
            "argument": self.arg_type,
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
    Represents performance statistics for a job.
    """

    objects: int = Field(default=0, alias="loc-objs")
    bytes: int = Field(default=0, alias="loc-bytes")
    out_objects: int = Field(default=0, alias="out-objs")
    out_bytes: int = Field(default=0, alias="out-bytes")
    in_objects: int = Field(default=0, alias="in-objs")
    in_bytes: int = Field(default=0, alias="in-bytes")

    class Config:  # pylint: disable=missing-class-docstring
        allow_population_by_field_name = True


class JobSnap(BaseModel):
    """
    Represents a snapshot of a job on a target node.
    """

    id: str = Field(default="")
    kind: str = Field(default="")
    start_time: str = Field(default="", alias="start-time")
    end_time: str = Field(default="", alias="end-time")
    bucket: Optional[BucketModel] = Field(default=None, alias="bck")
    source_bck: Optional[BucketModel] = Field(default=None, alias="src-bck")
    destination_bck: Optional[BucketModel] = Field(default=None, alias="dst-bck")
    stats: Optional[JobStats] = Field(default=None)
    aborted: bool = Field(default=False)
    is_idle: bool = Field(default=False)
    abort_err: str = Field(default="", alias="abort-err")
    packed: int = Field(
        default=0, alias="glob.id"
    )  # Bitwise representation of the number of joggers, workers, and channel full status

    class Config:  # pylint: disable=missing-class-docstring
        allow_population_by_field_name = True

    def unpack_glob_id(self) -> tuple[int, int, int]:
        """
        Unpack the `glob_id` into (njoggers, nworkers, chan_full).
        The packed value is a bitwise representation of the number of joggers, workers, and channel full status.
        """
        njoggers = self.packed & JOGGER_COUNT_MASK
        nworkers = (self.packed & WORKER_COUNT_MASK) >> WORKER_COUNT_SHIFT
        chbuf_cnt = (self.packed & CHANNEL_COUNT_MASK) >> CHANNEL_COUNT_SHIFT
        return njoggers, nworkers, chbuf_cnt


class AggregatedJobSnap(BaseModel):
    """
    Represents job snapshots grouped by target ID.

    Under the hood this is a root model (mapping node IDs →
    lists of JobSnapshot), but we expose it via the `snapshots`
    property so that static checkers don’t get confused.
    """

    __root__: Dict[str, List[JobSnap]] = Field(default_factory=dict)

    @property
    def snapshots(self) -> Dict[str, List[JobSnap]]:
        """
        Return the underlying dict of nodeID → [JobSnapshot,...].
        """
        # __root__ in __dict__ is the real dict, not a FieldInfo
        return self.__dict__.get("__root__", {})

    def list_snapshots(self) -> List[JobSnap]:
        """
        Return a flat list of all JobSnapshot instances.
        """
        return [snap for snaps in self.snapshots.values() for snap in snaps]

    def get_num_workers(self) -> int:
        """
        Return the nworkers from any one snapshot (as all are the same).
        """
        for snaps in self.snapshots.values():
            if snaps:
                # unpack() → (njoggers, nworkers, chan_full)
                return snaps[0].unpack_glob_id()[1]
        return 0


class CopyBckMsg(BaseModel):
    """
    API message structure for copying a bucket
    """

    prefix: str = ""
    prepend: str
    dry_run: bool
    force: bool
    latest: bool
    sync: bool

    def as_dict(self):
        return {
            "prefix": self.prefix,
            "prepend": self.prepend,
            "dry_run": self.dry_run,
            "force": self.force,
            "latest-ver": self.latest,
            "synchronize": self.sync,
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
    num_workers: int = None

    def as_dict(self):
        dict_rep = {}
        if self.ext:
            dict_rep["ext"] = self.ext
        if self.num_workers:
            dict_rep["num-workers"] = self.num_workers
        if self.copy_msg:
            for key, val in self.copy_msg.as_dict().items():
                dict_rep[key] = val
        if self.transform_msg:
            for key, val in self.transform_msg.as_dict().items():
                dict_rep[key] = val
        return dict_rep


class PrefetchMsg(BaseModel):
    """
    API message structure for prefetching objects from remote buckets.
    """

    object_selection: Dict
    continue_on_err: bool
    latest: bool
    blob_threshold: int = None
    num_workers: int = None

    def as_dict(self):
        dict_rep = self.object_selection
        dict_rep["coer"] = self.continue_on_err
        dict_rep["latest-ver"] = self.latest
        if self.blob_threshold:
            dict_rep["blob-threshold"] = self.blob_threshold
        if self.num_workers:
            dict_rep["num-workers"] = self.num_workers
        return dict_rep


class TCMultiObj(BaseModel):
    """
    API message structure for transforming or copying multiple objects between buckets
    """

    to_bck: BucketModel
    tc_msg: TCBckMsg = None
    continue_on_err: bool
    object_selection: Dict
    num_workers: int = None

    def as_dict(self):
        dict_rep = self.object_selection
        if self.tc_msg:
            for key, val in self.tc_msg.as_dict().items():
                dict_rep[key] = val
        dict_rep["tobck"] = self.to_bck.as_dict()
        dict_rep["coer"] = self.continue_on_err
        if self.num_workers:
            dict_rep["num-workers"] = self.num_workers
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
    object_selection: Dict

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


class BlobMsg(BaseModel):
    """
    Represents the set of args the sdk will pass to AIStore when making a blob-download request
    and provides conversion to the expected json format
    """

    chunk_size: int = None
    num_workers: int = None
    latest: bool

    def as_dict(self):
        dict_rep = {
            "latest-ver": self.latest,
        }
        if self.chunk_size:
            dict_rep["chunk-size"] = self.chunk_size
        if self.num_workers:
            dict_rep["num-workers"] = self.num_workers
        return dict_rep
