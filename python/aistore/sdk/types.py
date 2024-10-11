#
# Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations
import base64
from typing import Any, Mapping, List, Optional, Dict

import msgspec
import humanize
from pydantic import BaseModel, Field, validator

from requests.structures import CaseInsensitiveDict

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
)


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

    id: str = Field(default="", alias="id")
    kind: str = Field(default="", alias="kind")
    start_time: str = Field(default="", alias="start-time")
    end_time: str = Field(default="", alias="end-time")
    bucket: BucketModel = Field(default=None, alias="bck")
    source_bck: str = Field(default="", alias="source-bck")
    dest_bck: str = Field(default="", alias="dest-bck")
    rebalance_id: str = Field(default="", alias="glob.id")
    stats: JobStats = Field(default=None, alias="stats")
    aborted: bool = Field(default=False, alias="aborted")
    is_idle: bool = Field(default=False, alias="is_idle")
    abort_err: str = Field(default="", alias="abort-err")

    class Config:
        """
        Configuration for Pydantic model to use alias.
        """

        allow_population_by_field_name = True


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


class NodeTracker(BaseModel):
    """
    Represents the tracker info of a cluster node
    """

    append_ns: int = Field(0, alias="append.ns")
    del_n: int = Field(0, alias="del.n")
    disk_sdb_util: float = Field(0, alias="disk.sdb..util")
    disk_sdb_avg_rsize: float = Field(0, alias="disk.sdb.avg.rsize")
    disk_sdb_avg_wsize: float = Field(0, alias="disk.sdb.avg.wsize")
    disk_sdb_read_bps: int = Field(0, alias="disk.sdb.read.bps")
    disk_sdb_write_bps: int = Field(0, alias="disk.sdb.write.bps")
    dl_ns: int = Field(0, alias="dl.ns")
    dsort_creation_req_n: int = Field(0, alias="dsort.creation.req.n")
    dsort_creation_resp_n: int = Field(0, alias="dsort.creation.resp.n")
    dsort_creation_resp_ns: int = Field(0, alias="dsort.creation.resp.ns")
    dsort_extract_shard_mem_n: int = Field(0, alias="dsort.extract.shard.mem.n")
    dsort_extract_shard_size: int = Field(0, alias="dsort.extract.shard.size")
    err_del_n: int = Field(0, alias="err.del.n")
    err_get_n: int = Field(0, alias="err.get.n")
    get_bps: int = Field(0, alias="get.bps")
    get_cold_n: int = Field(0, alias="get.cold.n")
    get_cold_rw_ns: int = Field(0, alias="get.cold.rw.ns")
    get_cold_size: int = Field(0, alias="get.cold.size")
    get_n: int = Field(0, alias="get.n")
    get_ns: int = Field(0, alias="get.ns")
    get_redir_ns: int = Field(0, alias="get.redir.ns")
    get_size: int = Field(0, alias="get.size")
    kalive_ns: int = Field(0, alias="kalive.ns")
    lcache_evicted_n: int = Field(0, alias="lcache.evicted.n")
    lcache_flush_cold_n: int = Field(0, alias="lcache.flush.cold.n")
    lru_evict_n: int = Field(0, alias="lru.evict.n")
    lru_evict_size: int = Field(0, alias="lru.evict.size")
    lst_n: int = Field(0, alias="lst.n")
    lst_ns: int = Field(0, alias="lst.ns")
    put_bps: int = Field(0, alias="put.bps")
    put_n: int = Field(0, alias="put.n")
    put_ns: int = Field(0, alias="put.ns")
    put_redir_ns: int = Field(0, alias="put.redir.ns")
    put_size: int = Field(0, alias="put.size")
    remote_deleted_del_n: int = Field(0, alias="remote.deleted.del.n")
    stream_in_n: int = Field(0, alias="stream.in.n")
    stream_in_size: int = Field(0, alias="stream.in.size")
    stream_out_n: int = Field(0, alias="stream.out.n")
    stream_out_size: int = Field(0, alias="stream.out.size")
    up_ns_time: int = Field(0, alias="up.ns.time")
    ver_change_n: int = Field(0, alias="ver.change.n")
    ver_change_size: int = Field(0, alias="ver.change.size")

    class Config:
        """
        Configuration for Pydantic model to use alias.
        """

        allow_population_by_field_name = True


class DiskInfo(BaseModel):
    """
    Represents the disk info of a node mountpath
    """

    used: str
    avail: str
    pct_used: float
    disks: List[str]
    mountpath_label: str
    fs: str


class DiskInfoV322(BaseModel):
    """
    Represents the disk info of a node mountpath
    """

    used: str
    avail: str
    pct_used: float
    disks: List[str]
    fs: str


class NodeCapacityV322(BaseModel):
    """
    Represents the capacity info of a node, including its mountpaths.
    """

    mountpaths: Mapping[str, DiskInfoV322] = Field({}, alias="MountPaths")
    pct_max: float
    pct_avg: float
    pct_min: float
    cs_err: str

    class Config:
        """
        Configuration for Pydantic model to use alias.
        """

        allow_population_by_field_name = True


class NodeCapacity(BaseModel):
    """
    Represents the capacity info of a node, including its mountpaths.
    """

    mountpaths: Mapping[str, DiskInfo] = Field({}, alias="MountPaths")
    pct_max: float
    pct_avg: float
    pct_min: float
    cs_err: str
    total_used: int
    total_avail: int

    class Config:
        """
        Configuration for Pydantic model to use alias.
        """

        allow_population_by_field_name = True


class NodeStatsV322(BaseModel):
    """
    Represents the response from cluster performance API
    """

    snode: Snode
    tracker: NodeTracker
    capacity: NodeCapacityV322
    rebalance_snap: Optional[Dict] = None
    status: str
    deployment: str
    ais_version: str
    build_time: str
    k8s_pod_name: str
    sys_info: Dict
    smap_version: str


class NodeStats(BaseModel):
    """
    Represents the response from cluster performance API
    """

    snode: Snode
    tracker: NodeTracker
    capacity: NodeCapacity
    rebalance_snap: Optional[Dict] = None
    status: str
    deployment: str
    ais_version: str
    build_time: str
    k8s_pod_name: str
    sys_info: Dict
    smap_version: str
    reserved1: Optional[str] = ""
    reserved2: Optional[str] = ""
    reserved3: Optional[int] = 0
    reserved4: Optional[int] = 0


# pylint: disable=too-many-instance-attributes
class NodeThroughput:
    """
    Represents the throughput stats of a node
    """

    def __init__(self, node_tracker: NodeTracker):
        get_cold_avg_size = 0
        if node_tracker.get_cold_n > 0:
            get_cold_avg_size = node_tracker.get_cold_size / node_tracker.get_cold_n

        get_avg_size = 0
        if node_tracker.get_n > 0:
            get_avg_size = node_tracker.get_bps / node_tracker.get_n

        put_avg_size = 0
        if node_tracker.put_n > 0:
            put_avg_size = node_tracker.put_bps / node_tracker.put_n

        self.get_bw = node_tracker.get_bps
        self.get_cold_n = node_tracker.get_cold_n
        self.get_cold_total_size = node_tracker.get_cold_size
        self.get_cold_avg_size = get_cold_avg_size
        self.get_n = node_tracker.get_n
        self.get_total_size = node_tracker.get_bps
        self.get_avg_size = get_avg_size
        self.put_bw = node_tracker.put_bps
        self.put_n = node_tracker.put_n
        self.put_total_size = node_tracker.put_bps
        self.put_avg_size = put_avg_size
        self.err_get_n = node_tracker.err_get_n

    def as_dict(self):
        return {
            "GET(bw)": f"{humanize.naturalsize(self.get_bw)}/s",
            "GET-COLD(n)": self.get_cold_n,
            "GET-COLD(total size)": humanize.naturalsize(self.get_cold_total_size),
            "GET-COLD(avg size)": humanize.naturalsize(self.get_cold_avg_size),
            "GET(n)": self.get_n,
            "GET(total size)": humanize.naturalsize(self.get_total_size),
            "GET(avg size)": humanize.naturalsize(self.get_avg_size),
            "PUT(bw)": f"{humanize.naturalsize(self.put_bw)}/s",
            "PUT(n)": self.put_n,
            "PUT(total size)": humanize.naturalsize(self.put_total_size),
            "PUT(avg size)": humanize.naturalsize(self.put_avg_size),
            "ERR-GET(n)": self.err_get_n,
        }


# pylint: disable=too-many-instance-attributes
class NodeLatency:
    """
    Represents the latency stats of a node
    """

    def __init__(self, node_tracker: NodeTracker):
        get_cold_avg_size = 0
        if node_tracker.get_cold_n > 0:
            get_cold_avg_size = node_tracker.get_cold_size / node_tracker.get_cold_n

        get_avg_size = 0
        if node_tracker.get_n > 0:
            get_avg_size = node_tracker.get_bps / node_tracker.get_n

        put_avg_size = 0
        if node_tracker.put_n > 0:
            put_avg_size = node_tracker.put_bps / node_tracker.put_n

        self.get_cold_n = node_tracker.get_cold_n
        self.get_cold_total_size = node_tracker.get_cold_size
        self.get_cold_avg_size = get_cold_avg_size
        self.get_n = node_tracker.get_n
        self.get_total_size = node_tracker.get_bps
        self.get_avg_size = get_avg_size
        self.put_n = node_tracker.put_n
        self.put_total_size = node_tracker.put_bps
        self.put_avg_size = put_avg_size
        self.err_get_n = node_tracker.err_get_n

    def as_dict(self):
        return {
            "GET-COLD(n)": self.get_cold_n,
            "GET-COLD(total size)": humanize.naturalsize(self.get_cold_total_size),
            "GET-COLD(avg size)": humanize.naturalsize(self.get_cold_avg_size),
            "GET(n)": self.get_n,
            "GET(total size)": humanize.naturalsize(self.get_total_size),
            "GET(avg size)": humanize.naturalsize(self.get_avg_size),
            "PUT(n)": self.put_n,
            "PUT(total size)": humanize.naturalsize(self.put_total_size),
            "PUT(avg size)": humanize.naturalsize(self.put_avg_size),
            "ERR-GET(n)": self.err_get_n,
        }


# pylint: disable=too-many-instance-attributes
class NodeCounter:
    """
    Represents the counter stats of a node
    """

    def __init__(self, node_tracker: NodeTracker):
        self.del_n = node_tracker.del_n
        self.dsort_creation_req_n = node_tracker.dsort_creation_req_n
        self.dsort_creation_resp_n = node_tracker.dsort_creation_resp_n
        self.dsort_extract_shard_mem_n = node_tracker.dsort_extract_shard_mem_n
        self.dsort_extract_shard_size = node_tracker.dsort_extract_shard_size
        self.get_cold_n = node_tracker.get_cold_n
        self.get_cold_size = node_tracker.get_cold_size
        self.get_n = node_tracker.get_n
        self.get_size = node_tracker.get_size
        self.lcache_evicted_n = node_tracker.lcache_evicted_n
        self.lcache_flush_cold_n = node_tracker.lcache_flush_cold_n
        self.evict_n = node_tracker.lru_evict_n
        self.evict_size = node_tracker.lru_evict_size
        self.list_n = node_tracker.lst_n
        self.put_n = node_tracker.put_n
        self.put_size = node_tracker.put_size
        self.remote_deleted_del_n = node_tracker.remote_deleted_del_n
        self.stream_in_n = node_tracker.stream_in_n
        self.stream_in_size = node_tracker.stream_in_size
        self.stream_out_n = node_tracker.stream_out_n
        self.stream_out_size = node_tracker.stream_out_size
        self.version_change_n = node_tracker.ver_change_n
        self.version_change_size = node_tracker.ver_change_size
        self.err_del_n = node_tracker.err_del_n
        self.err_get_n = node_tracker.err_get_n

    def as_dict(self):
        return {
            "DELETE(n)": self.del_n,
            "DSORT-CREATION-REQ(n)": self.dsort_creation_req_n,
            "DSORT-CREATION-RESP(n)": self.dsort_creation_resp_n,
            "DSORT-EXTRACT-SHARD-MEM(n)": self.dsort_extract_shard_mem_n,
            "DSORT-EXTRACT-SHARD(size)": humanize.naturalsize(
                self.dsort_extract_shard_size
            ),
            "GET-COLD(n)": self.get_cold_n,
            "GET-COLD(size)": humanize.naturalsize(self.get_cold_size),
            "GET(n)": self.get_n,
            "GET(size)": humanize.naturalsize(self.get_size),
            "LCACHE-EVICTED(n)": self.lcache_evicted_n,
            "LCACHE-FLUSH-COLD(n)": self.lcache_flush_cold_n,
            "EVICT(n)": self.evict_n,
            "EVICT(size)": humanize.naturalsize(self.evict_size),
            "LIST(n)": self.list_n,
            "PUT(n)": self.put_n,
            "PUT(size)": humanize.naturalsize(self.put_size),
            "REMOTE-DELETED-DEL(n)": self.remote_deleted_del_n,
            "STREAM-IN(n)": self.stream_in_n,
            "STREAM-IN(size)": humanize.naturalsize(self.stream_in_size),
            "STREAM-OUT(n)": self.stream_out_n,
            "STREAM-OUT(size)": humanize.naturalsize(self.stream_out_size),
            "VERSION-CHANGE(n)": self.version_change_n,
            "VERSION-CHANGE(size)": humanize.naturalsize(self.version_change_size),
            "ERR-DEL(n)": self.err_del_n,
            "ERR-GET(n)": self.err_get_n,
        }


class ClusterPerformance:
    """
    Represents the performance metrics for the cluster
    """

    def __init__(
        self,
        throughput: Mapping[str, NodeThroughput],
        latency: Mapping[str, NodeLatency],
        counters: Mapping[str, NodeCounter],
    ):
        self.throughput = throughput
        self.latency = latency
        self.counters = counters

    def as_dict(self):
        return {
            "throughput": self.throughput,
            "latency": self.latency,
            "counters": self.counters,
        }
