from typing import List, Optional

from pydantic import BaseModel, Field

from aistore.sdk.types import BucketModel

# See ext/dsort/metric.go for cluster-side type definitions

# pylint: disable=too-few-public-methods


class TimeStats(BaseModel):
    """
    Statistics for time spent on tasks
    """

    total_ms: str
    count: str
    min_ms: str
    max_ms: str
    avg_ms: str


class ThroughputStats(BaseModel):
    """
    Statistics on task throughput
    """

    total: str
    count: str
    min_throughput: str
    max_throughput: str
    avg_throughput: str


class DetailedStats(TimeStats, ThroughputStats):
    """
    Include fields from both time and throughput stats
    """


class PhaseInfo(BaseModel):
    """
    Stats for a specific dSort phase
    """

    started_time: str
    end_time: str
    elapsed: int
    running: bool
    finished: bool


class LocalExtraction(PhaseInfo):
    """
    Metrics for first phase of dSort
    """

    total_count: str
    extracted_count: str
    extracted_size: str
    extracted_record_count: str
    extracted_to_disk_count: str
    extracted_to_disk_size: str
    single_shard_stats: Optional[DetailedStats] = None


class MetaSorting(PhaseInfo):
    """
    Metrics for second phase of dSort
    """

    sent_stats: Optional[TimeStats] = None
    recv_stats: Optional[TimeStats] = None


class ShardCreation(PhaseInfo):
    """
    Metrics for final phase of dSort
    """

    to_create: str
    created_count: str
    moved_shard_count: str
    req_stats: Optional[TimeStats] = None
    resp_stats: Optional[TimeStats] = None
    local_send_stats: Optional[DetailedStats] = None
    local_recv_stats: Optional[DetailedStats] = None
    single_shard_stats: Optional[DetailedStats] = None


class DsortMetrics(BaseModel):
    """
    All stats for a dSort run
    """

    local_extraction: LocalExtraction
    meta_sorting: MetaSorting
    shard_creation: ShardCreation
    aborted: Optional[bool] = None
    archived: Optional[bool] = None
    description: Optional[str] = None
    warnings: Optional[List[str]] = None
    errors: Optional[List[str]] = None
    extended: Optional[bool] = None


class JobInfo(BaseModel):
    """
    Info about a dsort Job, including metrics
    """

    id: str
    src_bck: BucketModel = Field(alias="src-bck")
    dst_bck: BucketModel = Field(alias="dst-bck")
    started_time: Optional[str] = None
    finish_time: Optional[str] = None
    extracted_duration: Optional[int] = Field(
        alias="started_meta_sorting", default=None
    )
    sorting_duration: Optional[int] = Field(
        alias="started_shard_creation", default=None
    )
    creation_duration: Optional[int] = Field(
        alias="finished_shard_creation", default=None
    )
    objects: str = Field(alias="loc-objs")
    bytes: str = Field(alias="loc-bytes")
    metrics: DsortMetrics = Field(alias="Metrics")
    aborted: bool
    archived: bool
    model_config = {"populate_by_name": True}
