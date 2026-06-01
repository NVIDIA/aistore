#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
# Mirror of the Go xaction kind table (`xact/api_table.go`): kind constants
# plus the idle/known sets and predicates used by Job.wait() dispatch.
#

from aistore.sdk.const import (
    ACT_ARCHIVE_OBJECTS,
    ACT_BLOB_DOWNLOAD,
    ACT_COPY_BCK,
    ACT_COPY_OBJECTS,
    ACT_CREATE_NBI,
    ACT_DELETE_OBJECTS,
    ACT_ETL_BCK,
    ACT_EVICT_OBJECTS,
    ACT_EVICT_REMOTE_BCK,
    ACT_LIST,
    ACT_MOVE_BCK,
    ACT_PREFETCH_OBJECTS,
    ACT_PROMOTE,
    ACT_SUMMARY_BCK,
    ACT_TRANSFORM_OBJECTS,
)

# Kind constants. Reuse the ACT_* wire strings from const.py where they exist;
# the rest are xaction-only kinds.
XACT_KIND_REBALANCE = "rebalance"
XACT_KIND_RESILVER = "resilver"
XACT_KIND_ELECTION = "election"
XACT_KIND_LRU = "lru"
XACT_KIND_STORE_CLEANUP = "cleanup-store"
XACT_KIND_SUMMARY_BCK = ACT_SUMMARY_BCK
XACT_KIND_RECHUNK = "rechunk"
XACT_KIND_INDEX_SHARD = "index-shard"
XACT_KIND_EC_GET = "ec-get"
XACT_KIND_EC_PUT = "ec-put"
XACT_KIND_EC_RESP = "ec-resp"
XACT_KIND_EC_ENCODE = "ec-encode"
XACT_KIND_PUT_COPIES = "put-copies"
XACT_KIND_MAKE_NCOPIES = "make-n-copies"
XACT_KIND_ARCHIVE = ACT_ARCHIVE_OBJECTS
XACT_KIND_COPY_OBJECTS = ACT_COPY_OBJECTS
XACT_KIND_ETL_OBJECTS = ACT_TRANSFORM_OBJECTS
XACT_KIND_DELETE_OBJECTS = ACT_DELETE_OBJECTS
XACT_KIND_EVICT_OBJECTS = ACT_EVICT_OBJECTS
XACT_KIND_PREFETCH_OBJECTS = ACT_PREFETCH_OBJECTS
XACT_KIND_BLOB_DL = ACT_BLOB_DOWNLOAD
XACT_KIND_DOWNLOAD = "download"
XACT_KIND_DSORT = "dsort"
XACT_KIND_PROMOTE = ACT_PROMOTE
XACT_KIND_MOVE_BCK = ACT_MOVE_BCK
XACT_KIND_COPY_BCK = ACT_COPY_BCK
XACT_KIND_ETL_BCK = ACT_ETL_BCK
XACT_KIND_ETL_INLINE = "etl-inline"
XACT_KIND_EVICT_REMOTE_BCK = ACT_EVICT_REMOTE_BCK
XACT_KIND_LIST = ACT_LIST
XACT_KIND_GET_BATCH = "get-batch"
XACT_KIND_CREATE_NBI = ACT_CREATE_NBI
XACT_KIND_LOAD_LOM_CACHE = "load-lom-cache"


# Kinds that idle between requests instead of terminating (Descriptor.Idles).
# Job.wait() waits for idle on these.
IDLE_KINDS = frozenset(
    {
        XACT_KIND_EC_GET,
        XACT_KIND_EC_PUT,
        XACT_KIND_EC_RESP,
        XACT_KIND_PUT_COPIES,
        XACT_KIND_ARCHIVE,
        XACT_KIND_COPY_OBJECTS,
        XACT_KIND_ETL_OBJECTS,
        XACT_KIND_DOWNLOAD,
        XACT_KIND_LIST,
        XACT_KIND_GET_BATCH,
    }
)


# All kinds in `xact.Table`. A kind not listed here is not an error: Job.wait()
# falls back to terminal wait (polls for a non-zero end time).
KNOWN_KINDS = frozenset(
    {
        XACT_KIND_REBALANCE,
        XACT_KIND_RESILVER,
        XACT_KIND_ELECTION,
        XACT_KIND_LRU,
        XACT_KIND_STORE_CLEANUP,
        XACT_KIND_SUMMARY_BCK,
        XACT_KIND_RECHUNK,
        XACT_KIND_INDEX_SHARD,
        XACT_KIND_EC_GET,
        XACT_KIND_EC_PUT,
        XACT_KIND_EC_RESP,
        XACT_KIND_EC_ENCODE,
        XACT_KIND_PUT_COPIES,
        XACT_KIND_MAKE_NCOPIES,
        XACT_KIND_ARCHIVE,
        XACT_KIND_COPY_OBJECTS,
        XACT_KIND_ETL_OBJECTS,
        XACT_KIND_DELETE_OBJECTS,
        XACT_KIND_EVICT_OBJECTS,
        XACT_KIND_PREFETCH_OBJECTS,
        XACT_KIND_BLOB_DL,
        XACT_KIND_DOWNLOAD,
        XACT_KIND_DSORT,
        XACT_KIND_PROMOTE,
        XACT_KIND_MOVE_BCK,
        XACT_KIND_COPY_BCK,
        XACT_KIND_ETL_BCK,
        XACT_KIND_ETL_INLINE,
        XACT_KIND_EVICT_REMOTE_BCK,
        XACT_KIND_LIST,
        XACT_KIND_GET_BATCH,
        XACT_KIND_CREATE_NBI,
        XACT_KIND_LOAD_LOM_CACHE,
    }
)


def idles_before_finishing(kind: str) -> bool:
    """
    True if the xaction kind idles between requests rather than terminating
    (mirrors Go `xact.IdlesBeforeFinishing`). Unknown kinds return False.
    """
    return kind in IDLE_KINDS


def is_valid_kind(kind: str) -> bool:
    """True if `kind` is a known entry in the Go `xact.Table`."""
    return kind in KNOWN_KINDS
