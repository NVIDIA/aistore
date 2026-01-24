#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#
from enum import IntEnum


class Colocation(IntEnum):
    """
    Colocation hint for Batch (GetBatch) API optimization.

    See api/apc/ml.go for Go equivalent (ColocLevel).

    NONE - no optimization; suitable for uniformly distributed data lakes
           where objects are spread evenly across all targets
    TARGET_AWARE - indicates that objects in this batch are collocated on few targets;
           proxy will compute HRW distribution and select the optimal distributed
           target (DT) to minimize cross-cluster data movement
    TARGET_AND_SHARD_AWARE - implies TARGET_AWARE, plus indicates that archpaths are
           collocated in few shards; enables additional optimization for archive handle reuse

    E.g., use TARGET_AWARE or TARGET_AND_SHARD_AWARE when input TARs were constructed
    to match requested batches.
    """

    NONE = 0
    TARGET_AWARE = 1
    TARGET_AND_SHARD_AWARE = 2


class FLTPresence(IntEnum):
    """
    An enum representing the existence/lack thereof of buckets and objects in the AIS cluster.

    FLT_EXISTS - object or bucket exists inside and/or outside cluster
    FLT_EXISTS_NO_PROPS - same as FLT_EXISTS but no need to return summary
    FLT_PRESENT - bucket is present or object is present and properly
    located
    FLT_PRESENT_NO_PROPS - same as FLT_PRESENT but no need to return summary
    FLT_PRESENT_CLUSTER - objects present anywhere/how in
    the cluster as replica, ec-slices, misplaced
    FLT_EXISTS_OUTSIDE - not present; exists outside cluster
    """

    FLT_EXISTS = 0  # (object | bucket) exists inside and/or outside cluster
    FLT_EXISTS_NO_PROPS = 1  # same as above but no need to return props/info
    FLT_PRESENT = 2  # bucket: is present | object: present and properly located
    FLT_PRESENT_NO_PROPS = 3  # same as above but no need to return props/info

    # objects are present on any target, any disk inside the cluster
    # (including replicas, EC slices, misplaced, or rebalancing)
    FLT_PRESENT_CLUSTER = 4
    FLT_EXISTS_OUTSIDE = 5  # not present - exists _outside_ cluster
