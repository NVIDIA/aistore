#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#
from enum import IntEnum


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
    FLT_PRESENT_CLUSTER = 4  # objects: present anywhere/anyhow _in_ the cluster as: replica, ec-slices, misplaced
    FLT_EXISTS_OUTSIDE = 5  # not present - exists _outside_ cluster
