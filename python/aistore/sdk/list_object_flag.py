#
# Copyright (c) 2023-2026, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations
from enum import Enum

from typing import List


class ListObjectFlag(Enum):
    """
    Bit-position flags for listing objects in a bucket.

    Each value is a bit position (not a bitmask). Use `join_flags()` to combine
    multiple flags into a single bitmask integer.

    Must stay in sync with Go: `api/apc/lsmsg.go` (`LsCached`, `LsMissing`, ...).
    """

    # Only list in-cluster ("cached") objects from a remote bucket.
    # See also: NOT_CACHED (strict opposite).
    CACHED = 0

    # Include missing main objects (where a copy exists but the main is absent).
    MISSING = 1

    # Include objects marked for deletion (not implemented yet).
    DELETED = 2

    # Expand archives (tar, zip, etc.) as virtual directories.
    ARCH_DIR = 3

    # Return only object names and statuses (skip all other properties).
    NAME_ONLY = 4

    # Return only object names and sizes (minor speedup over full listing).
    NAME_SIZE = 5

    # Same as fltPresence == apc.Present: only list buckets already present
    # in the cluster metadata (skip HEAD-ing the remote).
    BCK_PRESENT = 6

    # Do not HEAD the remote bucket. Primarily for GCP buckets with
    # anonymous-access ACL policies that may reject HEAD requests with 401/403.
    # See also: `QparamDontHeadRemote` in Go.
    DONT_HEAD_REMOTE = 7

    # List remote buckets without adding them to AIS cluster metadata.
    # See also: `QparamDontAddRemote` in Go.
    DONT_ADD_REMOTE = 8

    # Strict opposite of CACHED: list only objects that are NOT cached in-cluster.
    NOT_CACHED = 9

    # (Optimization, not used yet) List only remote properties in a
    # pass-through fashion: a single target forwards the request to the
    # remote backend and delivers results as-is.
    ONLY_REMOTE_PROPS = 10

    # List objects without recursion (POSIX-style, non-recursive).
    # See also: feature flag `feat.DontOptimizeVirtualDir`.
    NO_RECURSION = 11

    # Bidirectional diff (remote <-> in-cluster). Requires remote buckets
    # with versioning metadata. Checks whether the remote version exists
    # and whether it differs from the in-cluster copy.
    DIFF = 12

    # Do not return virtual subdirectories as listing entries.
    NO_DIRS = 13

    # Internal: the caller is the S3 compatibility API layer.
    IS_S3 = 14

    # List native bucket inventory (NBI) snapshot instead of the remote bucket.
    # PageSize (if non-zero) is best-effort and approximate.
    NBI = 15

    @staticmethod
    def join_flags(flags: List[ListObjectFlag]) -> int:
        """
        Combine a list of ListObjectFlag enums into a single bitmask integer.

        Args:
            flags (List[ListObjectFlag]): List of flags to combine.

        Returns:
            int: Combined bitmask. E.g., NOT_CACHED (9) + NAME_ONLY (4)
                = 2^9 + 2^4 = 528.
        """
        res = 0
        for flag in flags:
            res = res ^ 2**flag.value
        return res
