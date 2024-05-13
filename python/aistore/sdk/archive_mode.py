#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from enum import Enum


class ArchiveMode(Enum):
    """
    Archive mode for getting files/objects from an archive in a bucket
    See `MatchMode` enum in the cmn/archive/read.go
    """

    REGEXP = "regexp"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    SUBSTR = "substr"
    WDSKEY = "wdskey"
