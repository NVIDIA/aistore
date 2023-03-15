from __future__ import annotations
from enum import Enum

from typing import List


class ListObjectFlag(Enum):
    """
    Flags to pass when listing objects in a bucket.
    See api/apc/lsmsg.go
    """

    CACHED = 0
    ALL = 1
    DELETED = 2
    ARCH_DIR = 3
    NAME_ONLY = 4
    NAME_SIZE = 5
    DONT_HEAD_REMOTE = 6
    TRY_HEAD_REMOTE = 7
    DONT_ADD_REMOTE = 8
    USE_CACHE = 9
    ONLY_REMOTE_PROPS = 10

    @staticmethod
    def join_flags(flags: List[ListObjectFlag]) -> int:
        """
        Take a list of ListObjectFlag enums and return the integer value of the combined flags
        Args:
            flags: List of ListObjectFlag enums

        Returns:
            A single bit string with each digit corresponding to the flag's value from the right.
                E.g. USE_CACHE = 9 and NAME_ONLY = 4 so if both flags are passed, the result will be 2^9 + 2^4 = 528

        """
        res = 0
        for flag in flags:
            res = res ^ 2**flag.value
        return res
