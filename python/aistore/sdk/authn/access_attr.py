#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from enum import IntFlag


class AccessAttr(IntFlag):
    """
    AccessAttr defines permissions as bitwise flags for access control (for more details, refer to the Go API).
    """

    GET = 1 << 0
    OBJ_HEAD = 1 << 1
    PUT = 1 << 2
    APPEND = 1 << 3
    OBJ_DELETE = 1 << 4
    OBJ_MOVE = 1 << 5
    PROMOTE = 1 << 6
    OBJ_UPDATE = 1 << 7
    BCK_HEAD = 1 << 8
    OBJ_LIST = 1 << 9
    # TODO: Not implemented in SDK
    PATCH = 1 << 10
    # TODO: Not implemented in SDK
    BCK_SET_ACL = 1 << 11
    LIST_BUCKETS = 1 << 12
    SHOW_CLUSTER = 1 << 13
    CREATE_BUCKET = 1 << 14
    DESTROY_BUCKET = 1 << 15
    MOVE_BUCKET = 1 << 16
    ADMIN = 1 << 17

    # Derived Roles
    ACCESS_RO = GET | OBJ_HEAD | LIST_BUCKETS | BCK_HEAD | OBJ_LIST
    ACCESS_RW = ACCESS_RO | PUT | APPEND | OBJ_DELETE | OBJ_MOVE
    ACCESS_SU = (
        ACCESS_RW
        | PROMOTE
        | OBJ_UPDATE
        | PATCH
        | BCK_SET_ACL
        | SHOW_CLUSTER
        | CREATE_BUCKET
        | DESTROY_BUCKET
        | MOVE_BUCKET
        | ADMIN
    )

    @staticmethod
    def describe(perms: int) -> str:
        """
        Returns a comma-separated string describing the permissions based on the provided bitwise flags.
        """
        access_op = {v.value: v.name for v in AccessAttr}
        return ",".join(op for perm, op in access_op.items() if perms & perm)
