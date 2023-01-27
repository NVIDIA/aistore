#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from typing import Optional, List, Callable

from aistore.sdk.types import BucketEntry


# pylint: disable=unused-variable
class ObjectIterator:
    """
    Represents an iterable that will fetch all objects from a bucket, querying as needed with the specified function

    Args:
        list_objects (Callable): Function returning a BucketList from an AIS cluster
    """

    _fetched: Optional[List[BucketEntry]] = []
    _token: str = ""
    _uuid: str = ""

    def __init__(self, list_objects: Callable):
        self._list_objects = list_objects

    def __iter__(self):
        return self

    def __next__(self) -> BucketEntry:
        # Iterator is exhausted.
        if len(self._fetched) == 0 and self._token == "" and self._uuid != "":
            raise StopIteration
        # Read the next page of objects.
        if len(self._fetched) == 0:
            resp = self._list_objects(uuid=self._uuid, token=self._token)
            self._fetched = resp.get_entries()
            self._uuid = resp.uuid
            self._token = resp.continuation_token
            # Empty page and token mean no more objects left.
            if len(self._fetched) == 0 and self._token == "":
                raise StopIteration
        return self._fetched.pop(0)
