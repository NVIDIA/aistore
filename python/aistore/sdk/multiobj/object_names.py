#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
from typing import List, Iterator, Dict

from aistore.sdk.multiobj.object_collection import ObjectCollection


class ObjectNames(ObjectCollection):
    """
    A collection of object names, provided as a list of strings

    Args:
         names (List[str]): A list of object names

    """

    def __init__(self, names: List[str]):
        self._names = names

    def __str__(self):
        return self._names

    def get_value(self) -> Dict[str, any]:
        return {"objnames": self._names}

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)
