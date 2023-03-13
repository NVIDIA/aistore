#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#

from typing import Dict, Iterator

from aistore.sdk import utils
from aistore.sdk.multiobj.object_collection import ObjectCollection


class ObjectTemplate(ObjectCollection):
    """
    A collection of object names specified by a template in the bash brace expansion format
    """

    def __init__(self, template: str):
        self._template = template

    def __iter__(self) -> Iterator[str]:
        return utils.expand_braces(self._template)

    def get_value(self) -> Dict[str, str]:
        return {"template": self._template}
