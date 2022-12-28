#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=unused-variable
class ObjectRange:
    def __init__(
        self,
        prefix: str,
        min_index: int,
        max_index: int,
        step: int = 1,
        suffix: str = "",
    ):
        """
        Args:
            prefix (str): Prefix contained in all names of objects
            min_index (int): Starting index in the name of objects
            max_index (int): Last index in the name of all objects
            step (int, optional): Size of iterator steps between each item
            suffix (str, optional): Suffix at the end of all object names
        """
        self._prefix = prefix
        self._min_index = min_index
        self._max_index = max_index
        self._step = step
        self._suffix = suffix

    def string_template(self) -> str:
        return "{prefix}{{{min_index}..{max_index}..{step}}}{suffix}".format(
            prefix=self._prefix,
            min_index=str(self._min_index),
            max_index=str(self._max_index),
            step=str(self._step),
            suffix=self._suffix,
        )
