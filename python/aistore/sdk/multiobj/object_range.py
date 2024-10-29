#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
import re

from typing import Iterator

from aistore.sdk import utils
from aistore.sdk.errors import InvalidObjectRangeIndex
from aistore.sdk.multiobj.object_collection import ObjectCollection


# pylint: disable=too-few-public-methods
class ObjectRange(ObjectCollection):
    """
    Class representing a range of object names

    Args:
        prefix (str): Prefix contained in all names of objects
        min_index (int): Starting index in the name of objects
        max_index (int): Last index in the name of all objects
        pad_width (int, optional): Left-pad indices with zeros up to the width provided, e.g. pad_width = 3 will
            transform 1 to 001
        step (int, optional): Size of iterator steps between each item
        suffix (str, optional): Suffix at the end of all object names
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        prefix: str,
        min_index: int = None,
        max_index: int = None,
        pad_width: int = 0,
        step: int = 1,
        suffix: str = "",
    ):
        self._prefix = prefix
        self._step = step
        self._suffix = suffix
        self._validate_indices(min_index, max_index, pad_width)
        min_set = isinstance(min_index, int)
        if self._suffix and not min_set:
            raise ValueError("Suffix cannot be used without indices")
        self._min_index = str(min_index).zfill(pad_width) if min_set else None
        self._max_index = (
            str(max_index).zfill(pad_width) if isinstance(min_index, int) else None
        )

    @classmethod
    def from_string(cls, range_string: str):
        """
        Construct an ObjectRange instance from a valid range string like 'input-{00..99..1}.txt'

        Args:
            range_string (str): The range string to parse

        Returns:
            ObjectRange: An instance of the ObjectRange class
        """
        pattern = r"(?P<prefix>.*)\{(?P<min_index>\d+)\.\.(?P<max_index>\d+)(?:\.\.(?P<step>\d+))?\}(?P<suffix>.*)?"
        match = re.match(pattern, range_string)
        if not match:
            raise ValueError(f"Invalid range string format: {range_string}")
        parts = match.groupdict()
        min_index = int(parts["min_index"])
        max_index = int(parts["max_index"])
        prefix = parts["prefix"]
        step = int(parts["step"]) if parts["step"] else 1
        suffix = parts["suffix"] if parts["suffix"] else ""
        pad_width = len(parts["min_index"])

        return cls(prefix, min_index, max_index, pad_width, step, suffix)

    @staticmethod
    def _validate_indices(min_index, max_index, pad_width):
        """
        Validate the indices passed to create a range: min_index < max_index and pad_width (if set) can fit the indices
            provided.

        Raises:
            InvalidObjectRangeIndex: If the indices passed to the range are not valid
        """
        indices = [isinstance(min_index, int), isinstance(max_index, int)]
        if not all(indices):
            if any(indices):
                raise InvalidObjectRangeIndex(
                    "Provide both min_index and max_index or neither"
                )
            return

        if min_index >= max_index:
            raise InvalidObjectRangeIndex(f"{min_index} must be less than {max_index}")
        if pad_width != 0 and len(str(min_index)) > pad_width:
            raise InvalidObjectRangeIndex(
                f"Number of digits in min index {min_index} must not be greater than pad width {pad_width}"
            )

    def _indices_set(self) -> bool:
        if all([self._min_index, self._max_index]):
            return True
        return False

    def __str__(self) -> str:
        if self._indices_set():
            return f"{self._prefix}{{{self._min_index}..{self._max_index}..{self._step}}}{self._suffix}"
        return f"{self._prefix}"

    def __iter__(self) -> Iterator[str]:
        if not self._indices_set():
            raise RuntimeError("Cannot iterate over object range with no indices")
        return utils.expand_braces(str(self))

    def get_value(self):
        return {"template": str(self)}
