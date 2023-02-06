#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
from aistore.sdk.errors import InvalidObjectRangeIndex


# pylint: disable=unused-variable,too-few-public-methods
class ObjectRange:
    """
    Class representing a range of object names

    Args:
        prefix (str): Prefix contained in all names of objects
        min_index (int): Starting index in the name of objects
        max_index (int): Last index in the name of all objects
        pad_width (int): Left-pad indices with zeros up to the width provided, e.g. pad_width = 3 will transform 1
            to 001
        step (int, optional): Size of iterator steps between each item
        suffix (str, optional): Suffix at the end of all object names
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        prefix: str,
        min_index: int,
        max_index: int,
        pad_width: int = 0,
        step: int = 1,
        suffix: str = "",
    ):
        self._prefix = prefix
        self._step = step
        self._suffix = suffix
        self._validate_indices(min_index, max_index, pad_width)
        self._min_index = str(min_index).zfill(pad_width)
        self._max_index = str(max_index).zfill(pad_width)

    @staticmethod
    def _validate_indices(min_index, max_index, pad_width):
        """
        Validate the indices passed to create a range: min_index < max_index and pad_width (if set) can fit the indices
            provided.

        Raises:
            InvalidObjectRangeIndex: If the indices passed to the range are not valid
        """
        if min_index >= max_index:
            raise InvalidObjectRangeIndex(f"{min_index} must be less than {max_index}")
        if pad_width != 0 and len(str(min_index)) > pad_width:
            raise InvalidObjectRangeIndex(
                f"Number of digits in min index {min_index} must not be greater than pad width {pad_width}"
            )

    def __str__(self) -> str:
        return f"{self._prefix}{{{self._min_index}..{self._max_index}..{self._step}}}{self._suffix}"
