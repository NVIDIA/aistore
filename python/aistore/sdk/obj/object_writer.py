#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from aistore.sdk.obj.obj_file.object_file import ObjectFileWriter

# pylint: disable=too-few-public-methods

# TODO: Move all object writing methods to this class (e.g.
#       put_content, put_file, append_content, and set_custom_props).


class ObjectWriter:
    """
    Provide a way to write an object's contents and attributes.
    """

    def __init__(self, obj: "Object"):
        self._obj = obj

    def as_file(self, mode: str = "a") -> ObjectFileWriter:
        """
        Return a file-like object for writing object data.

        Args:
            mode (str): Specifies the mode in which the file is opened (defaults to 'a').
            - `'w'`: Write mode. Opens the object for writing, truncating any existing content.
                     Writing starts from the beginning of the object.
            - `'a'`: Append mode. Opens the object for appending. Existing content is preserved,
                     and writing starts from the end of the object.

        Returns:
            A file-like object for writing object data.

        Raises:
            ValueError: Invalid mode provided.
        """
        if mode not in {"w", "a"}:
            raise ValueError(f"Invalid mode: {mode}")

        return ObjectFileWriter(self._obj, mode)
