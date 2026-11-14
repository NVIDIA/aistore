#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

from aistore.sdk.errors import _pickle_exception


class ObjectFileReaderMaxResumeError(Exception):
    """
    Raised when ObjectFileReader has exceeded the max number of stream resumes for an object
    """

    def __init__(self, err, max_retries):
        self.original_error = err
        super().__init__(
            f"ObjectFileReader exceeded max number of stream resumptions: {max_retries}"
        )

    def __reduce__(self):
        return _pickle_exception(self)


class ObjectFileReaderUnexpectedEOF(Exception):
    """
    Raised internally when an object stream ends before the expected byte position.
    """

    def __init__(self, actual: int, expected: int):
        self.actual = actual
        self.expected = expected
        super().__init__(
            f"Object stream ended early at byte {actual}; expected byte {expected}"
        )

    def __reduce__(self):
        return _pickle_exception(self)


class ObjectFileReaderStreamError(Exception):
    """
    Raised when ObjectFileReader fails to establish a stream for an object
    """

    def __init__(self, err):
        self.original_error = err
        super().__init__("ObjectFileReader failed to establish stream connection")
