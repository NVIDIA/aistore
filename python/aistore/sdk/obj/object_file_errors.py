#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#


class ObjectFileMaxResumeError(Exception):
    """
    Raised when ObjectFile has exceeded the max number of stream resumes for an object
    """

    def __init__(self, err, max_retries):
        self.original_error = err
        super().__init__(
            f"Object file exceeded max number of stream resumptions: {max_retries}"
        )


class ObjectFileStreamError(Exception):
    """
    Raised when ObjectFile fails to establish a stream for an object
    """

    def __init__(self, err):
        self.original_error = err
        super().__init__("Object file failed to establish stream connection")
