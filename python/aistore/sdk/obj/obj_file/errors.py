#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#


class ObjectFileReaderMaxResumeError(Exception):
    """
    Raised when ObjectFileReader has exceeded the max number of stream resumes for an object
    """

    def __init__(self, err, max_retries):
        self.original_error = err
        super().__init__(
            f"ObjectFileReader exceeded max number of stream resumptions: {max_retries}"
        )


class ObjectFileReaderStreamError(Exception):
    """
    Raised when ObjectFileReader fails to establish a stream for an object
    """

    def __init__(self, err):
        self.original_error = err
        super().__init__("ObjectFileReader failed to establish stream connection")
