#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#


class AISError(Exception):
    """
    Raised when an error is encountered from a query to the AIS cluster
    """

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"STATUS:{status_code}, MESSAGE:{message}")


# pylint: disable=unused-variable
class InvalidBckProvider(Exception):
    """
    Raised when the bucket provider is invalid for the requested operation
    """

    def __init__(self, provider):
        super().__init__(f"Invalid bucket provider {provider}")


# pylint: disable=unused-variable
class ErrRemoteBckNotFound(AISError):
    """
    Raised when a remote bucket its required and missing for the requested operation
    """

    def __init__(self, status_code, message):
        super().__init__(status_code=status_code, message=message)


# pylint: disable=unused-variable
class ErrBckNotFound(AISError):
    """
    Raised when a bucket is expected and not found
    """

    def __init__(self, status_code, message):
        super().__init__(status_code=status_code, message=message)


# pylint: disable=unused-variable
class Timeout(Exception):
    """
    Raised when an operation takes too long to complete
    """

    def __init__(self, action):
        super().__init__(f"{action} timed out")


# pylint: disable=unused-variable
class InvalidObjectRangeIndex(Exception):
    """
    Raised when incorrect range parameters are passed when creating an ObjectRange
    """

    def __init__(self, message):
        super().__init__(f"Invalid argument provided for object range index: {message}")
