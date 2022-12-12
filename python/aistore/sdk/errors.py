#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#


class AISError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"STATUS:{status_code}, MESSAGE:{message}")


# pylint: disable=unused-variable
class InvalidBckProvider(Exception):
    def __init__(self, provider):
        super().__init__(f"Invalid bucket provider {provider}")


# pylint: disable=unused-variable
class ErrRemoteBckNotFound(AISError):
    def __init__(self, status_code, message):
        super().__init__(status_code=status_code, message=message)


# pylint: disable=unused-variable
class ErrBckNotFound(AISError):
    def __init__(self, status_code, message):
        super().__init__(status_code=status_code, message=message)


# pylint: disable=unused-variable
class Timeout(Exception):
    def __init__(self, action):
        super().__init__(f"{action} timed out")
