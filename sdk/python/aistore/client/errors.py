#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#


# pylint: disable=unused-variable
class InvalidBckProvider(Exception):
    def __init__(self, provider):
        super().__init__(f"Invalid bucket provider {provider}")


# pylint: disable=unused-variable
class Timeout(Exception):
    def __init__(self, action):
        super().__init__(f"{action} timed out")
