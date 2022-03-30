#
# Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
#


class Bck:  #pylint: disable=too-few-public-methods,unused-variable
    def __init__(self, name, provider="ais", ns="") -> None:
        self.provider = provider
        self.name = name
        self.namespace = ns


class ActionMsg:  #pylint: disable=unused-variable
    def __init__(self, action, name="", value=None) -> None:
        self.action = action
        self.name = name
        self.value = value

    def json(self):
        return _to_json(self)


def _to_json(value):
    result = {}
    for k in value.__dict__:
        val = value.__dict__[k]
        if hasattr(val, "__dict__"):
            val = _to_json(val)
        result[k] = val
    return result
