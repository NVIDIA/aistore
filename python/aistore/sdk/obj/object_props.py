#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

from aistore.sdk.const import (
    AIS_BCK_NAME,
    AIS_BCK_PROVIDER,
    AIS_OBJ_NAME,
)
from aistore.sdk.obj.object_attributes import ObjectAttributes


class ObjectProps(ObjectAttributes):
    """
    Object metadata together with the object's name, bucket, and provider.

    `Object.props` and `Object.props_cached` return this type. Other metadata
    fields are defined by `ObjectAttributes`.

    Args:
        response_headers (CaseInsensitiveDict, optional): Response header dict containing object attributes
    """

    @property
    def bucket_name(self):
        """
        Name of object's bucket
        """
        return self._response_headers.get(AIS_BCK_NAME, "")

    @property
    def bucket_provider(self):
        """
        Provider of object's bucket.
        """
        return self._response_headers.get(AIS_BCK_PROVIDER, "")

    @property
    def name(self) -> str:
        """
        Name of the object.
        """
        return self._response_headers.get(AIS_OBJ_NAME, "")
