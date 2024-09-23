#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import List
from aistore.sdk.const import (
    AIS_BCK_NAME,
    AIS_BCK_PROVIDER,
    AIS_OBJ_NAME,
    AIS_LOCATION,
    AIS_MIRROR_PATHS,
    AIS_MIRROR_COPIES,
    AIS_PRESENT,
)
from aistore.sdk.obj.object_attributes import ObjectAttributes


class ObjectProps(ObjectAttributes):
    """
    Represents the attributes parsed from the response headers returned from an API call to get an object.
    Extends ObjectAtributes and is a superset of that class.

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

    @property
    def location(self) -> str:
        """
        Location of the object.
        """
        return self._response_headers.get(AIS_LOCATION, "")

    @property
    def mirror_paths(self) -> List[str]:
        """
        List of mirror paths.
        """
        return self._response_headers.get(AIS_MIRROR_PATHS, []).strip("[]").split(",")

    @property
    def mirror_copies(self) -> int:
        """
        Number of mirror copies.
        """
        return int(self._response_headers.get(AIS_MIRROR_COPIES, 0))

    @property
    def present(self) -> bool:
        """
        True if object is present in cluster.
        """
        return self._response_headers.get(AIS_PRESENT, "") == "true"
