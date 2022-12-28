#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    ACT_DELETE_MULTIPLE_OBJ,
    ACT_PREFETCH_MULTIPLE_OBJ,
    HTTP_METHOD_POST,
    ACT_EVICT_MULTIPLE_OBJ,
)
from aistore.sdk.object_range import ObjectRange


# pylint: disable=unused-variable
class ObjectGroup:
    """
    A class representing multiple objects within the same bucket. Only one of obj_names or obj_range should be provided.

    Args:
        bck (Bucket): Bucket the objects belong to
        obj_names (list[str], optional): List of object names to include in this collection
        obj_range (ObjectRange): Range defining which object names in the bucket should be included
    """

    def __init__(self, bck, obj_names: list = None, obj_range: ObjectRange = None):
        self.bck = bck
        if obj_names and obj_range:
            raise ValueError(
                "ObjectGroup only accepts either a list of objects or an ObjectRange"
            )
        self.obj_names = obj_names
        self.obj_range = obj_range

    def delete(self):
        """
        Deletes a list or range of objects in a bucket

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore

        Returns:
            Xaction id (as str) that can be used to check the status of the operation

        """

        return self.bck.make_request(
            HTTP_METHOD_DELETE, ACT_DELETE_MULTIPLE_OBJ, value=self._determine_value()
        ).text

    def evict(self):
        """
        Evicts a list or range of objects in a bucket so that they are no longer cached in AIS
        NOTE: only Cloud buckets can be evicted.

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore

        Returns:
            Xaction id (as str) that can be used to check the status of the operation

        """
        self.bck.verify_cloud_bucket()
        return self.bck.make_request(
            HTTP_METHOD_DELETE, ACT_EVICT_MULTIPLE_OBJ, value=self._determine_value()
        ).text

    def prefetch(self):
        """
        Prefetches a list or range of objects in a bucket so that they are cached in AIS
        NOTE: only Cloud buckets can be prefetched.

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore

        Returns:
            Xaction id (as str) that can be used to check the status of the operation

        """
        self.bck.verify_cloud_bucket()
        return self.bck.make_request(
            HTTP_METHOD_POST, ACT_PREFETCH_MULTIPLE_OBJ, value=self._determine_value()
        ).text

    def _determine_value(self):
        if self.obj_names:
            return {"objnames": self.obj_names}
        if self.obj_range:
            return {"template": self.obj_range.string_template()}
        return None
