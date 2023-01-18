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
        obj_range (ObjectRange, optional): Range defining which object names in the bucket should be included
        obj_template (str, optional): String argument to pass as template value directly to api
    """

    def __init__(
        self,
        bck,
        obj_names: list = None,
        obj_range: ObjectRange = None,
        obj_template: str = None,
    ):
        self.bck = bck
        num_args = sum(
            1 if x is not None else 0 for x in [obj_names, obj_range, obj_template]
        )
        if num_args != 1:
            raise ValueError(
                "ObjectGroup accepts one and only one of: obj_names, obj_range, or obj_template"
            )
        self.obj_names = obj_names
        self.obj_range = obj_range
        self.obj_template = obj_template

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
            Job ID (as str) that can be used to check the status of the operation

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
            Job ID (as str) that can be used to check the status of the operation

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
            Job ID (as str) that can be used to check the status of the operation

        """
        self.bck.verify_cloud_bucket()
        return self.bck.make_request(
            HTTP_METHOD_POST, ACT_PREFETCH_MULTIPLE_OBJ, value=self._determine_value()
        ).text

    def _determine_value(self):
        if self.obj_names:
            return {"objnames": self.obj_names}
        if self.obj_range:
            return {"template": str(self.obj_range)}
        if self.obj_template:
            return {"template": self.obj_template}
        return None
