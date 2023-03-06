#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
from typing import Any, Dict

from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    ACT_DELETE_OBJECTS,
    ACT_PREFETCH_OBJECTS,
    HTTP_METHOD_POST,
    ACT_EVICT_OBJECTS,
    ACT_COPY_OBJECTS,
    PROVIDER_AIS,
    ACT_TRANSFORM_OBJECTS,
)
from aistore.sdk.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.object_range import ObjectRange
from aistore.sdk.types import TCMultiObj, BucketModel, CopyBckMsg, TransformBckMsg


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
        if obj_range and not isinstance(obj_range, ObjectRange):
            raise TypeError("obj_range must be of type ObjectRange")
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
            HTTP_METHOD_DELETE, ACT_DELETE_OBJECTS, value=self._determine_value()
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
            HTTP_METHOD_DELETE, ACT_EVICT_OBJECTS, value=self._determine_value()
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
            HTTP_METHOD_POST, ACT_PREFETCH_OBJECTS, value=self._determine_value()
        ).text

    def copy(
        self,
        to_bck: str,
        to_provider: str = PROVIDER_AIS,
        continue_on_error: bool = False,
    ):
        """
        Copies a list or range of objects in a bucket

        Args:
            to_bck (str): Name of the destination bucket
            to_provider (str, optional): Name of destination bucket provider
            continue_on_error (bool, optional): Whether to continue if there is an error copying a single object

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
        to_bck = BucketModel(name=to_bck, provider=to_provider)
        copy_msg = CopyBckMsg(prefix="", dry_run=False, force=False)
        value = TCMultiObj(
            to_bck=to_bck,
            copy_msg=copy_msg,
            object_selection=self._determine_value(),
            continue_on_err=continue_on_error,
        ).as_dict()
        return self.bck.make_request(
            HTTP_METHOD_POST, ACT_COPY_OBJECTS, value=value
        ).text

    # pylint: disable=too-many-arguments
    def transform(
        self,
        to_bck: str,
        etl_name: str,
        timeout: str = DEFAULT_ETL_TIMEOUT,
        to_provider: str = PROVIDER_AIS,
        continue_on_error: bool = False,
    ):
        """
        Performs ETL operation on a list or range of objects in a bucket, placing the results in the destination bucket

        Args:
            to_bck (str): Name of the destination bucket
            etl_name (str): Name of existing ETL to apply
            timeout (str): Timeout of the ETL job (e.g. 5m for 5 minutes)
            to_provider (str, optional): Name of destination bucket provider
            continue_on_error (bool, optional): Whether to continue if there is an error transforming a single object

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
        to_bck = BucketModel(name=to_bck, provider=to_provider)
        transform_msg = TransformBckMsg(etl_name=etl_name, timeout=timeout)
        value = TCMultiObj(
            to_bck=to_bck,
            transform_msg=transform_msg,
            object_selection=self._determine_value(),
            continue_on_err=continue_on_error,
        ).as_dict()
        return self.bck.make_request(
            HTTP_METHOD_POST, ACT_TRANSFORM_OBJECTS, value=value
        ).text

    def _determine_value(self) -> Dict[str, Any]:
        if self.obj_names:
            return {"objnames": self.obj_names}
        if self.obj_range:
            return {"template": str(self.obj_range)}
        if self.obj_template:
            return {"template": self.obj_template}
        return {}
