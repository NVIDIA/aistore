#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
import logging
from typing import List, Iterable

from aistore.sdk.ais_source import AISSource
from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    ACT_DELETE_OBJECTS,
    ACT_PREFETCH_OBJECTS,
    ACT_EVICT_OBJECTS,
    ACT_COPY_OBJECTS,
    ACT_TRANSFORM_OBJECTS,
    ACT_ARCHIVE_OBJECTS,
)
from aistore.sdk.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.multiobj.object_names import ObjectNames
from aistore.sdk.multiobj.object_range import ObjectRange
from aistore.sdk.multiobj.object_template import ObjectTemplate
from aistore.sdk.types import (
    TCMultiObj,
    CopyBckMsg,
    TransformBckMsg,
    TCBckMsg,
    ArchiveMultiObj,
)


# pylint: disable=unused-variable
class ObjectGroup(AISSource):
    """
    A class representing multiple objects within the same bucket. Only one of obj_names, obj_range, or obj_template
     should be provided.

    Args:
        bck (Bucket): Bucket the objects belong to
        obj_names (list[str], optional): List of object names to include in this collection
        obj_range (ObjectRange, optional): Range defining which object names in the bucket should be included
        obj_template (str, optional): String argument to pass as template value directly to api
    """

    def __init__(
        self,
        bck: "Bucket",
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

        if obj_range:
            self._obj_collection = obj_range
        elif obj_names:
            self._obj_collection = ObjectNames(obj_names)
        else:
            self._obj_collection = ObjectTemplate(obj_template)

    def list_urls(self, prefix: str = "", etl_name: str = None) -> Iterable[str]:
        """
            Get an iterator of the full URL for every object in this group
        Args:
            prefix (str, optional): Limit objects selected by a given string prefix
            etl_name (str, optional): ETL to include in URLs

        Returns:
            Iterator of all object URLs in the group
        """
        for obj_name in self._obj_collection:
            yield self.bck.object(obj_name).get_url(etl_name=etl_name)

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
            HTTP_METHOD_DELETE,
            ACT_DELETE_OBJECTS,
            value=self._obj_collection.get_value(),
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
            HTTP_METHOD_DELETE,
            ACT_EVICT_OBJECTS,
            value=self._obj_collection.get_value(),
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
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            value=self._obj_collection.get_value(),
        ).text

    # pylint: disable=too-many-arguments
    def copy(
        self,
        to_bck: "Bucket",
        prepend: str = "",
        continue_on_error: bool = False,
        dry_run: bool = False,
        force: bool = False,
    ):
        """
        Copies a list or range of objects in a bucket

        Args:
            to_bck (Bucket): Destination bucket
            prepend (str, optional): Value to prepend to the name of copied objects
            continue_on_error (bool, optional): Whether to continue if there is an error copying a single object
            dry_run (bool, optional): Skip performing the copy and just log the intended actions
            force (bool, optional): Force this job to run over others in case it conflicts
                (see "limited coexistence" and xact/xreg/xreg.go)

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
        if dry_run:
            logger = logging.getLogger(f"{__name__}.copy")
            logger.info(
                "Copy dry-run. Running with dry_run=False will copy the following objects from bucket '%s' to '%s': %s",
                f"{self.bck.get_path()}",
                f"{to_bck.get_path()}",
                list(self._obj_collection),
            )
        copy_msg = CopyBckMsg(prepend=prepend, dry_run=dry_run, force=force)

        value = TCMultiObj(
            to_bck=to_bck.as_model(),
            tc_msg=TCBckMsg(copy_msg=copy_msg),
            object_selection=self._obj_collection.get_value(),
            continue_on_err=continue_on_error,
        ).as_dict()
        return self.bck.make_request(
            HTTP_METHOD_POST, ACT_COPY_OBJECTS, value=value
        ).text

    # pylint: disable=too-many-arguments
    def transform(
        self,
        to_bck: "Bucket",
        etl_name: str,
        timeout: str = DEFAULT_ETL_TIMEOUT,
        prepend: str = "",
        continue_on_error: bool = False,
        dry_run: bool = False,
        force: bool = False,
    ):
        """
        Performs ETL operation on a list or range of objects in a bucket, placing the results in the destination bucket

        Args:
            to_bck (Bucket): Destination bucket
            etl_name (str): Name of existing ETL to apply
            timeout (str): Timeout of the ETL job (e.g. 5m for 5 minutes)
            prepend (str, optional): Value to prepend to the name of resulting transformed objects
            continue_on_error (bool, optional): Whether to continue if there is an error transforming a single object
            dry_run (bool, optional): Skip performing the transform and just log the intended actions
            force (bool, optional): Force this job to run over others in case it conflicts
                (see "limited coexistence" and xact/xreg/xreg.go)

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
        if dry_run:
            logger = logging.getLogger(f"{__name__}.transform")
            logger.info(
                "Transform dry-run. Running with dry_run=False will apply ETL '%s' to objects %s",
                etl_name,
                list(self._obj_collection),
            )

        copy_msg = CopyBckMsg(prepend=prepend, dry_run=dry_run, force=force)
        transform_msg = TransformBckMsg(etl_name=etl_name, timeout=timeout)
        value = TCMultiObj(
            to_bck=to_bck.as_model(),
            tc_msg=TCBckMsg(transform_msg=transform_msg, copy_msg=copy_msg),
            object_selection=self._obj_collection.get_value(),
            continue_on_err=continue_on_error,
        ).as_dict()
        return self.bck.make_request(
            HTTP_METHOD_POST, ACT_TRANSFORM_OBJECTS, value=value
        ).text

    def archive(
        self,
        archive_name: str,
        mime: str = "",
        to_bck: "Bucket" = None,
        include_source_name: bool = False,
        allow_append: bool = False,
        continue_on_err: bool = False,
    ):
        """
        Create or append to an archive

        Args:
            archive_name (str): Name of archive to create or append
            mime (str, optional): MIME type of the content
            to_bck (Bucket, optional): Destination bucket, defaults to current bucket
            include_source_name (bool, optional): Include the source bucket name in the archived objects' names
            allow_append (bool, optional): Allow appending to an existing archive
            continue_on_err (bool, optional): Whether to continue if there is an error archiving a single object

        Returns:
            Job ID (as str) that can be used to check the status of the operation

        """
        val = ArchiveMultiObj(
            object_selection=self._obj_collection.get_value(),
            archive_name=archive_name,
            mime=mime,
            to_bck=to_bck.as_model() if to_bck else self.bck.as_model(),
            include_source_name=include_source_name,
            allow_append=allow_append,
            continue_on_err=continue_on_err,
        ).as_dict()
        return self.bck.make_request(
            HTTP_METHOD_PUT, ACT_ARCHIVE_OBJECTS, value=val
        ).text

    def list_names(self) -> List[str]:
        """
        List all the object names included in this group of objects

        Returns:
            List of object names

        """
        return list(self._obj_collection)
