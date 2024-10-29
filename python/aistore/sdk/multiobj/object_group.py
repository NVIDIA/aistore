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
from aistore.sdk.etl.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.obj.object import Object
from aistore.sdk.multiobj.object_names import ObjectNames
from aistore.sdk.multiobj.object_range import ObjectRange
from aistore.sdk.multiobj.object_template import ObjectTemplate
from aistore.sdk.types import (
    TCMultiObj,
    CopyBckMsg,
    TransformBckMsg,
    TCBckMsg,
    ArchiveMultiObj,
    PrefetchMsg,
)
from aistore.sdk.request_client import RequestClient


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
        obj_names: List = None,
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

    @property
    def client(self) -> RequestClient:
        """The client bound to the bucket used by the ObjectGroup."""
        return self.bck.client

    @client.setter
    def client(self, client) -> RequestClient:
        """Update the client bound to the bucket used by the ObjectGroup."""
        self.bck.client = client

    def list_urls(self, prefix: str = "", etl_name: str = None) -> Iterable[str]:
        """
        Implementation of the abstract method from AISSource that provides an iterator
        of full URLs to every object in this bucket matching the specified prefix
        Args:
            prefix (str, optional): Limit objects selected by a given string prefix
            etl_name (str, optional): ETL to include in URLs

        Returns:
            Iterator of all object URLs in the group
        """
        for obj_name in self._obj_collection:
            yield self.bck.object(obj_name).get_url(etl_name=etl_name)

    def list_all_objects_iter(
        self, prefix: str = "", props: str = "name,size"
    ) -> Iterable[Object]:
        """
        Implementation of the abstract method from AISSource that provides an iterator
        of all the objects in this bucket matching the specified prefix.

        Args:
            prefix (str, optional): Limit objects selected by a given string prefix
            props (str, optional): By default, will include all object properties.
                Pass in None to skip and avoid the extra API call.

        Returns:
            Iterator of all the objects in the group
        """
        for obj_name in self._obj_collection:

            # If object does not start the prefix, skip it
            if not obj_name.startswith(prefix):
                continue

            obj = self.bck.object(obj_name)

            if props is not None:
                obj.head()  # Updates the objects props as well

            yield obj

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

    def prefetch(
        self,
        blob_threshold: int = None,
        num_workers: int = None,
        latest: bool = False,
        continue_on_error: bool = False,
    ):
        """
        Prefetches a list or range of objects in a bucket so that they are cached in AIS
        NOTE: only Cloud buckets can be prefetched.

        Args:
            latest (bool, optional): GET the latest object version from the associated remote bucket
            continue_on_error (bool, optional): Whether to continue if there is an error prefetching a single object
            blob_threshold (int, optional): Utilize built-in blob-downloader for remote objects
                greater than the specified (threshold) size in bytes
            num_workers (int, optional): Number of concurrent workers (readers). Defaults to the number of target
                mountpaths if omitted or zero. A value of -1 indicates no workers at all (i.e., single-threaded
                execution). Any positive value will be adjusted not to exceed the number of target CPUs.

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

        value = PrefetchMsg(
            object_selection=self._obj_collection.get_value(),
            continue_on_err=continue_on_error,
            latest=latest,
            blob_threshold=blob_threshold,
            num_workers=num_workers,
        ).as_dict()

        return self.bck.make_request(
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            value=value,
        ).text

    # pylint: disable=too-many-arguments
    def copy(
        self,
        to_bck: "Bucket",
        prepend: str = "",
        continue_on_error: bool = False,
        dry_run: bool = False,
        force: bool = False,
        latest: bool = False,
        sync: bool = False,
        num_workers: int = None,
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
            latest (bool, optional): GET the latest object version from the associated remote bucket
            sync (bool, optional): synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source
            num_workers (int, optional): Number of concurrent workers (readers). Defaults to the number of target
                mountpaths if omitted or zero. A value of -1 indicates no workers at all (i.e., single-threaded
                execution). Any positive value will be adjusted not to exceed the number of target CPUs.

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
        copy_msg = CopyBckMsg(
            prepend=prepend, dry_run=dry_run, force=force, latest=latest, sync=sync
        )

        value = TCMultiObj(
            to_bck=to_bck.as_model(),
            tc_msg=TCBckMsg(copy_msg=copy_msg),
            object_selection=self._obj_collection.get_value(),
            continue_on_err=continue_on_error,
            num_workers=num_workers,
        ).as_dict()

        return self.bck.make_request(
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            value=value,
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
        latest: bool = False,
        sync: bool = False,
        num_workers: int = None,
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
            latest (bool, optional): GET the latest object version from the associated remote bucket
            sync (bool, optional): synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source
            num_workers (int, optional): Number of concurrent workers (readers). Defaults to the number of target
                mountpaths if omitted or zero. A value of -1 indicates no workers at all (i.e., single-threaded
                execution). Any positive value will be adjusted not to exceed the number of target CPUs.

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

        copy_msg = CopyBckMsg(
            prepend=prepend, dry_run=dry_run, force=force, latest=latest, sync=sync
        )
        transform_msg = TransformBckMsg(etl_name=etl_name, timeout=timeout)
        value = TCMultiObj(
            to_bck=to_bck.as_model(),
            tc_msg=TCBckMsg(transform_msg=transform_msg, copy_msg=copy_msg),
            object_selection=self._obj_collection.get_value(),
            continue_on_err=continue_on_error,
            num_workers=num_workers,
        ).as_dict()
        return self.bck.make_request(
            HTTP_METHOD_POST, ACT_TRANSFORM_OBJECTS, value=value
        ).text

    # pylint: disable=too-many-arguments
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
