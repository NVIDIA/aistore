#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import time
from typing import Dict, List, NewType, Iterable, Union
import requests
from requests import structures

from aistore.sdk.ais_source import AISSource
from aistore.sdk.etl.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.obj.object_iterator import ObjectIterator
from aistore.sdk.const import (
    ACT_COPY_BCK,
    ACT_CREATE_BCK,
    ACT_DESTROY_BCK,
    ACT_ETL_BCK,
    ACT_EVICT_REMOTE_BCK,
    ACT_LIST,
    ACT_MOVE_BCK,
    ACT_SUMMARY_BCK,
    HEADER_ACCEPT,
    HEADER_BUCKET_PROPS,
    HEADER_BUCKET_SUMM,
    HEADER_XACTION_ID,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_POST,
    MSGPACK_CONTENT_TYPE,
    QPARAM_BCK_TO,
    QPARAM_BSUMM_REMOTE,
    QPARAM_FLT_PRESENCE,
    QPARAM_KEEP_REMOTE,
    QPARAM_NAMESPACE,
    QPARAM_PROVIDER,
    QPARAM_UUID,
    URL_PATH_BUCKETS,
    STATUS_ACCEPTED,
    STATUS_OK,
    STATUS_PARTIAL_CONTENT,
    DEFAULT_JOB_POLL_TIME,
)
from aistore.sdk.enums import FLTPresence
from aistore.sdk.provider import Provider
from aistore.sdk.dataset.dataset_config import DatasetConfig

from aistore.sdk.errors import (
    InvalidBckProvider,
    ErrBckAlreadyExists,
    ErrBckNotFound,
    UnexpectedHTTPStatusCode,
)
from aistore.sdk.multiobj import ObjectGroup, ObjectRange
from aistore.sdk.request_client import RequestClient
from aistore.sdk.obj.object import Object, BucketDetails
from aistore.sdk.types import (
    ActionMsg,
    BucketEntry,
    BucketList,
    BucketModel,
    BsummCtrlMsg,
    Namespace,
    CopyBckMsg,
    TransformBckMsg,
    TCBckMsg,
    ListObjectsMsg,
)
from aistore.sdk.list_object_flag import ListObjectFlag
from aistore.sdk.utils import validate_directory, get_file_size
from aistore.sdk.obj.object_props import ObjectProps

Header = NewType("Header", structures.CaseInsensitiveDict)


# pylint: disable=too-many-public-methods,too-many-lines
class Bucket(AISSource):
    """
    A class representing a bucket that contains user data.

    Args:
        client (RequestClient): Client for interfacing with AIS cluster
        name (str): name of bucket
        provider (str or Provider, optional): Provider of bucket (one of "ais", "aws", "gcp", ...), defaults to "ais"
        namespace (Namespace, optional): Namespace of bucket, defaults to None
    """

    def __init__(
        self,
        name: str,
        client: RequestClient = None,
        provider: Union[Provider, str] = Provider.AIS,
        namespace: Namespace = None,
    ):
        self._client = client
        self._name = name
        self._provider = Provider.parse(provider)
        self._namespace = namespace
        self._qparam = {QPARAM_PROVIDER: self.provider.value}
        if self.namespace:
            self._qparam[QPARAM_NAMESPACE] = namespace.get_path()

    @property
    def client(self) -> RequestClient:
        """The client used by this bucket."""
        return self._client

    @client.setter
    def client(self, client):
        """Update the client used by this bucket."""
        self._client = client

    @property
    def qparam(self) -> Dict:
        """Default query parameters to use with API calls from this bucket."""
        return self._qparam

    @property
    def provider(self) -> Provider:
        """The provider for this bucket."""
        return self._provider

    @property
    def name(self) -> str:
        """The name of this bucket."""
        return self._name

    @property
    def namespace(self) -> Namespace:
        """The namespace for this bucket."""
        return self._namespace

    def list_urls(self, prefix: str = "", etl_name: str = None) -> Iterable[str]:
        """
        Implementation of the abstract method from AISSource that provides an iterator
        of full URLs to every object in this bucket matching the specified prefix

        Args:
            prefix (str, optional): Limit objects selected by a given string prefix
            etl_name (str, optional): ETL to include in URLs

        Returns:
            Iterator of full URLs of all objects matching the prefix
        """
        for entry in self.list_objects_iter(prefix=prefix, props="name"):
            yield self.object(entry.name).get_url(etl_name=etl_name)

    def list_all_objects_iter(
        self, prefix: str = "", props: str = "name,size"
    ) -> Iterable[Object]:
        """
        Implementation of the abstract method from AISSource that provides an iterator
        of all the objects in this bucket matching the specified prefix.

        Args:
            prefix (str, optional): Limit objects selected by a given string prefix
            props (str, optional): Comma-separated list of object properties to return. Default value is "name,size".
                Properties: "name", "size", "atime", "version", "checksum", "target_url", "copies".

        Returns:
            Iterator of all object URLs matching the prefix
        """
        for entry in self.list_objects_iter(prefix=prefix, props=props):
            yield self.object(entry.name, entry.generate_object_props())

    def create(self, exist_ok=False):
        """
        Creates a bucket in AIStore cluster.
        Can only create a bucket for AIS provider on localized cluster. Remote cloud buckets do not support creation.

        Args:
            exist_ok (bool, optional): Ignore error if the cluster already contains this bucket

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            aistore.sdk.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        self._verify_ais_bucket()
        try:
            self.make_request(HTTP_METHOD_POST, ACT_CREATE_BCK)
        except ErrBckAlreadyExists as err:
            if not exist_ok:
                raise err
        return self

    def delete(self, missing_ok=False):
        """
        Destroys bucket in AIStore cluster.
        In all cases removes both the bucket's content _and_ the bucket's metadata from the cluster.
        Note: AIS will _not_ call the remote backend provider to delete the corresponding Cloud bucket
        (iff the bucket in question is, in fact, a Cloud bucket).

        Args:
            missing_ok (bool, optional): Ignore error if bucket does not exist

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            aistore.sdk.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        self._verify_ais_bucket()
        try:
            self.make_request(HTTP_METHOD_DELETE, ACT_DESTROY_BCK)
        except ErrBckNotFound as err:
            if not missing_ok:
                raise err

    def rename(self, to_bck_name: str) -> str:
        """
        Renames bucket in AIStore cluster.
        Only works on AIS buckets. Returns job ID that can be used later to check the status of the asynchronous
            operation.

        Args:
            to_bck_name (str): New bucket name for bucket to be renamed as

        Returns:
            Job ID (as str) that can be used to check the status of the operation

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            aistore.sdk.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        self._verify_ais_bucket()
        params = self.qparam.copy()
        params[QPARAM_BCK_TO] = Bucket(
            name=to_bck_name, namespace=self.namespace
        ).get_path()
        resp = self.make_request(HTTP_METHOD_POST, ACT_MOVE_BCK, params=params)
        self._name = to_bck_name
        return resp.text

    def evict(self, keep_md: bool = False):
        """
        Evicts bucket in AIStore cluster.
        NOTE: only Cloud buckets can be evicted.

        Args:
            keep_md (bool, optional): If true, evicts objects but keeps the bucket's metadata (i.e., the bucket's name
                and its properties)

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            aistore.sdk.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        self.verify_cloud_bucket()
        params = self.qparam.copy()
        params[QPARAM_KEEP_REMOTE] = str(keep_md)
        self.make_request(HTTP_METHOD_DELETE, ACT_EVICT_REMOTE_BCK, params=params)

    def head(self) -> Header:
        """
        Requests bucket properties.

        Returns:
            Response header with the bucket properties

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        return self.client.request(
            HTTP_METHOD_HEAD,
            path=f"{URL_PATH_BUCKETS}/{self.name}",
            params=self.qparam,
        ).headers

    # pylint: disable=too-many-arguments
    def summary(
        self,
        uuid: str = "",
        prefix: str = "",
        cached: bool = True,
        present: bool = True,
    ):
        """
        Returns bucket summary (starts xaction job and polls for results).

        Args:
            uuid (str): Identifier for the bucket summary. Defaults to an empty string.
            prefix (str): Prefix for objects to be included in the bucket summary.
                          Defaults to an empty string (all objects).
            cached (bool): If True, summary entails cached entities. Defaults to True.
            present (bool): If True, summary entails present entities. Defaults to True.

        Raises:
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
            aistore.sdk.errors.AISError: All other types of errors with AIStore
        """
        bsumm_ctrl_msg = BsummCtrlMsg(
            uuid=uuid, prefix=prefix, cached=cached, present=present
        )

        # Start the job and get the job ID
        resp = self.make_request(
            HTTP_METHOD_GET,
            ACT_SUMMARY_BCK,
            params=self.qparam,
            value=bsumm_ctrl_msg.dict(),
        )

        # Initial response status code should be 202
        if resp.status_code == STATUS_OK:
            raise UnexpectedHTTPStatusCode([STATUS_ACCEPTED], resp.status_code)

        job_id = resp.text.strip('"')

        # Update the uuid in the control message
        bsumm_ctrl_msg.uuid = job_id

        # Sleep and request frequency in sec (starts at 0.2s)
        sleep_time = DEFAULT_JOB_POLL_TIME

        # Poll async task for http.StatusOK completion
        while True:
            resp = self.make_request(
                HTTP_METHOD_GET,
                ACT_SUMMARY_BCK,
                params=self.qparam,
                value=bsumm_ctrl_msg.dict(),
            )

            # If task completed successfully, break the loop
            if resp.status_code == STATUS_OK:
                break
            # If status code received is neither STATUS_ACCEPTED nor STATUS_PARTIAL_CONTENT, raise an exception
            if resp.status_code not in (STATUS_ACCEPTED, STATUS_PARTIAL_CONTENT):
                raise UnexpectedHTTPStatusCode(
                    [STATUS_OK, STATUS_ACCEPTED, STATUS_PARTIAL_CONTENT],
                    resp.status_code,
                )

            # If task is still running, wait for some time and try again
            time.sleep(sleep_time)
            if resp.status_code != STATUS_PARTIAL_CONTENT:
                sleep_time = min(
                    10, sleep_time * 1.5
                )  # Increase sleep_time by 50%, but don't exceed 10 seconds

        return json.loads(resp.content.decode("utf-8"))[0]

    def info(
        self,
        flt_presence: int = FLTPresence.FLT_EXISTS,
        bsumm_remote: bool = True,
        prefix: str = "",
    ):
        """
        Returns bucket summary and information/properties.

        Args:
            flt_presence (FLTPresence): Describes the presence of buckets and objects with respect to their existence
                                or non-existence in the AIS cluster using the enum FLTPresence. Defaults to
                                value FLT_EXISTS and values are:
                                FLT_EXISTS - object or bucket exists inside and/or outside cluster
                                FLT_EXISTS_NO_PROPS - same as FLT_EXISTS but no need to return summary
                                FLT_PRESENT - bucket is present or object is present and properly
                                located
                                FLT_PRESENT_NO_PROPS - same as FLT_PRESENT but no need to return summary
                                FLT_PRESENT_CLUSTER - objects present anywhere/how in
                                the cluster as replica, ec-slices, misplaced
                                FLT_EXISTS_OUTSIDE - not present; exists outside cluster
            bsumm_remote (bool): If True, returned bucket info will include remote objects as well
            prefix (str): Only include objects with the given prefix in the bucket

        Raises:
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
            ValueError: `flt_presence` is not one of the expected values
            aistore.sdk.errors.AISError: All other types of errors with AIStore
        """
        try:
            flt_presence = int(FLTPresence(flt_presence))
        except Exception as err:
            raise ValueError(
                "`flt_presence` must be in values of enum FLTPresence"
            ) from err

        params = self.qparam.copy()
        params.update({QPARAM_FLT_PRESENCE: flt_presence})
        params[QPARAM_BSUMM_REMOTE] = bsumm_remote

        path = f"{URL_PATH_BUCKETS}/{self.name}"
        if prefix:
            path += f"/{prefix}"

        response = self.client.request(
            HTTP_METHOD_HEAD,
            path=path,
            params=params,
        )

        bucket_props = response.headers.get(HEADER_BUCKET_PROPS, "{}")
        uuid = response.headers.get(HEADER_XACTION_ID, "").strip('"')
        params[QPARAM_UUID] = uuid

        result = {}

        # Initial response status code should be 202
        if response.status_code != int(STATUS_ACCEPTED):
            raise UnexpectedHTTPStatusCode([STATUS_ACCEPTED], response.status_code)

        # Sleep and request frequency in sec (starts at 2 s)
        sleep_time = 2
        time.sleep(sleep_time)
        i = 0

        # Poll async task for http.StatusOK completion
        while True:
            response = self.client.request(
                HTTP_METHOD_HEAD,
                path=f"{URL_PATH_BUCKETS}/{self.name}",
                params=params,
            )

            bucket_summ = response.headers.get(HEADER_BUCKET_SUMM, "")

            if bucket_summ != "":
                result = json.loads(bucket_summ)

            # If task completed successfully, break the loop
            if response.status_code == STATUS_OK:
                break

            time.sleep(sleep_time)
            i += 1

            if i == 8 and response.status_code != STATUS_PARTIAL_CONTENT:
                sleep_time *= 2
            elif i == 16 and response.status_code != STATUS_PARTIAL_CONTENT:
                sleep_time *= 2

        return bucket_props, result

    # pylint: disable=too-many-arguments
    def copy(
        self,
        to_bck: Bucket,
        prefix_filter: str = "",
        prepend: str = "",
        dry_run: bool = False,
        force: bool = False,
        latest: bool = False,
        sync: bool = False,
    ) -> str:
        """
        Returns job ID that can be used later to check the status of the asynchronous operation.

        Args:
            to_bck (Bucket): Destination bucket
            prefix_filter (str, optional): Only copy objects with names starting with this prefix
            prepend (str, optional): Value to prepend to the name of copied objects
            dry_run (bool, optional): Determines if the copy should actually
                happen or not
            force (bool, optional): Override existing destination bucket
            latest (bool, optional): GET the latest object version from the associated remote bucket
            sync (bool, optional): synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source

        Returns:
            Job ID (as str) that can be used to check the status of the operation

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        value = CopyBckMsg(
            prefix=prefix_filter,
            prepend=prepend,
            dry_run=dry_run,
            force=force,
            latest=latest,
            sync=sync,
        ).as_dict()
        params = self.qparam.copy()
        params[QPARAM_BCK_TO] = to_bck.get_path()
        return self.make_request(
            HTTP_METHOD_POST, ACT_COPY_BCK, value=value, params=params
        ).text

    # pylint: disable=too-many-arguments
    def list_objects(
        self,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
        uuid: str = "",
        continuation_token: str = "",
        flags: List[ListObjectFlag] = None,
        target: str = "",
    ) -> BucketList:
        """
        Returns a structure that contains a page of objects, job ID, and continuation token (to read the next page, if
            available).

        Args:
            prefix (str, optional): Return only objects that start with the prefix
            props (str, optional): Comma-separated list of object properties to return. Default value is "name,size".
                Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies",
                "ec", "custom", "node".
            page_size (int, optional): Return at most "page_size" objects.
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return
                    more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number of objects.
            uuid (str, optional): Job ID, required to get the next page of objects
            continuation_token (str, optional): Marks the object to start reading the next page
            flags (List[ListObjectFlag], optional): Optional list of ListObjectFlag enums to include as flags in the
             request
            target(str, optional): Only list objects on this specific target node

        Returns:
            BucketList: the page of objects in the bucket and the continuation token to get the next page
            Empty continuation token marks the final page of the object list

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """

        value = ListObjectsMsg(
            prefix=prefix,
            page_size=page_size,
            uuid=uuid,
            props=props,
            continuation_token=continuation_token,
            flags=[] if flags is None else flags,
            target=target,
        ).as_dict()
        action = ActionMsg(action=ACT_LIST, value=value).dict()

        bucket_list = self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_BUCKETS}/{ self.name }",
            headers={HEADER_ACCEPT: MSGPACK_CONTENT_TYPE},
            res_model=BucketList,
            json=action,
            params=self.qparam,
        )

        for entry in bucket_list.entries:
            entry.object = self.object(entry.name)
        return bucket_list

    def list_objects_iter(
        self,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
        flags: List[ListObjectFlag] = None,
        target: str = "",
    ) -> ObjectIterator:
        """
        Returns an iterator for all objects in bucket

        Args:
            prefix (str, optional): Return only objects that start with the prefix
            props (str, optional): Comma-separated list of object properties to return. Default value is "name,size".
                Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies",
                "ec", "custom", "node".
            page_size (int, optional): return at most "page_size" objects
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return
                    more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number objects
            flags (List[ListObjectFlag], optional): Optional list of ListObjectFlag enums to include as flags in the
             request
            target(str, optional): Only list objects on this specific target node

        Returns:
            ObjectIterator: object iterator

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """

        def fetch_objects(uuid, token):
            return self.list_objects(
                prefix,
                props,
                page_size,
                uuid=uuid,
                continuation_token=token,
                flags=flags,
                target=target,
            )

        return ObjectIterator(fetch_objects)

    def list_all_objects(
        self,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
        flags: List[ListObjectFlag] = None,
        target: str = "",
    ) -> List[BucketEntry]:
        """
        Returns a list of all objects in bucket

        Args:
            prefix (str, optional): return only objects that start with the prefix
            props (str, optional): comma-separated list of object properties to return. Default value is "name,size".
                Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies",
                "ec", "custom", "node".
            page_size (int, optional): return at most "page_size" objects
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return
                    more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number objects
            flags (List[ListObjectFlag], optional): Optional list of ListObjectFlag enums to include as flags in the
             request
            target(str, optional): Only list objects on this specific target node

        Returns:
            List[BucketEntry]: list of objects in bucket

        Raises:
            aistore.sdk.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        uuid = ""
        continuation_token = ""
        obj_list = None

        while True:
            resp = self.list_objects(
                prefix=prefix,
                props=props,
                page_size=page_size,
                uuid=uuid,
                continuation_token=continuation_token,
                flags=flags,
                target=target,
            )
            if obj_list:
                obj_list = obj_list + resp.entries
            obj_list = obj_list or resp.entries
            if resp.continuation_token == "":
                break
            continuation_token = resp.continuation_token
            uuid = resp.uuid
        return obj_list

    # pylint: disable=too-many-arguments
    def transform(
        self,
        etl_name: str,
        to_bck: Bucket,
        timeout: str = DEFAULT_ETL_TIMEOUT,
        prefix_filter: str = "",
        prepend: str = "",
        ext: Dict[str, str] = None,
        force: bool = False,
        dry_run: bool = False,
        latest: bool = False,
        sync: bool = False,
    ) -> str:
        """
        Visits all selected objects in the source bucket and for each object, puts the transformed
        result to the destination bucket

        Args:
            etl_name (str): name of etl to be used for transformations
            to_bck (str): destination bucket for transformations
            timeout (str, optional): Timeout of the ETL job (e.g. 5m for 5 minutes)
            prefix_filter (str, optional): Only transform objects with names starting with this prefix
            prepend (str, optional): Value to prepend to the name of resulting transformed objects
            ext (Dict[str, str], optional): dict of new extension followed by extension to be replaced
                (i.e. {"jpg": "txt"})
            dry_run (bool, optional): determines if the copy should actually happen or not
            force (bool, optional): override existing destination bucket
            latest (bool, optional): GET the latest object version from the associated remote bucket
            sync (bool, optional): synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source

        Returns:
            Job ID (as str) that can be used to check the status of the operation
        """
        value = TCBckMsg(
            ext=ext,
            transform_msg=TransformBckMsg(etl_name=etl_name, timeout=timeout),
            copy_msg=CopyBckMsg(
                prefix=prefix_filter,
                prepend=prepend,
                force=force,
                dry_run=dry_run,
                latest=latest,
                sync=sync,
            ),
        ).as_dict()

        params = self.qparam.copy()
        params[QPARAM_BCK_TO] = to_bck.get_path()
        return self.make_request(
            HTTP_METHOD_POST, ACT_ETL_BCK, value=value, params=params
        ).text

    def put_files(
        self,
        path: str,
        prefix_filter: str = "",
        pattern: str = "*",
        basename: bool = False,
        prepend: str = None,
        recursive: bool = False,
        dry_run: bool = False,
        verbose: bool = True,
    ) -> List[str]:
        """
        Puts files found in a given filepath as objects to a bucket in AIS storage.

        Args:
            path (str): Local filepath, can be relative or absolute
            prefix_filter (str, optional): Only put files with names starting with this prefix
            pattern (str, optional): Shell-style wildcard pattern to filter files
            basename (bool, optional): Whether to use the file names only as object names and omit the path information
            prepend (str, optional): Optional string to use as a prefix in the object name for all objects uploaded
                No delimiter ("/", "-", etc.) is automatically applied between the prepend value and the object name
            recursive (bool, optional): Whether to recurse through the provided path directories
            dry_run (bool, optional): Option to only show expected behavior without an actual put operation
            verbose (bool, optional): Whether to print upload info to standard output

        Returns:
            List of object names put to a bucket in AIS

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            ValueError: The path provided is not a valid directory
        """
        validate_directory(path)
        file_iterator = (
            Path(path).rglob(pattern) if recursive else Path(path).glob(pattern)
        )
        obj_names = []
        dry_run_prefix = "Dry-run enabled. Proposed action:" if dry_run else ""

        logger = logging.getLogger(f"{__name__}.put_files")
        logger.disabled = not verbose
        for file in file_iterator:
            if not file.is_file() or not str(file.name).startswith(prefix_filter):
                continue
            obj_name = self._get_uploaded_obj_name(file, path, basename, prepend)
            if not dry_run:
                self.object(obj_name).put_file(str(file))
            logger.info(
                "%s File '%s' uploaded as object '%s' with size %s",
                dry_run_prefix,
                file,
                obj_name,
                get_file_size(file),
            )
            obj_names.append(obj_name)
        logger.info(
            "%s Specified files from %s uploaded to bucket %s",
            dry_run_prefix,
            path,
            f"{self.provider}://{self.name}",
        )
        return obj_names

    @staticmethod
    def _get_uploaded_obj_name(file, root_path, basename, prepend):
        obj_name = str(file.relative_to(root_path)) if not basename else file.name
        if prepend:
            return prepend + obj_name
        return obj_name

    def object(self, obj_name: str, props: ObjectProps = None) -> Object:
        """
        Factory constructor for an object in this bucket.
        Does not make any HTTP request, only instantiates an object in a bucket owned by the client.

        Args:
            obj_name (str): Name of object
            props (ObjectProps, optional): Properties of the object, as updated by head(), optionally pre-initialized.

        Returns:
            The object created.
        """
        details = BucketDetails(self.name, self.provider, self.qparam)
        return Object(
            client=self.client, bck_details=details, name=obj_name, props=props
        )

    def objects(
        self,
        obj_names: List = None,
        obj_range: ObjectRange = None,
        obj_template: str = None,
    ) -> ObjectGroup:
        """
        Factory constructor for multiple objects belonging to this bucket.

        Args:
            obj_names (list): Names of objects to include in the group
            obj_range (ObjectRange): Range of objects to include in the group
            obj_template (str): String template defining objects to include in the group

        Returns:
            The ObjectGroup created
        """
        return ObjectGroup(
            bck=self,
            obj_names=obj_names,
            obj_range=obj_range,
            obj_template=obj_template,
        )

    def make_request(
        self,
        method: str,
        action: str,
        value: Dict = None,
        params: Dict = None,
    ) -> requests.Response:
        """
        Use the bucket's client to make a request to the bucket endpoint on the AIS server

        Args:
            method (str): HTTP method to use, e.g. POST/GET/DELETE
            action (str): Action string used to create an ActionMsg to pass to the server
            value (dict): Additional value parameter to pass in the ActionMsg
            params (dict, optional): Optional parameters to pass in the request

        Returns:
            Response from the server

        """
        if self._client is None:
            raise ValueError(
                "Bucket requires a client to use functions. Try defining a client and accessing this bucket with "
                "client.bucket()"
            )
        json_val = ActionMsg(action=action, value=value).dict()
        return self._client.request(
            method,
            path=f"{URL_PATH_BUCKETS}/{self.name}",
            json=json_val,
            params=params if params else self.qparam,
        )

    def _verify_ais_bucket(self):
        """
        Verify the bucket provider is AIS
        """
        if self.provider is not Provider.AIS:
            raise InvalidBckProvider(self.provider)

    def verify_cloud_bucket(self):
        """
        Verify the bucket provider is a cloud provider
        """
        if self.provider is Provider.AIS:
            raise InvalidBckProvider(self.provider)

    def get_path(self) -> str:
        """
        Get the path representation of this bucket
        """
        namespace_path = self.namespace.get_path() if self.namespace else "@#"
        return f"{ self.provider.value }/{ namespace_path }/{ self.name }/"

    def as_model(self) -> BucketModel:
        """
        Return a data-model of the bucket

        Returns:
            BucketModel representation
        """
        return BucketModel(
            name=self.name, namespace=self.namespace, provider=self.provider.value
        )

    def write_dataset(
        self,
        config: DatasetConfig,
        skip_missing: bool = True,
        **kwargs,
    ):
        """
        Write a dataset to a bucket in AIS in webdataset format using wds.ShardWriter. Logs the missing attributes

        Args:
            config (DatasetConfig): Configuration dict specifying how to process
                and store each part of the dataset item
            skip_missing (bool, optional): Skip samples that are missing one or more attributes, defaults to True
            **kwargs (optional): Optional keyword arguments to pass to the ShardWriter
        """

        # Add the upload shard logic to the original post-processing function
        original_post = kwargs.get("post", lambda path: None)

        def combined_post_processing(shard_path):
            original_post(shard_path)
            self.object(shard_path).put_file(shard_path)
            os.unlink(shard_path)

        kwargs["post"] = combined_post_processing
        config.write_shards(skip_missing=skip_missing, **kwargs)
