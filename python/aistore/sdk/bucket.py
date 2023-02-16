#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable

from pathlib import Path
from typing import Dict, List, NewType
import requests

from aistore.sdk.object_iterator import ObjectIterator
from aistore.sdk.const import (
    ACT_COPY_BCK,
    ACT_CREATE_BCK,
    ACT_DESTROY_BCK,
    ACT_ETL_BCK,
    ACT_EVICT_REMOTE_BCK,
    ACT_LIST,
    ACT_MOVE_BCK,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_POST,
    ProviderAIS,
    QParamBucketTo,
    QParamKeepBckMD,
    QParamProvider,
    URL_PATH_BUCKETS,
)

from aistore.sdk.errors import InvalidBckProvider
from aistore.sdk.object_range import ObjectRange
from aistore.sdk.request_client import RequestClient
from aistore.sdk.object_group import ObjectGroup
from aistore.sdk.object import Object
from aistore.sdk.types import (
    ActionMsg,
    BucketEntry,
    BucketList,
    Namespace,
)
from aistore.sdk.utils import validate_directory

Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=unused-variable
class Bucket:
    """
    A class representing a bucket that contains user data.

    Args:
        client (RequestClient): Client for interfacing with AIS cluster
        name (str): name of bucket
        provider (str, optional): Provider of bucket (one of "ais", "aws", "gcp", ...), defaults to "ais"
        namespace (Namespace, optional): Namespace of bucket, defaults to None
    """

    def __init__(
        self,
        client: RequestClient,
        name: str,
        provider: str = ProviderAIS,
        namespace: Namespace = None,
    ):
        self._client = client
        self._name = name
        self._provider = provider
        self._namespace = namespace
        self._qparam = {QParamProvider: provider}

    @property
    def client(self):
        """The client bound to this bucket."""
        return self._client

    @property
    def qparam(self):
        """The QParamProvider of this bucket."""
        return self._qparam

    @property
    def provider(self):
        """The provider for this bucket."""
        return self._provider

    @property
    def name(self):
        """The name of this bucket."""
        return self._name

    @property
    def namespace(self):
        """The namespace for this bucket."""
        return self._namespace

    def create(self):
        """
        Creates a bucket in AIStore cluster.
        Can only create a bucket for AIS provider on localized cluster. Remote cloud buckets do not support creation.

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
        self.make_request(HTTP_METHOD_POST, ACT_CREATE_BCK)

    def delete(self):
        """
        Destroys bucket in AIStore cluster.
        In all cases removes both the bucket's content _and_ the bucket's metadata from the cluster.
        Note: AIS will _not_ call the remote backend provider to delete the corresponding Cloud bucket
        (iff the bucket in question is, in fact, a Cloud bucket).

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
        self.make_request(HTTP_METHOD_DELETE, ACT_DESTROY_BCK)

    def rename(self, to_bck: str) -> str:
        """
        Renames bucket in AIStore cluster.
        Only works on AIS buckets. Returns job ID that can be used later to check the status of the asynchronous
            operation.

        Args:
            to_bck (str): New bucket name for bucket to be renamed as

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
        params[QParamBucketTo] = f"{ProviderAIS}/@#/{to_bck}/"
        resp = self.make_request(HTTP_METHOD_POST, ACT_MOVE_BCK, params=params)
        self._name = to_bck
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
        params[QParamKeepBckMD] = str(keep_md)
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
    def copy(
        self,
        to_bck_name: str,
        prefix: str = "",
        dry_run: bool = False,
        force: bool = False,
        to_provider: str = ProviderAIS,
    ) -> str:
        """
        Returns job ID that can be used later to check the status of the asynchronous operation.

        Args:
            to_bck_name (str): Name of the destination bucket
            prefix (str, optional): If set, only the objects starting with
                provider prefix will be copied
            dry_run (bool, optional): Determines if the copy should actually
                happen or not
            force (bool, optional): Override existing destination bucket
            to_provider (str, optional): Name of destination bucket provider

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
        value = {"prefix": prefix, "dry_run": dry_run, "force": force}
        params = self.qparam.copy()
        params[QParamBucketTo] = f"{ to_provider }/@#/{ to_bck_name }/"
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
        value = {
            "prefix": prefix,
            "pagesize": page_size,
            "uuid": uuid,
            "props": props,
            "continuation_token": continuation_token,
        }
        action = ActionMsg(action=ACT_LIST, value=value).dict()

        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=f"{URL_PATH_BUCKETS}/{ self.name }",
            res_model=BucketList,
            json=action,
            params=self.qparam,
        )

    def list_objects_iter(
        self,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
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
                prefix, props, page_size, uuid=uuid, continuation_token=token
            )

        return ObjectIterator(fetch_objects)

    def list_all_objects(
        self,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
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
            )
            if obj_list:
                obj_list = obj_list + resp.get_entries()
            obj_list = obj_list or resp.get_entries()
            if resp.continuation_token == "":
                break
            continuation_token = resp.continuation_token
            uuid = resp.uuid
        return obj_list

    # pylint: disable=too-many-arguments
    def transform(
        self,
        etl_name: str,
        to_bck: str,
        prefix: str = "",
        ext: Dict[str, str] = None,
        force: bool = False,
        dry_run: bool = False,
    ):
        """
        Transforms all objects in a bucket and puts them to destination bucket.

        Args:
            etl_name (str): name of etl to be used for transformations
            to_bck (str): destination bucket for transformations
            prefix (str, optional): prefix to be added to resulting transformed objects
            ext (Dict[str, str], optional): dict of new extension followed by extension to be replaced
                (i.e. {"jpg": "txt"})
            dry_run (bool, optional): determines if the copy should actually happen or not
            force (bool, optional): override existing destination bucket

        Returns:
            Job ID (as str) that can be used to check the status of the operation
        """
        value = {
            "id": etl_name,
            "prefix": prefix,
            "force": force,
            "dry_run": dry_run,
        }

        if ext:
            value["ext"] = ext

        params = self.qparam.copy()
        params[QParamBucketTo] = f"{ProviderAIS}/@#/{to_bck}/"
        return self.make_request(
            HTTP_METHOD_POST, ACT_ETL_BCK, value=value, params=params
        ).text

    def put_files(
        self,
        path: str,
        prefix_filter: str = "",
        pattern: str = "*",
        basename: bool = False,
        obj_prefix: str = None,
        recursive: bool = False,
        dry_run: bool = False,
        verbose: bool = True,
    ) -> List[str]:
        """
        Puts files found in a given filepath as objects to a bucket in AIS storage.

        Args:
            path (str): Local filepath, can be relative or absolute
            prefix_filter (str, optional): Required prefix in names of all files to put
            pattern (str, optional): Regex pattern to filter files
            basename (bool, optional): Whether to use the file names only as object names and omit the path information
            obj_prefix (str, optional): Optional string to use as a prefix in the object name for all objects uploaded
                No delimiter ("/", "-", etc.) is automatically applied between the obj_prefix and the object name
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
        for file in file_iterator:
            if not file.is_file() or not str(file.name).startswith(prefix_filter):
                continue
            obj_name = self._get_uploaded_obj_name(file, path, basename, obj_prefix)
            if not dry_run:
                self.object(obj_name).put_file(str(file))
            if verbose:
                print(
                    f"{dry_run_prefix} File {file} uploaded as object {obj_name} with size {file.stat().st_size}"
                )
            obj_names.append(obj_name)
        print(
            f"{dry_run_prefix} All files from {path} uploaded to bucket {self.provider}://{self.name}"
        )
        return obj_names

    @staticmethod
    def _get_uploaded_obj_name(file, root_path, basename, prefix):
        obj_name = str(file.relative_to(root_path)) if not basename else file.name
        if prefix:
            return prefix + obj_name
        return obj_name

    def object(self, obj_name: str):
        """
        Factory constructor for object belonging to this bucket.
        Does not make any HTTP request, only instantiates an object in a bucket owned by the client.

        Args:
            obj_name (str): Name of object

        Returns:
            The object created.
        """
        return Object(
            bucket=self,
            name=obj_name,
        )

    def objects(
        self,
        obj_names: list = None,
        obj_range: ObjectRange = None,
        obj_template: str = None,
    ):
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
        value: dict = None,
        params: dict = None,
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
        json_val = ActionMsg(action=action, value=value).dict()
        return self.client.request(
            method,
            path=f"{URL_PATH_BUCKETS}/{self.name}",
            json=json_val,
            params=params if params else self.qparam,
        )

    def _verify_ais_bucket(self):
        """
        Verify the bucket provider is AIS
        """
        if self.provider is not ProviderAIS:
            raise InvalidBckProvider(self.provider)

    def verify_cloud_bucket(self):
        """
        Verify the bucket provider is a cloud provider
        """
        if self.provider is ProviderAIS:
            raise InvalidBckProvider(self.provider)
