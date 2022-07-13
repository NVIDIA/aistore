#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import List, NewType
import requests

from aistore.client.const import (
    ACT_COPY_BCK,
    ACT_CREATE_BCK,
    ACT_DESTROY_BCK,
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
    QParamProvider
)

from aistore.client.errors import InvalidBckProvider
from aistore.client.object import Object
from aistore.client.types import (ActionMsg, Bck, BucketEntry, BucketList, BucketLister)

Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=unused-variable
class Bucket:
    """
    A class representing a bucket that contains user data.
    
    Args:
        bck_name (str): name of bucket
        provider (str, optional): provider of bucket (one of "ais", "aws", "gcp", ...), defaults to "ais"
        ns (str, optional): namespace of bucket, defaults to ""
    """
    def __init__(self, client, bck_name: str, provider: str = ProviderAIS, ns: str = ""):
        self._client = client
        self._bck = Bck(name=bck_name, provider=provider, ns=ns)
        self._qparam = {QParamProvider: provider}

    @property
    def client(self):
        """The client bound to this bucket."""
        return self._client

    @property
    def bck(self):
        """The custom type [Bck] corresponding to this bucket."""
        return self._bck

    @property
    def qparam(self):
        """The QParamProvider of this bucket."""
        return self._qparam

    @property
    def provider(self):
        """The provider for this bucket."""
        return self.bck.provider

    @property
    def name(self):
        """The name of this bucket."""
        return self.bck.name

    @property
    def namespace(self):
        """The namespace for this bucket."""
        return self.bck.ns

    def create(self):
        """
        Creates a bucket in AIStore cluster.
        Can only create a bucket for AIS provider on localized cluster. Remote cloud buckets do not support creation.
        Args:
            None

        Returns:
            None

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            aistore.client.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        if self.provider is not ProviderAIS:
            raise InvalidBckProvider(self.provider)
        action = ActionMsg(action=ACT_CREATE_BCK).dict()
        self.client.request(
            HTTP_METHOD_POST,
            path=f"buckets/{ self.name }",
            json=action,
            params=self.qparam,
        )

    def delete(self):
        """
        Destroys bucket in AIStore cluster.
        In all cases removes both the bucket's content _and_ the bucket's metadata from the cluster. 
        Note: AIS will _not_ call the remote backend provider to delete the corresponding Cloud bucket 
        (iff the bucket in question is, in fact, a Cloud bucket).

        Args:
            None

        Returns:
            None

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            aistore.client.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        if self.provider is not ProviderAIS:
            raise InvalidBckProvider(self.provider)
        action = ActionMsg(action=ACT_DESTROY_BCK).dict()
        self.client.request(
            HTTP_METHOD_DELETE,
            path=f"buckets/{ self.name }",
            json=action,
            params=self.qparam,
        )

    def rename(self, to_bck: str) -> str:
        """
        Renames bucket in AIStore cluster. 
        Only works on AIS buckets. Returns xaction id that can be used later to check the status of the asynchronous operation.

        Args:
            to_bck (str): New bucket name for bucket to be renamed as
        
        Returns:
            xaction id (as str) that can be used to check the status of the operation

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            aistore.client.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        if self.provider is not ProviderAIS:
            raise InvalidBckProvider(self.provider)
        params = self.qparam.copy()
        params[QParamBucketTo] = f"{ProviderAIS}/@#/{to_bck}/"
        action = ActionMsg(action=ACT_MOVE_BCK).dict()
        resp = self.client.request(HTTP_METHOD_POST, path=f"buckets/{ self.name }", json=action, params=params)
        self.bck.name = to_bck
        return resp.text

    def evict(self, keep_md: bool = False):
        """
        Evicts bucket in AIStore cluster.
        NOTE: only Cloud buckets can be evicted.

        Args:
            keep_md (bool, optional): If true, evicts objects but keeps the bucket's metadata (i.e., the bucket's name and its properties)

        Returns:
            None

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            aistore.client.errors.InvalidBckProvider: Invalid bucket provider for requested operation
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        if self.provider is ProviderAIS:
            raise InvalidBckProvider(self.provider)
        params = self.qparam.copy()
        params[QParamKeepBckMD] = keep_md
        action = ActionMsg(action=ACT_EVICT_REMOTE_BCK).dict()
        self.client.request(
            HTTP_METHOD_DELETE,
            path=f"buckets/{ self.name }",
            json=action,
            params=params,
        )

    def head(self) -> Header:
        """
        Requests bucket properties.

        Args:
            None

        Returns:
            Response header with the bucket properties

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        return self.client.request(
            HTTP_METHOD_HEAD,
            path=f"buckets/{ self.name }",
            params=self.qparam,
        ).headers

    def copy(
        self,
        to_bck_name: str,
        prefix: str = "",
        dry_run: bool = False,
        force: bool = False,
        to_provider: str = ProviderAIS,
    ) -> str:
        """
        Returns xaction id that can be used later to check the status of the asynchronous operation.

        Args:
            to_bck_name (str): Name of the destination bucket
            prefix (str, optional): If set, only the objects starting with
                provider prefix will be copied
            dry_run (bool, optional): Determines if the copy should actually
                happen or not
            force (bool, optional): Override existing destination bucket
            to_provider (str, optional): Name of destination bucket provider

        Returns:
            Xaction id (as str) that can be used to check the status of the operation

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """

        value = {"prefix": prefix, "dry_run": dry_run, "force": force}
        action = ActionMsg(action=ACT_COPY_BCK, value=value).dict()
        params = self.qparam.copy()
        params[QParamBucketTo] = f"{ to_provider }/@#/{ to_bck_name }/"
        return self.client.request(
            HTTP_METHOD_POST,
            path=f"buckets/{ self.name }",
            json=action,
            params=params,
        ).text

    def list_objects(self, prefix: str = "", props: str = "", page_size: int = 0, uuid: str = "", continuation_token: str = "") -> BucketList:
        """
        Returns a structure that contains a page of objects, xaction UUID, and continuation token (to read the next page, if available).

        Args:
            prefix (str, optional): Return only objects that start with the prefix
            props (str, optional): Comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
            page_size (int, optional): Return at most "page_size" objects.
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number objects.
            uuid (str, optional): Job UUID, required to get the next page of objects
            continuation_token (str, optional): Marks the object to start reading the next page

        Returns:
            BucketList: the page of objects in the bucket and the continuation token to get the next page
            Empty continuation token marks the final page of the object list

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        value = {"prefix": prefix, "pagesize": page_size, "uuid": uuid, "props": props, "continuation_token": continuation_token}
        action = ActionMsg(action=ACT_LIST, value=value).dict()

        return self.client.request_deserialize(
            HTTP_METHOD_GET,
            path=f"buckets/{ self.name }",
            res_model=BucketList,
            json=action,
            params=self.qparam,
        )

    def list_objects_iter(
        self,
        prefix: str = "",
        props: str = "",
        page_size: int = 0,
    ) -> BucketLister:
        """
        Returns an iterator for all objects in bucket

        Args:
            prefix (str, optional): Return only objects that start with the prefix
            props (str, optional): Comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
            page_size (int, optional): return at most "page_size" objects
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number objects
                
        Returns:
            BucketLister: object iterator

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        return BucketLister(self.client, bck_name=self.name, provider=self.provider, prefix=prefix, props=props, page_size=page_size)

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
            props (str, optional): comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
            page_size (int, optional): return at most "page_size" objects
                The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
                NOTE: If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
                Defaults to "0" - return maximum number objects

        Returns:
            List[BucketEntry]: list of objects in bucket

        Raises:
            aistore.client.errors.AISError: All other types of errors with AIStore
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.exceptions.HTTPError: Service unavailable 
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ReadTimeout: Timed out receiving response from AIStore
        """
        value = {"prefix": prefix, "uuid": "", "props": props, "continuation_token": "", "pagesize": page_size}
        obj_list = None

        while True:
            resp = self.list_objects(prefix=prefix, props=props, uuid=value["uuid"], continuation_token=value["continuation_token"])
            if obj_list:
                obj_list = obj_list + resp.get_entries()
            obj_list = obj_list or resp.get_entries()
            if resp.continuation_token == "":
                break
            value["continuation_token"] = resp.continuation_token
            value["uuid"] = resp.uuid
        return obj_list

    def object(self, obj_name: str):
        """
        Factory constructor for object bound to bucket. 
        Does not make any HTTP request, only instantiates an object in a bucket owned by the client.

        Args:
            obj_name (str): Name of object
        
        Returns:
            The object created.
        """
        return Object(bck=self, obj_name=obj_name)
