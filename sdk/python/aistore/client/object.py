#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations  # pylint: disable=unused-variable
from typing import NewType
import requests

from aistore.client.const import (
    DEFAULT_CHUNK_SIZE,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_PUT,
    QParamArchpath,
)

from aistore.client.types import ObjStream

Header = NewType("Header", requests.structures.CaseInsensitiveDict)


# pylint: disable=unused-variable
class Object:
    """
    A class representing an object of a bucket bound to a client.
    
    Args:
        obj_name (str): name of object
    """
    def __init__(self, bck, obj_name: str):
        self._bck = bck
        self._obj_name = obj_name

    @property
    def bck(self):
        """The custom type [Bck] bound to this object."""
        return self._bck

    @property
    def obj_name(self):
        """The name of this object."""
        return self._obj_name

    def head(self) -> Header:
        """
        Requests object properties.

        Args:
            None

        Returns:
            Response header with the object properties.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exeptions.HTTPError(404): The object does not exist
        """
        return self.bck.client.request(
            HTTP_METHOD_HEAD,
            path=f"objects/{ self.bck.name }/{ self.obj_name }",
            params=self.bck.qparam,
        ).headers

    def get(self, archpath: str = "", chunk_size: int = DEFAULT_CHUNK_SIZE) -> ObjStream:
        """
        Reads an object

        Args:
            archpath (str, optional): If the object is an archive, use `archpath` to extract a single file from the archive
            chunk_size (int, optional): chunk_size to use while reading from stream

        Returns:
            The stream of bytes to read an object or a file inside an archive.

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        params = self.bck.qparam
        params[QParamArchpath] = archpath
        resp = self.bck.client.request(HTTP_METHOD_GET, path=f"objects/{ self.bck.name }/{ self.obj_name }", params=params, stream=True)
        length = int(resp.headers.get("content-length", 0))
        e_tag = resp.headers.get("ais-checksum-value", "")
        e_tag_type = resp.headers.get("ais-checksum-type", "")
        return ObjStream(content_length=length, e_tag=e_tag, e_tag_type=e_tag_type, stream=resp, chunk_size=chunk_size)

    def put(self, path: str) -> Header:
        """
        Puts a local file as an object to a bucket in AIS storage.

        Args:
            path (str): path to local file.

        Returns:
            Object properties

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
        """
        url = f"/objects/{ self.bck.name }/{ self.obj_name }"
        with open(path, "rb") as data:
            return self.bck.client.request(
                HTTP_METHOD_PUT,
                path=url,
                params=self.bck.qparam,
                data=data,
            ).headers

    def delete(self):
        """
        Delete an object from a bucket.

        Args:
            None

        Returns:
            None

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            requests.exeptions.HTTPError(404): The object does not exist
        """
        self.bck.client.request(
            HTTP_METHOD_DELETE,
            path=f"objects/{ self.bck.name }/{ self.obj_name }",
            params=self.bck.qparam,
        )
