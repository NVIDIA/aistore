#
# Copyright (c) 2023 - 2025, NVIDIA CORPORATION. All rights reserved.
#
from abc import ABC, abstractmethod
from typing import Iterable, Optional
from aistore.sdk.obj.object import Object
from aistore.sdk.request_client import RequestClient
from aistore.sdk.etl import ETLConfig


# pylint: disable=too-few-public-methods
class AISSource(ABC):
    """
    Interface for all AIS class types providing access to AIS objects via URLs
    """

    @property
    @abstractmethod
    def client(self) -> RequestClient:
        """The client bound to the AISSource."""

    @abstractmethod
    def list_all_objects_iter(
        self, prefix: str = "", props: str = "name,size"
    ) -> Iterable[Object]:
        """
        Get an iterable of all the objects contained in this source (bucket, group, etc.)

        Args:
            prefix (str, optional): Only include objects with names matching this prefix
            props (str, optional): Comma-separated list of object properties to return. Default value is "name,size".
                Properties: "name", "size", "atime", "version", "checksum", "target_url", "copies".

        Returns:
            Iterable over selected objects
        """

    @abstractmethod
    def list_urls(
        self, prefix: str = "", etl: Optional[ETLConfig] = None
    ) -> Iterable[str]:
        """
        Get an iterable of full urls to reference the objects contained in this source (bucket, group, etc.)
        Args:
            prefix (str, optional): Only include objects with names matching this prefix
            etl (Optional[ETLConfig], optional): An optional ETL configuration. If provided, the URLs
                will include ETL processing parameters. Defaults to None.

        Returns:
            Iterable over selected object URLS
        """
