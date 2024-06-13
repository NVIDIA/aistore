"""
Base classes for AIS Datasets and Iterable Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Union
from aistore.sdk.ais_source import AISSource
from aistore.pytorch.utils import list_objects, list_objects_iterator
from aistore.sdk import Client


class AISBaseClass:
    """
    A base class for creating AIS Datasets for PyTorch.

    Args:
        client_url (str): AIS endpoint URL
        urls_list (Union[str, List[str]]): Single or list of URL prefixes to load data
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
    """

    def __init__(
        self,
        client_url: str,
        urls_list: Union[str, List[str]],
        ais_source_list: Union[AISSource, List[AISSource]],
    ) -> None:
        self.client = Client(client_url)
        if isinstance(urls_list, str):
            urls_list = [urls_list]
        if isinstance(ais_source_list, AISSource):
            ais_source_list = [ais_source_list]
        self._objects = list_objects(self.client, urls_list, ais_source_list)


class AISBaseClassIter:
    """
    A base class for creating AIS Iterable Datasets for PyTorch.

    Args:
        client_url (str): AIS endpoint URL
        urls_list (Union[str, List[str]]): Single or list of URL prefixes to load data
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
    """

    def __init__(
        self,
        client_url: str,
        urls_list: Union[str, List[str]],
        ais_source_list: Union[AISSource, List[AISSource]],
    ) -> None:
        self.client = Client(client_url)
        if isinstance(urls_list, str):
            urls_list = [urls_list]
        if isinstance(ais_source_list, AISSource):
            ais_source_list = [ais_source_list]
        self.urls_list = urls_list
        self.ais_source_list = ais_source_list
        self._reset_iterator()

    def _reset_iterator(self):
        """Reset the object iterator to start from the beginning"""
        self._object_iter = list_objects_iterator(
            self.client, self.urls_list, self.ais_source_list
        )
