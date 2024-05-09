"""
AIS Plugin for PyTorch

PyTorch Dataset and DataLoader for AIS.

Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import Iterator, List, Union
from torch.utils.data import Dataset, IterableDataset

from aistore.sdk import Client
from aistore.sdk.ais_source import AISSource
from aistore.sdk.dataset.data_shard import DataShard
from aistore.pytorch.utils import (
    list_objects,
    list_objects_iterator,
    list_shard_objects_iterator,
)


class AISBaseClass:
    """
    A base class for creating AIS Datasets for PyTorch

    Args:
        client_url(str): AIS endpoint URL
        urls_list (str, List[str]): single or list of url prefixes objects to load data
        ais_source_list (AISSource, List[AISSource]): single or list of AISSource objects to load data
    """

    def __init__(
        self,
        client_url: str,
        urls_list: Union[str, List],
        ais_source_list: Union[AISSource, List[AISSource]],
    ) -> None:
        self.client = Client(client_url)
        if isinstance(urls_list, str):
            urls_list = [urls_list]
        if isinstance(ais_source_list, AISSource):
            ais_source_list = [ais_source_list]

        self._objects = list_objects(self.client, urls_list, ais_source_list)


class AISDataset(AISBaseClass, Dataset):
    """
    A map-style dataset for objects in AIS
    If `etl_name` is provided, that ETL must already exist on the AIStore cluster

    Args:
        client_url (str): AIS endpoint URL
        urls_list (str, List[str]): single or list of url prefixes objects to load data
        ais_source_list (AISSource, List[AISSource]): single or list of AISSource objects to load data
        etl_name (str, optional): Optional etl on the AIS cluster to apply to each object

    Note:
        Each object is represented as a tuple of object_name(str) and object_content(bytes)
    """

    def __init__(
        self,
        client_url: str,
        urls_list: Union[str, List[str]] = [],
        ais_source_list: Union[AISSource, List[AISSource]] = [],
        etl_name: str = None,
    ):
        if not urls_list and not ais_source_list:
            raise ValueError(
                "At least one of urls_list or ais_source_list must be provided"
            )
        AISBaseClass.__init__(self, client_url, urls_list, ais_source_list)
        self.etl_name = etl_name

    def __len__(self):
        return len(self._objects)

    def __getitem__(self, index: int):
        obj = self._objects[index]
        obj_name = self._objects[index].name
        content = obj.get(etl_name=self.etl_name).read_all()
        return obj_name, content


class AISBaseClassIter:
    """
    A base class for creating AIS Iterable Datasets for PyTorch

    Args:
        client_url (str): AIS endpoint URL
        urls_list (str, List[str]): single or list of url prefixes objects to load data
        ais_source_list (AISSource, List[AISSource]): single or list of AISSource objects to load data
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
        """
        Reset the object iterator to start from the beginning
        """
        self._object_iter = list_objects_iterator(
            self.client, self.urls_list, self.ais_source_list
        )


class AISIterDataset(AISBaseClassIter, IterableDataset):
    """
    A iterable style dataset which iterates over objects in AIS
    If `etl_name` is provided, that ETL must already exist on the AIStore cluster

    Args:
        client_url (str): AIS endpoint URL
        urls_list (str, List[str]): single or list of url prefixes objects to load data
        ais_source_list (AISSource, List[AISSource]): single or list of AISSource objects to load data
        etl_name (str, optional): Optional etl on the AIS cluster to apply to each object

    Note:
        Each object is represented as a tuple of object_name(str) and object_content(bytes)
    """

    def __init__(
        self,
        client_url: str,
        urls_list: Union[str, List[str]] = [],
        ais_source_list: Union[AISSource, List[AISSource]] = [],
        etl_name: str = None,
    ):
        if not urls_list and not ais_source_list:
            raise ValueError(
                "At least one of urls_list or ais_source_list must be provided"
            )
        AISBaseClassIter.__init__(self, client_url, urls_list, ais_source_list)
        self.etl_name = etl_name
        self.length = None

    def __iter__(self):
        self._reset_iterator()
        for obj in self._object_iter:
            obj_name = obj.name
            content = obj.get(etl_name=self.etl_name).read_all()
            yield obj_name, content

    def __len__(self):
        if not self.length:
            self._reset_iterator()
            self.length = self._calculate_len()
        return self.length

    def _calculate_len(self):
        return sum(1 for _ in self._object_iter)


class AISMultiShardStream(IterableDataset):
    """
    A iterable style dataset which iterates over multiple shard streams and yields combined samples

    Args:
        data_sources (List[DataShard]): List of DataShard objects

    Returns:
        Iterable: Iterable over the combined samples, where each sample is a tuple of
            one object bytes from each shard stream
    """

    def __init__(
        self,
        data_sorces: List[DataShard],
    ):
        self.data_sources = data_sorces

    def __iter__(self) -> Iterator:
        object_iter = self._combined_iterator()
        for obj in object_iter:
            yield obj

    def _combined_iterator(self):
        """
        Combine multiple iterators into one
        """
        data_iterators = []
        for data_source in self.data_sources:
            bucket, prefix, etl_name = (
                data_source.bucket,
                data_source.prefix,
                data_source.etl_name,
            )
            data_iterator = list_shard_objects_iterator(bucket, prefix, etl_name)
            data_iterators.append(data_iterator)

        return zip(*data_iterators)
