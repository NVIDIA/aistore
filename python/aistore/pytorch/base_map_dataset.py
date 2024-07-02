"""
Base class for AIS Map Style Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Union, Dict
from aistore.sdk.ais_source import AISSource
from aistore.sdk import Client
from aistore.sdk.object import Object
from torch.utils.data import Dataset
from abc import ABC, abstractmethod


class AISBaseMapDataset(ABC, Dataset):
    """
    A base class for creating map-style AIS Datasets. Should not be instantiated directly. Subclasses
    should implement :meth:`__getitem__` which fetches a samples given a key from the dataset and can optionally
    override other methods from torch IterableDataset such as :meth:`__len__`.  Additionally,
    to modify the behavior of loading samples from a source, override :meth:`_get_sample_list_from_source`.

    Args:
        client_url (str): AIS endpoint URL
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        prefix_map (Dict(AISSource, List[str]), optional): Map of AISSource objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
    """

    def __init__(
        self,
        client_url: str,
        ais_source_list: Union[AISSource, List[AISSource]],
        prefix_map: Dict[AISSource, Union[str, List[str]]] = {},
    ) -> None:
        if not ais_source_list:
            raise ValueError("ais_source_list must be provided")
        self._client = Client(client_url)
        self._ais_source_list = (
            [ais_source_list]
            if isinstance(ais_source_list, AISSource)
            else ais_source_list
        )
        self._prefix_map = prefix_map
        self._samples = self._create_samples_list()

    def _get_sample_list_from_source(self, source: AISSource, prefix: str) -> List:
        """
        Creates an list of samples from the AISSource and the objects stored within. Must be able to handle prefixes
        as well. The default implementation returns an list of objects. This method can be overridden
        to provides other functionality (such as reading the data and creating usable samples for different
        file types).

        Args:
            source (AISSource): AISSource (:class:`aistore.sdk.ais_source.AISSource`) provides an interface for accessing a list of
            AIS objects or their URLs
            prefix (str): Prefix to dictate what objects should be included

        Returns:
            List: List over the content of the dataset
        """
        return [obj for obj in source.list_all_objects_iter(prefix=prefix)]

    def _create_samples_list(self) -> List[Object]:
        """
        Create a list of all the objects in the given URLs and AIS sources.

        Returns:
            List[Object]: List of all the objects in the given URLs and AIS sources
        """
        samples = []

        for source in self._ais_source_list:
            if source not in self._prefix_map or self._prefix_map[source] is None:
                samples.extend(self._get_sample_list_from_source(source, ""))
            else:
                prefixes = (
                    [self._prefix_map[source]]
                    if isinstance(self._prefix_map[source], str)
                    else self._prefix_map[source]
                )
                for prefix in prefixes:
                    samples.extend(self._get_sample_list_from_source(source, prefix))

        return samples

    @abstractmethod
    def __getitem__(self, index):
        pass
