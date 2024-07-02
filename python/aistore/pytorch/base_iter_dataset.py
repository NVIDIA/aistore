"""
Base class for AIS Iterable Style Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Union, Iterable, Dict, Iterator
from aistore.sdk.ais_source import AISSource
from aistore.sdk import Client
from torch.utils.data import IterableDataset
from abc import ABC, abstractmethod


class AISBaseIterDataset(ABC, IterableDataset):
    """
    A base class for creating AIS Iterable Datasets. Should not be instantiated directly. Subclasses
    should implement :meth:`__iter__` which returns the samples from the dataset and can optionally
    override other methods from torch IterableDataset such as :meth:`__len__`. Additionally,
    to modify the behavior of loading samples from a source, override :meth:`_get_sample_iter_from_source`.

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
        self._iterator = None
        self._reset_iterator()

    def _get_sample_iter_from_source(self, source: AISSource, prefix: str) -> Iterable:
        """
        Creates an iterable of samples from the AISSource and the objects stored within. Must be able to handle prefixes
        as well. The default implementation returns an iterable of Objects. This method can be overridden
        to provides other functionality (such as reading the data and creating usable samples for different
        file types).

        Args:
            source (AISSource): AISSource (:class:`aistore.sdk.ais_source.AISSource`) provides an interface for accessing a list of
            AIS objects or their URLs
            prefix (str): Prefix to dictate what objects should be included

        Returns:
            Iterable: Iterable over the content of the dataset
        """
        yield from source.list_all_objects_iter(prefix=prefix)

    def _create_samples_iter(self) -> Iterable:
        """
        Create an iterable given the AIS sources and associated prefixes.

        Returns:
            Iterable: Iterable over the samples of the dataset
        """
        for source in self._ais_source_list:
            if source not in self._prefix_map or self._prefix_map[source] is None:
                yield from self._get_sample_iter_from_source(source, "")
            else:
                prefixes = (
                    [self._prefix_map[source]]
                    if isinstance(self._prefix_map[source], str)
                    else self._prefix_map[source]
                )
                for prefix in prefixes:
                    yield from self._get_sample_iter_from_source(source, prefix)

    @abstractmethod
    def __iter__(self) -> Iterator:
        """
        Return iterator with samples in this dataset.

        Returns:
            Iterator: Iterator of samples
        """
        pass

    def _reset_iterator(self):
        """Reset the iterator to start from the beginning"""
        self._iterator = self._create_samples_iter()
