"""
Base class for AIS Iterable Style Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Union, Iterable, Dict, Iterator
from aistore.sdk.ais_source import AISSource
from torch.utils.data import IterableDataset
from abc import ABC, abstractmethod
from aistore.pytorch.worker_request_client import WorkerRequestClient


class AISBaseIterDataset(ABC, IterableDataset):
    """
    A base class for creating AIS Iterable Datasets. Should not be instantiated directly. Subclasses
    should implement :meth:`__iter__` which returns the samples from the dataset and can optionally
    override other methods from torch IterableDataset such as :meth:`__len__`. Additionally,
    to modify the behavior of loading samples from a source, override :meth:`_get_sample_iter_from_source`.

    Args:
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        prefix_map (Dict(AISSource, List[str]), optional): Map of AISSource objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
    """

    def __init__(
        self,
        ais_source_list: Union[AISSource, List[AISSource]],
        prefix_map: Dict[AISSource, Union[str, List[str]]] = {},
    ) -> None:
        if not ais_source_list:
            raise ValueError(
                f"<{self.__class__.__name__}> ais_source_list must be provided"
            )
        self._ais_source_list = (
            [ais_source_list]
            if isinstance(ais_source_list, AISSource)
            else ais_source_list
        )
        self._prefix_map = prefix_map
        self._iterator = None
        self._length = None
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
        length = 0

        for source in self._ais_source_list:
            # Add pytorch worker support to the internal request client
            source.client = WorkerRequestClient(source.client)
            if source not in self._prefix_map or self._prefix_map[source] is None:
                for sample in self._get_sample_iter_from_source(source, ""):
                    length += 1
                    yield sample
            else:
                prefixes = (
                    [self._prefix_map[source]]
                    if isinstance(self._prefix_map[source], str)
                    else self._prefix_map[source]
                )
                for prefix in prefixes:
                    for sample in self._get_sample_iter_from_source(source, prefix):
                        length += 1
                        yield sample

        self._length = length

    @abstractmethod
    def __iter__(self) -> Iterator:
        """
        Return iterator with samples in this dataset.

        Returns:
            Iterator: Iterator of samples
        """
        pass

    def _reset_iterator(self):
        """Reset the iterator to start from the beginning. Also updates length."""
        # Generates the total length from a fresh iterator (COST)
        self._iterator = self._create_samples_iter()

    def __len__(self):
        if self._length is None:
            self._length = sum(1 for _ in self._iterator)
            self._reset_iterator()
        return self._length
