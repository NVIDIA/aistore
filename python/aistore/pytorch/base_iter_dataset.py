"""
Base class for AIS Iterable Style Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from itertools import islice
from typing import List, Union, Iterable, Dict, Iterator, Tuple
import torch.utils.data as torch_utils
from abc import ABC, abstractmethod
from aistore.sdk import AISSource


class AISBaseIterDataset(ABC, torch_utils.IterableDataset):
    """
    A base class for creating AIS Iterable Datasets. Should not be instantiated directly. Subclasses
    should implement :meth:`__iter__` which returns the samples from the dataset and can optionally
    override other methods from torch IterableDataset such as :meth:`__len__`.

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
        self._obj_iterator = None

    def _create_objects_iter(self) -> Iterable:
        """
        Create an iterable of objects given the AIS sources and associated prefixes.

        Returns:
            Iterable: Iterable over the objects from the sources provided
        """
        for source in self._ais_source_list:
            if source not in self._prefix_map or self._prefix_map[source] is None:
                for sample in source.list_all_objects_iter(prefix=""):
                    yield sample
            else:
                prefixes = (
                    [self._prefix_map[source]]
                    if isinstance(self._prefix_map[source], str)
                    else self._prefix_map[source]
                )
                for prefix in prefixes:
                    for sample in source.list_all_objects_iter(prefix=prefix):
                        yield sample

    def _get_worker_iter_info(self) -> Tuple[Iterator, str]:
        """
        Depending on how many Torch workers are present or if they are even present at all,
        return an iterator for the current worker to access and a worker name.

        Returns:
            tuple[Iterator, str]: Iterator of objects and name of worker
        """
        worker_info = torch_utils.get_worker_info()

        if worker_info is None or worker_info.num_workers == 1:
            return self._obj_iterator, ""

        worker_iter = islice(
            self._obj_iterator, worker_info.id, None, worker_info.num_workers
        )
        worker_name = f" (Worker {worker_info.id})"

        return worker_iter, worker_name

    @abstractmethod
    def __iter__(self) -> Iterator:
        """
        Return iterator with samples in this dataset.

        Returns:
            Iterator: Iterator of samples
        """
        pass

    def _reset_iterator(self):
        """Reset the object iterator to start from the beginning."""
        self._obj_iterator = self._create_objects_iter()

    def __len__(self):
        """
        Returns the length of the dataset. Note that calling this
        will iterate through the dataset, taking O(N) time.

        NOTE: If you want the length of the dataset after iterating through
        it, use `for i, data in enumerate(dataset)` instead.
        """
        self._reset_iterator()
        sum = 0

        for _ in self._obj_iterator:
            sum += 1

        return sum
