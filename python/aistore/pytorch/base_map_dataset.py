"""
Base class for AIS Map Style Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Union, Dict
from abc import ABC, abstractmethod
from torch.utils.data import Dataset
from aistore.sdk import AISSource, Object


class AISBaseMapDataset(ABC, Dataset):
    """
    A base class for creating map-style AIS Datasets. Should not be instantiated directly. Subclasses
    should implement :meth:`__getitem__` which fetches a samples given a key from the dataset and can optionally
    override other methods from torch Dataset such as :meth:`__len__` and :meth:`__getitems__`.

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
        self._obj_list = self._create_objects_list()

    def _create_objects_list(self) -> List[Object]:
        """
        Create a list of all the objects in the given URLs and AIS sources.

        Returns:
            List[Object]: List of all the objects in the given URLs and AIS sources
        """
        samples = []

        for source in self._ais_source_list:
            if source not in self._prefix_map or self._prefix_map[source] is None:
                samples.extend(list(source.list_all_objects_iter(prefix="")))
            else:
                prefixes = (
                    [self._prefix_map[source]]
                    if isinstance(self._prefix_map[source], str)
                    else self._prefix_map[source]
                )
                for prefix in prefixes:
                    samples.extend(list(source.list_all_objects_iter(prefix=prefix)))

        return samples

    def get_obj_list(self) -> List[Object]:
        """
        Getter for internal object data list.

        Returns:
            List[Object]: Object data of the dataset
        """
        return self._obj_list

    @abstractmethod
    def __getitem__(self, index):
        pass

    def __getitems__(self, indices: List[int]):
        return [self.__getitem__(index) for index in indices]
