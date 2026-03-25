"""
PyTorch Map-style Dataset with parallel download acceleration.

Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Optional, Union, Dict

from aistore.sdk import AISSource
from aistore.pytorch.base_map_dataset import AISBaseMapDataset


class AISParallelMapDataset(AISBaseMapDataset):
    """
    Map-style dataset that uses parallel download to fetch objects.

    Parallel download splits each object into byte ranges and fetches them
    concurrently using `num_workers` workers.

    `__getitem__` returns `(object_name, ParallelBuffer)`. The caller (or
    PyTorch DataLoader collate function) is responsible for consuming and
    closing the `ParallelBuffer`.

    Args:
        ais_source_list: Single or list of AISSource objects to load data.
        prefix_map: Map of AISSource to prefix(es) for filtering objects.
        num_workers: Number of concurrent range-read workers per object.
    """

    def __init__(
        self,
        ais_source_list: Optional[Union[AISSource, List[AISSource]]] = None,
        prefix_map: Optional[Dict[AISSource, Union[str, List[str]]]] = None,
        num_workers: int = 16,
    ):
        super().__init__(ais_source_list or [], prefix_map or {})
        self._num_workers = num_workers

    @property
    def num_workers(self) -> int:
        """Number of concurrent range-read workers per object."""
        return self._num_workers

    def __len__(self):
        return len(self._obj_list)

    def __getitem__(self, index: int):
        try:
            obj = self._obj_list[index]
            content = obj.get_reader(
                num_workers=self._num_workers,
            ).read_all()
            return obj.name, content
        except IndexError:
            raise IndexError(
                f"<{self.__class__.__name__}> index must be in bounds of dataset"
            )
