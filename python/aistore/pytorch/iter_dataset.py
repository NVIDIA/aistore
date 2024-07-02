"""
Iterable Dataset for AIS

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.pytorch.base_iter_dataset import AISBaseIterDataset
from typing import List, Union, Dict
from aistore.sdk.ais_source import AISSource


class AISIterDataset(AISBaseIterDataset):
    """
    An iterable-style dataset that iterates over objects in AIS.
    If `etl_name` is provided, that ETL must already exist on the AIStore cluster.

    Args:
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        prefix_map (Dict(AISSource, Union[str, List[str]]), optional): Map of AISSource objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object

    Note:
        Each object is represented as a tuple of object_name (str) and object_content (bytes)
    """

    def __init__(
        self,
        ais_source_list: Union[AISSource, List[AISSource]],
        prefix_map: Dict[AISSource, Union[str, List[str]]] = {},
        etl_name: str = None,
    ):
        super().__init__(ais_source_list, prefix_map)
        self._etl_name = etl_name
        self._length = None

    def __iter__(self):
        self._reset_iterator()
        self._length = 0
        for obj in self._iterator:
            yield obj.name, obj.get(etl_name=self._etl_name).read_all()

    def __len__(self):
        if self._length is None:
            self._reset_iterator()
            self._length = sum(1 for _ in self._iterator)
        return self._length
