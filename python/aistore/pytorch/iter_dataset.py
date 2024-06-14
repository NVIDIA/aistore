"""
Iterable Dataset for AIS

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.pytorch.base_dataset import AISBaseClassIter
from torch.utils.data import IterableDataset
from typing import List, Union
from aistore.sdk.ais_source import AISSource


class AISIterDataset(AISBaseClassIter, IterableDataset):
    """
    An iterable-style dataset that iterates over objects in AIS.
    If `etl_name` is provided, that ETL must already exist on the AIStore cluster.

    Args:
        client_url (str): AIS endpoint URL
        urls_list (Union[str, List[str]]): Single or list of URL prefixes to load data
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object

    Note:
        Each object is represented as a tuple of object_name (str) and object_content (bytes)
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
                "At least one of urls_list or ais_source_list must be provided."
            )
        super().__init__(client_url, urls_list, ais_source_list)
        self.etl_name = etl_name
        self.length = None

    def __iter__(self):
        self._reset_iterator()
        self.length = 0
        for obj in self._object_iter:
            self.length += 1
            yield obj.name, obj.get(etl_name=self.etl_name).read_all()

    def __len__(self):
        if self.length is None:
            self._reset_iterator()
            self.length = self._calculate_len()
        return self.length

    def _calculate_len(self):
        return sum(1 for _ in self._object_iter)
