"""
Multishard Stream Dataset for AIS.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from torch.utils.data import IterableDataset
from aistore.sdk.dataset.data_shard import DataShard
from typing import Iterator, List
from aistore.pytorch.utils import list_shard_objects_iterator


class AISMultiShardStream(IterableDataset):
    """
    An iterable-style dataset that iterates over multiple shard streams and yields combined samples.

    Args:
        data_sources (List[DataShard]): List of DataShard objects

    Returns:
        Iterable: Iterable over the combined samples, where each sample is a tuple of
            one object bytes from each shard stream
    """

    def __init__(self, data_sources: List[DataShard]):
        self.data_sources = data_sources

    def __iter__(self) -> Iterator:
        data_iterators = (
            list_shard_objects_iterator(ds.bucket, ds.prefix, ds.etl_name)
            for ds in self.data_sources
        )
        return zip(*data_iterators)
