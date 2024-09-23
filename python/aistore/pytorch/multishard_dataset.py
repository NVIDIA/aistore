"""
Multishard Stream Dataset for AIS.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.sdk import ArchiveConfig, DataShard, ListObjectFlag
from aistore.sdk import Bucket
from typing import Iterator, List, Iterable
from torch.utils.data import IterableDataset


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
        self._data_sources = data_sources

    def __iter__(self) -> Iterator:
        data_iterators = (
            self._get_shard_objects_iterator(ds.bucket, ds.prefix, ds.etl_name)
            for ds in self._data_sources
        )
        return zip(*data_iterators)

    def _get_shard_objects_iterator(
        self, bucket: Bucket, prefix: str = "", etl_name: str = ""
    ) -> Iterable[bytes]:
        """
        Create an iterable over all the objects in the given shards.

        Args:
            bucket (Bucket): Bucket containing the shards
            prefix (str): Prefix of the object names
            etl_name (str): ETL name to apply on each object

        Returns:
            Iterable[Object]: Iterable over all the objects in the given shards,
                            with each iteration returning a combined sample
        """
        shards_iter = bucket.list_objects_iter(prefix=prefix, props="name")

        for shard in shards_iter:
            path = shard.name
            objects_iter = bucket.list_objects_iter(
                prefix=path, props="name", flags=[ListObjectFlag.ARCH_DIR]
            )

            for obj in objects_iter:
                if obj.name != path:
                    obj_name = obj.name.replace(f"{path}/", "", 1)
                    yield bucket.object(path).get(
                        etl_name=etl_name,
                        archive_config=ArchiveConfig(archpath=obj_name),
                    ).read_all()
