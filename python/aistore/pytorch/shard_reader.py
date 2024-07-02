"""
AIS Shard Reader for PyTorch

PyTorch Dataset and DataLoader for AIS.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.sdk.bucket import Bucket
from typing import Dict, Iterator, List, Union, Iterable
from aistore.sdk.list_object_flag import ListObjectFlag
from aistore.pytorch.utils import get_basename
from aistore.sdk.types import ArchiveSettings
from aistore.pytorch.base_iter_dataset import AISBaseIterDataset


class AISShardReader(AISBaseIterDataset):
    """
    An iterable-style dataset that iterates over objects stored as Webdataset shards.

    Args:
        bucket_list (Union[Bucket, List[Bucket]]): Single or list of Bucket objects to load data
        prefix_map (Dict(AISSource, Union[str, List[str]]), optional): Map of Bucket objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object

    Yields:
        Tuple[str, List[bytes]]: Each item is a tuple where the first element is the basename of the shard
        and the second element is a list of bytes representing the files in the shard.
    """

    def __init__(
        self,
        bucket_list: Union[Bucket, List[Bucket]],
        prefix_map: Dict[Bucket, Union[str, List[str]]] = {},
        etl_name: str = None,
    ):
        super().__init__(bucket_list, prefix_map)
        self._etl_name = etl_name
        self._length = None

    def _get_sample_iter_from_source(self, source: Bucket, prefix: str) -> Iterable:
        """
        Creates an iterable for all samples and contents over each shard from a bucket.

        Args:
            name (str): Name of shard object
            source (Bucket): Bucket where the shard object is stored
            prefix (str): Prefix of objects in bucket which are shards

        Returns:
            Iterable[Tuple[str, dict(str, bytes)]]: Iterator over all the WDS basenames and content (file extension, data)
            in shards from the given shard
        """
        for entry in source.list_objects_iter(prefix=prefix):
            # get iterator of all objects in the shard
            objects_iter = source.list_objects_iter(
                prefix=entry.name, props="name", flags=[ListObjectFlag.ARCH_DIR]
            )

            # pool all files with the same basename into dictionary (basename, [file names])
            samples_dict = {}
            for obj in objects_iter:
                basename = get_basename(obj.name)

                # Original tar is included in basenames so only yield actual files
                if basename != entry.name.split(".")[0]:
                    if basename not in samples_dict:
                        samples_dict[basename] = []
                    samples_dict[basename].append(obj.name)

            # for each basename, get the byte data for each file and yield in dictionary
            shard = source.object(entry.name)
            for basename, files in samples_dict.items():
                content_dict = {}
                for file_name in files:
                    file_prefix = file_name.split(".")[-1]
                    content_dict[file_prefix] = shard.get(
                        etl_name=self._etl_name,
                        archive_settings=ArchiveSettings(archpath=file_name),
                    ).read_all()
                self._length += 1
                yield basename, content_dict

    def __iter__(self) -> Iterator:
        self._reset_iterator()
        self._length = 0
        yield from self._iterator

    def __len__(self):
        if self._length is None:
            self._length = 0
            self._reset_iterator()
        return self._length
