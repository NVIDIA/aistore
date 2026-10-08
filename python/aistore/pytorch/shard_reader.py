"""
AIS Shard Reader for PyTorch

PyTorch Dataset and DataLoader for AIS.

Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.sdk import Bucket
from aistore.sdk.etl.etl_config import ETLConfig
from typing import Dict, Iterator, List, Union
from aistore.pytorch.utils import get_basename, get_extension
from aistore.pytorch.base_iter_dataset import AISBaseIterDataset
from alive_progress import alive_it
from io import BytesIO
from tarfile import open, TarError


class AISShardReader(AISBaseIterDataset):
    """
    An iterable-style dataset that iterates over objects stored as Webdataset shards
    and yields samples represented as a tuple of basename (str) and contents (dictionary).

    Args:
        bucket_list (Union[Bucket, List[Bucket]]): Single or list of Bucket objects to load data
        prefix_map (Dict[Bucket, Union[str, List[str]]], optional): Prefixes to select from each bucket.
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object
        show_progress (bool, optional): Enables console shard reading progress indicator
        partition_sources_by_worker (bool, optional): When True, distributes buckets across
            DataLoader workers so each worker only lists its share, avoiding duplicate paged
            listing calls. Most effective when bucket_list has at least as many buckets
            as workers. Defaults to False.

    Yields:
        Tuple[str, Dict[str, bytes]]: Sample basename and contents grouped by file extension.
    """

    def __init__(
        self,
        bucket_list: Union[Bucket, List[Bucket]],
        prefix_map: Dict[Bucket, Union[str, List[str]]] = {},
        etl_name: str = None,
        show_progress: bool = False,
        partition_sources_by_worker: bool = False,
    ):
        super().__init__(bucket_list, prefix_map, partition_sources_by_worker)
        self._etl_name = etl_name
        self._show_progress = show_progress
        self._observed_keys = set()

    def __len__(self) -> int:
        """
        Return the number of samples in the dataset.

        NOTE:
            - This lists and reads every shard without buffering its full payload.
            - Count samples during iteration to avoid a second read.

        If ETL is enabled, this method counts samples before transformation.
        The count matches iteration only if ETL preserves the sample count
        in each shard.
        """
        length = 0

        for shard in self._create_objects_iter():
            with shard.get_reader().as_file() as shard_stream:
                length += self._count_samples_in_shard(shard_stream)

        return length

    class ZeroDict(dict):
        """
        Fill missing observed extensions with empty bytes.

        PyTorch batch collation requires matching keys and cannot collate None.
        """

        def __init__(self, dict, keys):
            super().__init__(dict)
            for key in keys:
                if key not in self:
                    self[key] = b""

    @staticmethod
    def _iter_sample_members(tar) -> Iterator:
        """Yield regular members with an extension, their basename, and extension."""
        for member in tar:
            if not member.isfile():
                continue
            extension = get_extension(member.name)
            if not extension:
                continue
            yield member, get_basename(member.name), extension

    def _count_samples_in_shard(self, shard_stream) -> int:
        """Count sample basenames without seeking or reading the full payload at once."""
        try:
            with open(fileobj=shard_stream, mode="r|") as tar:
                return len(
                    {basename for _, basename, _ in self._iter_sample_members(tar)}
                )
        except TarError as e:
            raise TarError(
                f"<{self.__class__.__name__}> Error opening tar file: {e}"
            ) from e

    def _read_samples_from_shards(self, shard_content) -> Dict:
        sample_dict = {}

        file = BytesIO(shard_content)

        try:
            with open(fileobj=file, mode="r:") as tar:
                # Collect keys before yielding samples so batch collation is consistent.
                self._observed_keys.update(
                    extension
                    for name in tar.getnames()
                    if (extension := get_extension(name))
                )

                for member, file_basename, file_extension in self._iter_sample_members(
                    tar
                ):
                    if file_basename not in sample_dict:
                        sample_dict[file_basename] = {}
                    sample_dict[file_basename][file_extension] = tar.extractfile(
                        member
                    ).read()
        except TarError as e:
            raise TarError(f"<{self.__class__.__name__}> Error opening tar file: {e}")

        return sample_dict

    def __iter__(self) -> Iterator:
        self._reset_iterator()

        # Get iterator for current worker and name (if no workers, just entire iter)
        worker_iter, worker_name = self._get_worker_iter_info()

        # Read shard, get samples, and yield
        for shard in worker_iter:
            shard_content = shard.get_reader(
                etl=ETLConfig(self._etl_name),
            ).read_all()

            sample_dict = self._read_samples_from_shards(shard_content)

            for basename, content_dict in alive_it(
                sample_dict.items(),
                title=shard.name + worker_name,
                disable=not self._show_progress,
                force_tty=worker_name == "",
            ):
                yield basename, self.ZeroDict(content_dict, self._observed_keys)
