"""
AIS Shard Reader for PyTorch

PyTorch Dataset and DataLoader for AIS.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.sdk import Bucket, ListObjectFlag
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
        prefix_map (Dict(AISSource, Union[str, List[str]]), optional): Map of Bucket objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object
        show_progress (bool, optional): Enables console shard reading progress indicator

    Yields:
        Tuple[str, Dict(str, bytes)]: Each item is a tuple where the first element is the basename of the shard
        and the second element is a dictionary mapping strings of file extensions to bytes.
    """

    def __init__(
        self,
        bucket_list: Union[Bucket, List[Bucket]],
        prefix_map: Dict[Bucket, Union[str, List[str]]] = {},
        etl_name: str = None,
        show_progress: bool = False,
    ):
        super().__init__(bucket_list, prefix_map)
        self._etl_name = etl_name
        self._show_progress = show_progress
        self._observed_keys = set()

    def __len__(self):
        """
        Returns the length of the dataset. Note that calling this
        will iterate through the dataset, taking O(N) time.

        NOTE: If you want the length of the dataset after iterating through
        it, use `for i, data in enumerate(dataset)` instead.
        """
        self._reset_iterator()
        length = 0

        for shard in self._obj_iterator:

            for _ in shard.bucket.list_objects_iter(
                prefix=shard.name, props="name", flags=[ListObjectFlag.ARCH_DIR]
            ):
                length += 1

            length -= 1  # Exclude the bucket (overcounted earlier)

        return length

    class ZeroDict(dict):
        """
        When `collate_fn` is called while using ShardReader with a dataloader,
        the content dictionaries for each sample are merged into a single dictionary
        with file extensions as keys and lists of contents as values. This means,
        however, that each sample must have a value for that file extension in the batch
        at iteration time or else collation will fail. To avoid forcing the user to
        pass in a custom collation function, we workaround the default implementation
        of collation.

        As such, we define a dictionary that has a default value of `b""` (zero bytes)
        for every key that we have seen so far. We cannot use None as collation
        does not accept None. Initially, when we open a shard tar, we collect every file type
        (pre-processing pass) from its members and cache those. Then, we read the shard files.
        Lastly, before yielding the sample, we wrap its content dictionary with this custom dictionary
        to insert any keys that it does not contain, hence ensuring consistent keys across
        samples.

        NOTE: For our use case, `defaultdict` does not work due to needing
        a `lambda` which cannot be pickled in multithreaded contexts.
        """

        def __init__(self, dict, keys):
            super().__init__(dict)
            for key in keys:
                if key not in self:
                    self[key] = b""

    def _read_samples_from_shards(self, shard_content) -> Dict:
        sample_dict = {}

        file = BytesIO(shard_content)

        try:
            # Open the shard as a tarfile as read samples into dict
            with open(fileobj=file, mode="r:") as tar:

                # Preprocess every key in the archive to ensure consistency in batch collation
                self._observed_keys.update(
                    [get_extension(name) for name in tar.getnames()]
                )

                for member in tar.getmembers():
                    if member.isfile():
                        file_basename = get_basename(member.name)
                        file_extension = get_extension(member.name)
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
            shard_content = shard.get(
                etl_name=self._etl_name,
            ).read_all()

            sample_dict = self._read_samples_from_shards(shard_content)

            for basename, content_dict in alive_it(
                sample_dict.items(),
                title=shard.name + worker_name,
                disable=not self._show_progress,
                force_tty=worker_name == "",
            ):
                yield basename, self.ZeroDict(content_dict, self._observed_keys)
