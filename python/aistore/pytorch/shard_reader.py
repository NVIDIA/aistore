"""
AIS Shard Reader for PyTorch

PyTorch Dataset and DataLoader for AIS.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.sdk.bucket import Bucket
from torch.utils.data import IterableDataset
from typing import Iterator, List, Union
from aistore.pytorch.utils import list_wds_samples_iter
from aistore.sdk import Client


class AISShardReader(IterableDataset):
    """
    An iterable-style dataset that iterates over objects stored as Webdataset shards.

    Args:
        client_url (str): AIS endpoint URL
        urls_list (Union[str, List[str]]): Single or list of URLs, can be URLS for buckets and/or objects
        bucket_list (Union[Bucket, List[Bucket]]): Single or list of Bucket objects to load data
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object

    Yields:
        Tuple[str, List[bytes]]: Each item is a tuple where the first element is the basename of the shard
        and the second element is a list of bytes representing the files in the shard.
    """

    def __init__(
        self,
        client_url: str,
        urls_list: Union[str, List[str]] = [],
        bucket_list: Union[Bucket, List[Bucket]] = [],
        etl_name: str = None,
    ):
        if not urls_list and not bucket_list:
            raise ValueError(
                "At least one of urls_list or bucket_list must be provided"
            )

        self.client = Client(client_url)
        self.urls_list = [urls_list] if isinstance(urls_list, str) else urls_list
        self.bucket_list = (
            [bucket_list] if isinstance(bucket_list, Bucket) else bucket_list
        )
        self.etl_name = etl_name
        self.length = None
        self._reset_iterator()

    def __iter__(self) -> Iterator:
        self._reset_iterator()
        self.length = 0
        for basename, content_dict in self._samples_iter:
            self.length += 1
            yield basename, content_dict

    def _reset_iterator(self):
        """
        Reset the iterator to start from the beginning
        """
        self._samples_iter = list_wds_samples_iter(
            client=self.client,
            urls_list=self.urls_list,
            bucket_list=self.bucket_list,
            etl_name=self.etl_name,
        )

    def __len__(self):
        if self.length is None:
            self._reset_iterator()
            self.length = self._calculate_len()
        return self.length

    def _calculate_len(self):
        return sum(1 for _ in self._samples_iter)
