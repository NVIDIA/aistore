"""
Dynamic Batch Sampler for Dynamic Batch Sizing

In scenarios where memory is a constraint, the DynamicBatchSampler
can be used to generate mini-batches that fit within a memory constraint
so that there is a guarantee that each batch fits within memory
while attempting to fit the maximum number of samples in each batch.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

import torch
from typing import Iterator, List
from aistore.pytorch.base_map_dataset import AISBaseMapDataset
from logging import getLogger


# Default saturation of a batch needed to not be dropped with drop_last=True
SATURATION_FACTOR = 0.8
logger = getLogger(__name__)


class DynamicBatchSampler(torch.utils.data.Sampler):
    """

    Dynamically adds samples to mini-batch up to a maximum batch size.

    NOTE: Using this sampler with AISBaseMapDatasets that use ObjectGroups
    in their ais_source_lists will be slower than using it with Buckets as
    ObjectGroups will perform one extra API call per object to get size metadata.

    Args:
        data_source (AISBaseMapDataset): Base AIS map-style dataset to sample from to create dynamic mini-batches.
        max_batch_size (float): Maximum size of mini-batch in bytes.
        drop_last (bool, optional): If `True`, then will drop last batch if the batch is not atleast 80% of `max_batch_size`.
            Defaults to `False`.
        allow_oversized_samples (bool, optional): If `True`, then any sample that is larger than the `max_batch_size` will be processed
            in its own min-batch by itself instead of being dropped. Defaults to `False`.
        saturation_factor (float, optional): Saturation of a batch needed to not be dropped with `drop_last=True`. Default is `0.8`.
        shuffle (bool, optional): Randomizes order of samples before calculating mini-batches. Default is `False`.
    """

    def __init__(
        self,
        data_source: AISBaseMapDataset,
        max_batch_size: float,
        drop_last: bool = False,
        allow_oversized_samples: bool = False,
        saturation_factor: float = SATURATION_FACTOR,
        shuffle: bool = False,
    ) -> None:
        super().__init__()
        self._data_source = data_source
        self._max_batch_size = max_batch_size
        self._samples_list = data_source.get_obj_list()
        self._drop_last = drop_last
        self._allow_oversized_samples = allow_oversized_samples
        if not (0 <= saturation_factor <= 1):
            raise ValueError(f"`saturation_factor` must be between 0 and 1")
        self._saturation_factor = saturation_factor
        self._shuffle = shuffle

    def __iter__(self) -> Iterator[List[int]]:
        """
        Returns an iterator containing mini-batches (lists of indices).
        """
        total_mem = 0
        batch = []

        if self._shuffle:
            self._indices = list(
                torch.randperm(len(self))
            )  # randomized list of indices

        # Get sample size for each index, check if there is space in the batch, and yield batches whenever full
        # Calculate spaces in batch non-preemptively
        index = self._get_next_index(-1)
        while index < len(self._samples_list):
            sample = self._samples_list[index]

            if sample.props.size == 0:
                logger.warning(
                    f"Sample {sample.name} cannot be processed as it has a size of 0 bytes"
                )
                index = self._get_next_index(index)
                continue

            if sample.props.size > self._max_batch_size:
                if self._allow_oversized_samples is True:
                    yield [index]
                else:
                    logger.warning(
                        f"Sample {sample.name} cannot be processed as it is larger than the max batch size: {sample.props.size} bytes > {self._max_batch_size} bytes"
                    )

                index = self._get_next_index(index)
                continue

            if total_mem + sample.props.size < self._max_batch_size:
                batch.append(index)
                index = self._get_next_index(index)
                total_mem += sample.props.size
            else:

                if total_mem + sample.props.size == self._max_batch_size:
                    batch.append(index)
                    index = self._get_next_index(index)

                yield batch
                batch = []
                total_mem = 0

        # If batch exists and we are not dropping last or if we are dropping last but the batch is saturated
        # then yield the last batch
        if (batch and not self._drop_last) or (
            self._drop_last
            and total_mem / self._max_batch_size > self._saturation_factor
        ):
            yield batch

    def __len__(self) -> int:
        """
        Returns the total number of samples.
        """
        return len(self._samples_list)

    def _get_next_index(self, index) -> int:
        """
        Get next index from indices if shuffling or otherwise return incremented count.

        Returns:
            index (int): Next index to sample from
        """
        if self._shuffle:
            if len(self._indices) == 0:
                return len(self._samples_list)
            return int(self._indices.pop())
        index += 1
        return index
