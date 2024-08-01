"""
Dynamic Batch Sampler for Dynamic Batch Sizing

In scenarios where memory is a constraint, the DynamicBatchSampler
can be used to generate mini-batches that fit within a memory constraint
so that there is a guarantee that each batch fits within memory
while attempting to fit the maximum number of samples in each batch.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from torch.utils.data import Sampler
from typing import Iterator, List
from aistore.pytorch.base_map_dataset import AISBaseMapDataset
from logging import getLogger

# Saturation of a batch needed to not be dropped with drop_last=True
SATURATION_FACTOR = 0.8


class DynamicBatchSampler(Sampler):
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
    """

    def __init__(
        self,
        data_source: AISBaseMapDataset,
        max_batch_size: float,
        drop_last: bool = False,
        allow_oversized_samples: bool = False,
    ) -> None:
        self._data_source = data_source
        self._max_batch_size = max_batch_size
        self._samples_list = data_source._create_objects_list()
        self._drop_last = drop_last
        self._allow_oversized_samples = allow_oversized_samples
        self._logger = getLogger(f"{__name__}.put_files")

    def __iter__(self) -> Iterator[List[int]]:
        """
        Returns an iterator containing mini-batches (lists of indices).
        """
        total_mem = 0
        batch = []
        index = 0

        # get sample size for each index, check if there is space in the batch, and yield batches whenever full
        # calculate spaces in batch non-preemptively
        while index < len(self):
            sample = self._samples_list[index]

            if sample.props.size > self._max_batch_size:
                if self._allow_oversized_samples is True:
                    yield [index]
                else:
                    self._logger.warn(
                        f"Sample {sample.name} cannot be processed as it is larger than the max batch size: {sample.props.size} bytes > {self._max_batch_size} bytes"
                    )

                index += 1
                continue

            if total_mem + sample.props.size < self._max_batch_size:
                batch.append(index)
                index += 1
                total_mem += sample.props.size
            else:

                if total_mem + sample.props.size == self._max_batch_size:
                    batch.append(index)
                    index += 1

                yield batch
                batch = []
                total_mem = 0

        # if batch exists and we are not dropping last or if we are dropping last but the batch is saturated
        # then yield the last batch
        if (batch and not self._drop_last) or (
            self._drop_last and total_mem / self._max_batch_size > SATURATION_FACTOR
        ):
            yield batch

    def __len__(self) -> int:
        """
        Returns the total number of samples.
        """
        return len(self._samples_list)
