"""
Iterable Dataset using Batch API for AIS

Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.pytorch import AISBaseIterDataset
from aistore.sdk import Client, AISSource
from typing import Iterator, Tuple, List, Dict, Union
from alive_progress import alive_it


class AISBatchIterDataset(AISBaseIterDataset):
    """
    Custom AIStore PyTorch dataset that uses the AIS batch API for efficient data loading
    with multi-worker support and memory-efficient iteration.

    Args:
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        client (Client): AIStore client instance
        max_batch_size (int, optional): Maximum number of objects to fetch in each batch request. Defaults to 32
        output_format (str, optional): Format for batch response. Defaults to ".tar"
        streaming (bool, optional): Enable streaming mode. Defaults to True
        prefix_map (Dict, optional): Map of AISSource objects to prefixes
        show_progress (bool, optional): Show progress indicator. Defaults to False
    """

    def __init__(
        self,
        ais_source_list,
        client: Client,
        prefix_map: Dict[AISSource, Union[str, List[str]]] = {},
        show_progress: bool = False,
        max_batch_size: int = 32,
        output_format: str = ".tar",
        streaming: bool = True,
    ):
        super().__init__(ais_source_list, prefix_map)
        self.client = client
        self.max_batch_size = max_batch_size
        self.output_format = output_format
        self.streaming = streaming
        self._show_progress = show_progress

    def __iter__(self) -> Iterator[Tuple[str, bytes]]:
        """
        Memory-efficient iterator with multi-worker support using batch API.
        """
        self._reset_iterator()
        # Get worker-specific iterator - this handles multi-worker partitioning
        worker_iter, worker_name = self._get_worker_iter_info()

        # Create progress iterator if needed
        if self._show_progress:
            worker_iter = alive_it(
                worker_iter,
                title="AISBatchIterDataset" + worker_name,
                disable=False,
                force_tty=worker_name == "",
            )

        # Accumulate objects in batches without storing entire iterator
        batch = []
        for obj in worker_iter:
            batch.append(obj)

            # When batch is full, process it
            if len(batch) == self.max_batch_size:
                yield from self._process_batch(batch)

                # Clear batch for next iteration
                batch.clear()

        # Process any remaining objects in the final batch
        if batch:
            yield from self._process_batch(batch)

    def _process_batch(self, batch_objects: List) -> Iterator[Tuple[str, bytes]]:
        """
        Process a batch of objects using the batch API.

        Args:
            batch_objects: List of objects to process in this batch

        Yields:
            Tuple[str, bytes]: Object name and content pairs
        """
        # Create and execute batch request using the new Batch API
        batch = self.client.batch(
            objects=batch_objects,
            output_format=self.output_format,
            streaming_get=self.streaming,
        )

        # Execute batch request and yield individual samples
        batch_response = batch.get()

        # Yield individual samples from batch response
        for obj_info, data in batch_response:
            yield obj_info.obj_name, data
