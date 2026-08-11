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
        partition_sources_by_worker (bool, optional): When True, distributes sources across
            DataLoader workers so each worker only lists its share, avoiding duplicate paged
            listing calls. Most effective when ais_source_list has at least as many sources
            as workers. Defaults to False.
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
        partition_sources_by_worker: bool = False,
    ):
        super().__init__(ais_source_list, prefix_map, partition_sources_by_worker)
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
        worker_iter, worker_name = self._get_worker_iter_info()

        if self._show_progress:
            worker_iter = alive_it(
                worker_iter,
                title="AISBatchIterDataset" + worker_name,
                disable=False,
                force_tty=worker_name == "",
            )

        pending = []
        for obj in worker_iter:
            pending.append(obj)
            if len(pending) == self.max_batch_size:
                yield from self._process_batch(pending)
                pending.clear()

        if pending:
            yield from self._process_batch(pending)

    def _process_batch(self, batch_objects: List) -> Iterator[Tuple[str, bytes]]:
        """
        Process a batch of objects using the batch API.

        Args:
            batch_objects: List of objects to process in this batch

        Yields:
            Tuple[str, bytes]: Object name and content pairs
        """
        batch = self.client.batch(
            objects=batch_objects,
            output_format=self.output_format,
            streaming_get=self.streaming,
        )
        for obj_info, data in batch.get():
            yield obj_info.obj_name, data
