#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Generator, Tuple

from aistore.sdk.obj.content_iterator.base import BaseContentIterProvider
from aistore.sdk.obj.object_client import ObjectClient


class ParallelContentIterProvider(BaseContentIterProvider):
    """
    Provide an iterator that fetches object content using concurrent range-reads,
    but yields chunks in order (same interface as ContentIterProvider).

    Args:
        client (ObjectClient): Client for accessing contents of an individual object.
        chunk_size (int): Size of each chunk of data yielded.
        num_workers (int): Number of concurrent workers for fetching chunks.
    """

    def __init__(
        self,
        client: ObjectClient,
        chunk_size: int,
        num_workers: int,
    ):
        # TODO: optimal chunk_size information will be included in the HEAD response in HeadObjectV2 API
        super().__init__(client, chunk_size)
        self._num_workers = num_workers
        # Fetch object size via HEAD request
        self._object_size = self.client.head().size

    @property
    def num_workers(self) -> int:
        """Get the number of concurrent workers."""
        return self._num_workers

    def _fetch_chunk(self, chunk_index: int, start: int, end: int) -> Tuple[int, bytes]:
        """
        Fetch a single chunk by byte range.

        Args:
            chunk_index: Index of this chunk (for ordering).
            start: Start byte offset (inclusive).
            end: End byte offset (exclusive).

        Returns:
            Tuple of (chunk_index, chunk_data).
        """
        return (chunk_index, self.client.get_chunk(start, end))

    def create_iter(self, offset: int = 0) -> Generator[bytes, None, None]:
        """
        Create an iterator over the object content using concurrent range-reads.

        Internally fetches multiple chunks in parallel, but yields them in order.

        Args:
            offset (int, optional): The starting offset in bytes. Defaults to 0.

        Yields:
            bytes: Chunks of the object's content, in order.
        """
        remaining_size = self._object_size - offset
        if remaining_size <= 0:
            return

        # Build list of (chunk_index, start, end) for all chunks
        chunks = []
        current_offset = offset
        chunk_index = 0
        while current_offset < self._object_size:
            chunk_start = current_offset
            chunk_end = min(current_offset + self._chunk_size, self._object_size)
            chunks.append((chunk_index, chunk_start, chunk_end))
            current_offset = chunk_end
            chunk_index += 1

        # Fetch in parallel, yield in order
        buffer: Dict[int, bytes] = {}
        next_to_yield = 0

        with ThreadPoolExecutor(max_workers=self._num_workers) as executor:
            futures = {
                executor.submit(self._fetch_chunk, idx, start, end): idx
                for idx, start, end in chunks
            }

            try:
                for future in as_completed(futures):
                    idx, data = future.result()
                    buffer[idx] = data

                    # Yield all consecutive chunks ready in order
                    while next_to_yield in buffer:
                        yield buffer.pop(next_to_yield)
                        next_to_yield += 1
            except Exception:
                # Cancel pending futures and don't wait for them
                executor.shutdown(wait=False, cancel_futures=True)
                raise
