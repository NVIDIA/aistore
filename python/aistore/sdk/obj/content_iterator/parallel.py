#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

"""
Parallel download via a shared-memory ring buffer.

The object is split into chunks fetched concurrently by forked workers.
Workers write directly into a fixed-size shared memory segment (no copy).

 /dev/shm  ┌──────────┬──────────┬──────────┐
           │ [slot 0] │ [slot 1] │ [slot 2] │  <- num_slots x slot_size bytes
           └──────────┴──────────┴──────────┘
 chunks:   [ chunk 0 ][ chunk 1 ][ chunk 2 ][ chunk 3 ].. slot = chunk_idx % num_slots

The main process consumes slots, yields the data in order, and immediately re-submits
each freed slot, keeping exactly `num_workers` requests in flight at all times.

 Main Process                       Forked Workers
 ------------                       -----------------------------------------
                                     [slot 0]      [slot 1]      [slot 2]
                                        │             │             │
 submit chunk 0 ------------------> write slot 0      │             │
 submit chunk 1 ------------------------------> write slot 1        │
 submit chunk 2 ----------------------------------------> write slot 2
                                        │             │             │
 future[0].result()  <-- (n bytes) done |             │             │
 wait_slot(0)        <-- event[0] set   |             │             │
 yield read_slot(0)                     │             │             │
 submit chunk 3 ------------------> write slot 0      │             │
                                        |             │             │
 future[1].result()  <-- (n bytes)      │             │             │
 wait_slot(1)        <-- event[1] set   |             │             │
 yield read_slot(1)                     │             │             │
 submit chunk 4 ------------------------------> write slot 1        │
 ...                                  ...           ...           ...
"""

import multiprocessing as mp
from dataclasses import dataclass, field
from multiprocessing import shared_memory
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Dict, Generator, List, Optional, Tuple

from aistore.sdk.obj.content_iterator.base import BaseContentIterProvider
from aistore.sdk.obj.object_client import ObjectClient
from aistore.sdk.const import (
    PROPS_CHUNKED,
    DEFAULT_PARALLEL_CHUNK_SIZE,
    DEFAULT_CHUNK_SIZE,
)


@dataclass
class WorkerState:
    """
    Per-worker state populated by _init_worker before any task is dispatched.

    This instance lives at module scope so each forked worker inherits it via
    fork's memory copy. _init_worker populates it once per worker; _fetch_chunk
    reads it on every invocation without re-serialization.
    """

    client: Optional[ObjectClient] = (
        None  # ObjectClient for the worker to use to download chunks
    )
    shm: Optional[shared_memory.SharedMemory] = (
        None  # Shared memory segment as the ring buffer for the worker to write to
    )
    slot_ready: List[mp.Event] = field(
        default_factory=list
    )  # Events for each slot to signal when data is ready
    num_slots: int = 0  # Number of slots in the ring buffer


# Module-level singleton inherited by each forked worker via fork's memory copy.
worker_state = WorkerState()


def _init_worker(
    client: ObjectClient,
    shm_name: str,
    slot_ready: List[mp.Event],
    num_slots: int,
) -> None:
    """
    ProcessPoolExecutor initializer hook — runs once per worker after forking.

    Populates `worker_state` before any `_fetch_chunk` task is dispatched so the
    worker is fully initialized before it does any work.
    """
    worker_state.client = client
    # Main process owns the segment and unlinks it via ring.close();
    # the worker's fd is reclaimed by the OS on process exit.
    worker_state.shm = shared_memory.SharedMemory(name=shm_name)
    worker_state.slot_ready = slot_ready
    worker_state.num_slots = num_slots


# Must be a module-level function so ProcessPoolExecutor can pickle it by name
def _fetch_chunk(idx: int, start: int, end: int, chunk_size: int) -> int:
    """
    Download a byte range and write directly into the assigned ring buffer slot.

    Always signals the slot's ready event — even on error — so the consumer
    can unblock and observe the exception via `future.result()`.
    """
    slot = idx % worker_state.num_slots
    n = 0
    try:
        offset = slot * chunk_size
        resp = worker_state.client.get_chunk(start, end)
        try:
            # TODO: replace with resp.raw.readinto() for a further zero-copy win
            for chunk in resp.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                worker_state.shm.buf[offset + n : offset + n + len(chunk)] = chunk
                n += len(chunk)
        finally:
            resp.close()
    finally:
        worker_state.slot_ready[slot].set()  # must run even on exception
    return n


class RingBuffer:
    """
    Shared-memory ring buffer with per-slot ready Events.

    Allocates `num_slots x slot_size` bytes in `/dev/shm`.  Each slot has
    a dedicated `mp.Event` that a worker sets when its data is ready.
    """

    def __init__(self, num_slots: int, slot_size: int) -> None:
        self.num_slots = num_slots
        self.slot_size = slot_size
        self.shm = shared_memory.SharedMemory(create=True, size=num_slots * slot_size)
        self.slot_ready: List[mp.Event] = [mp.Event() for _ in range(num_slots)]

    @property
    def name(self) -> str:
        """Name of the underlying shared memory segment."""
        return self.shm.name

    def wait_slot(self, slot: int) -> None:
        """Block until the slot is ready, then reset it for reuse."""
        self.slot_ready[slot].wait()
        self.slot_ready[slot].clear()

    def read_slot(self, slot: int, data_len: int) -> bytes:
        """Copy `data_len` bytes out of the slot into a new bytes object."""
        offset = slot * self.slot_size
        return bytes(self.shm.buf[offset : offset + data_len])

    def close(self) -> None:
        """Release and unlink the shared memory segment."""
        self.shm.close()
        self.shm.unlink()


class ParallelContentIterProvider(BaseContentIterProvider):
    """
    Parallel content iterator backed by a shared-memory ring buffer.

    Splits the object into fixed-size byte ranges and fetches them in parallel
    using `ProcessPoolExecutor`.  Workers write directly into a bounded ring
    buffer of `num_workers` slots. Chunks are yielded to the caller in order.

    Args:
        client (ObjectClient): Client for accessing contents of an individual object.
        chunk_size (Optional[int]): Size of each chunk of data yielded. If None,
            will attempt to use optimal chunk size from HeadObjectV2 response.
        num_workers (int): Number of concurrent workers for fetching chunks.
    """

    def __init__(
        self,
        client: ObjectClient,
        chunk_size: Optional[int],
        num_workers: int,
    ):
        attrs = client.head_v2(PROPS_CHUNKED)
        self._object_size = attrs.size

        if chunk_size is None or chunk_size <= 0:
            if attrs.chunks and attrs.chunks.max_chunk_size > 0:
                chunk_size = attrs.chunks.max_chunk_size
            else:
                chunk_size = DEFAULT_PARALLEL_CHUNK_SIZE

        super().__init__(client, chunk_size)
        self._num_workers = num_workers

    @property
    def num_workers(self) -> int:
        """Get the number of concurrent workers."""
        return self._num_workers

    def _build_chunk_ranges(self, offset: int) -> List[Tuple[int, int]]:
        """Return a list of `(start, end)` byte ranges covering the object from `offset`."""
        ranges = []
        pos = offset
        while pos < self._object_size:
            end = min(pos + self._chunk_size, self._object_size)
            ranges.append((pos, end))
            pos = end
        return ranges

    # pylint: disable=too-many-locals
    def create_iter(self, offset: int = 0) -> Generator[bytes, None, None]:
        """
        Yield object content in order using a sliding-window ring buffer.

        Keeps at most `num_workers` range-reads in flight at any time.

        Args:
            offset (int, optional): Starting byte offset. Defaults to 0.

        Yields:
            bytes: Consecutive chunks of the object's content.
        """
        try:
            fork_context = mp.get_context("fork")
        except ValueError as exc:
            raise RuntimeError(
                "Parallel content iteration requires the 'fork' start method, "
                "which is not available on this platform."
            ) from exc

        if self._object_size - offset <= 0:
            return

        chunk_ranges = self._build_chunk_ranges(offset)
        num_slots = min(self._num_workers, len(chunk_ranges))

        futures: Dict[int, Future] = {}
        ring = None

        try:
            ring = RingBuffer(num_slots=num_slots, slot_size=self._chunk_size)

            with ProcessPoolExecutor(
                max_workers=num_slots,
                mp_context=fork_context,
                initializer=_init_worker,
                initargs=(self.client, ring.name, ring.slot_ready, num_slots),
            ) as executor:
                # Fill the ring with the first num_slots requests.
                for i in range(num_slots):
                    start, end = chunk_ranges[i]
                    futures[i] = executor.submit(
                        _fetch_chunk, i, start, end, self._chunk_size
                    )
                next_submit = num_slots

                # Sliding window: consume in order; on each consumed slot, submit the next chunk.
                for next_yield in range(len(chunk_ranges)):
                    slot = next_yield % num_slots
                    data_len = futures.pop(
                        next_yield
                    ).result()  # re-raises worker exceptions
                    ring.wait_slot(slot)
                    yield ring.read_slot(slot, data_len)

                    if next_submit < len(chunk_ranges):
                        start, end = chunk_ranges[next_submit]
                        futures[next_submit] = executor.submit(
                            _fetch_chunk, next_submit, start, end, self._chunk_size
                        )
                        next_submit += 1
        finally:
            if ring is not None:
                ring.close()
