#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

"""
Parallel object download using forked workers and shared memory.

Provides two download modes:

* `read_all()`: workers write directly into a single full-size
  `ParallelBuffer` (shared memory).  The caller receives a memoryview
  with no additional copy on the return path.

* `create_iter()`: workers write into a fixed-size ring buffer in
  shared memory; the main process yields chunks in order.  See the
  `create_iter` docstring for the detailed data flow.
"""

import multiprocessing as mp
from dataclasses import dataclass, field
from multiprocessing import shared_memory
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Dict, Generator, List, Optional, Tuple

from aistore.sdk.obj.content_iterator.base import BaseContentIterProvider
from aistore.sdk.obj.content_iterator.buffer import ParallelBuffer, RingBuffer
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


# Module-level singleton inherited by each forked worker via fork's memory copy.
worker_state = WorkerState()


def _init_worker(
    client: ObjectClient,
    shm_name: str,
    slot_ready: Optional[List[mp.Event]] = None,
) -> None:
    """
    ProcessPoolExecutor initializer hook — runs once per worker after forking.

    Populates `worker_state` before any `_fetch_chunk` task is dispatched so the
    worker is fully initialized before it does any work.

    Args:
        client (ObjectClient): Client for the worker to use to download chunks.
        shm_name (str): Name of the shared memory segment to attach to.
        slot_ready (Optional[List[mp.Event]]): Per-slot events for ring-buffer
            mode. Omit for direct-to-destination callers.
    """
    worker_state.client = client
    # Main process owns the segment and unlinks it via close();
    worker_state.shm = shared_memory.SharedMemory(name=shm_name)
    worker_state.slot_ready = slot_ready if slot_ready is not None else []


# Must be a module-level function so ProcessPoolExecutor can pickle it by name
def _fetch_chunk(
    start: int,
    end: int,
    offset: int,
    slot_idx: int = -1,
) -> int:
    """
    Download byte range `[start, end)` and write into `worker_state.shm` at `offset`.

    Args:
        start (int): First byte of the range to fetch from the object.
        end (int): One past the last byte (exclusive).
        offset (int): Byte offset in the shm segment to write into. Ring-buffer
            callers pass `slot * chunk_size`; direct callers pass `start`.
        slot_idx (int): If >= 0, signals `slot_ready[slot_idx]` on completion
            (ring-buffer mode). Pass -1 to skip (direct mode).
    """
    n = 0
    try:
        resp = worker_state.client.get_chunk(start, end)
        try:
            # TODO: replace with resp.raw.readinto() for a further zero-copy win
            for chunk in resp.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                worker_state.shm.buf[offset + n : offset + n + len(chunk)] = chunk
                n += len(chunk)
        finally:
            resp.close()
    finally:
        if slot_idx >= 0:
            worker_state.slot_ready[slot_idx].set()  # must run even on exception
    return n


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

        if self._object_size <= 0:
            raise ValueError(
                "Parallel download requires a non-empty object "
                f"(got size={self._object_size})."
            )

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

    @staticmethod
    def _get_fork_context():
        """Return the 'fork' multiprocessing context, or raise RuntimeError on unsupported platforms."""
        try:
            return mp.get_context("fork")
        except ValueError as exc:
            raise RuntimeError(
                "Parallel content iteration requires the 'fork' start method, "
                "which is not available on this platform."
            ) from exc

    def read_all(self) -> ParallelBuffer:
        """
        Download the entire object into shared memory and return a :class:`ParallelBuffer`.

        Workers write directly into a `SharedMemory` segment; the caller
        receives a memoryview of that segment with no extra allocation or copy.
        The caller **must** call `result.close()` (or use it as a context
        manager) to release the shared memory when done.
        """
        dst = ParallelBuffer(
            shared_memory.SharedMemory(create=True, size=self._object_size),
            self._object_size,
        )
        try:
            self._fill_shm(dst)
        except Exception:
            dst.close()
            raise
        return dst

    def _fill_shm(self, dst: "ParallelBuffer") -> None:
        """
        Download the entire object into *dst* (direct-to-destination mode).

        Each worker owns a disjoint byte range within *dst* — no
        synchronization beyond waiting for all futures.

        Args:
            dst (ParallelBuffer): A `ParallelBuffer` of at least `_object_size` bytes,
                typically created and owned by `read_all()`.
        """
        fork_context = self._get_fork_context()
        chunk_ranges = self._build_chunk_ranges(0)
        if not chunk_ranges:
            return
        num_workers = min(self._num_workers, len(chunk_ranges))

        with ProcessPoolExecutor(
            max_workers=num_workers,
            mp_context=fork_context,
            initializer=_init_worker,
            initargs=(self.client, dst.name),  # no slot_ready: direct mode
        ) as executor:
            # write_offset = start (each worker owns its exact byte range in dst)
            # slot_idx = -1 (no ring-buffer events needed)
            futures = [
                executor.submit(_fetch_chunk, start, end, start, -1)
                for start, end in chunk_ranges
            ]
            for f in futures:
                f.result()  # re-raises worker exceptions

    # pylint: disable=too-many-locals
    def create_iter(self, offset: int = 0) -> Generator[bytes, None, None]:
        """
        Yield object content in order using a sliding-window ring buffer.

        Keeps at most `num_workers` range-reads in flight at any time.
        The ring buffer lives in `/dev/shm`::

         /dev/shm  ┌──────────┬──────────┬──────────┐
                   │ [slot 0] │ [slot 1] │ [slot 2] │  num_slots x slot_size bytes
                   └──────────┴──────────┴──────────┘
         chunks:   [ chunk 0 ][ chunk 1 ][ chunk 2 ][ chunk 3 ]..
                   slot = chunk_idx % num_slots

         Main Process                       Forked Workers
         ─────────────                      ──────────────────────────────
                                             [slot 0]      [slot 1]      [slot 2]
         submit chunk 0 ───────────────> write slot 0
         submit chunk 1 ─────────────────────────────> write slot 1
         submit chunk 2 ───────────────────────────────────────────> write slot 2
         future[0].result()  ← done
         wait_slot(0)        ← event set
         yield read_slot(0)
         submit chunk 3 ───────────────> write slot 0  (reuse)
         ...

        Args:
            offset (int, optional): Starting byte offset. Defaults to 0.

        Yields:
            bytes: Consecutive chunks of the object's content.
        """
        self._expected_end_position = self._object_size
        fork_context = self._get_fork_context()

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
                initargs=(self.client, ring.name, ring.slot_ready),
            ) as executor:
                # Fill the ring with the first num_slots requests.
                for i in range(num_slots):
                    start, end = chunk_ranges[i]
                    # i < num_slots, so slot == i; write_offset is slot-relative
                    futures[i] = executor.submit(
                        _fetch_chunk, start, end, i * self._chunk_size, i
                    )
                next_submit = num_slots

                # Sliding window: consume in order; on each consumed slot, submit the next chunk.
                for next_yield in range(len(chunk_ranges)):
                    slot = next_yield % num_slots
                    data_len = futures.pop(
                        next_yield
                    ).result()  # re-raises worker exceptions
                    # TODO: Validate data_len against this range's expected length
                    # before yielding. A clean short range read must be retried or
                    # raised here; file-level EOF recovery cannot safely infer the
                    # missing absolute offset after later parallel ranges are yielded.
                    ring.wait_slot(slot)
                    yield ring.read_slot(slot, data_len)

                    if next_submit < len(chunk_ranges):
                        start, end = chunk_ranges[next_submit]
                        next_slot = next_submit % num_slots
                        futures[next_submit] = executor.submit(
                            _fetch_chunk,
                            start,
                            end,
                            next_slot * self._chunk_size,
                            next_slot,
                        )
                        next_submit += 1
        finally:
            if ring is not None:
                ring.close()
