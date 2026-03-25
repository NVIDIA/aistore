#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

import multiprocessing as mp
from multiprocessing import shared_memory
from typing import List


class ParallelBuffer:
    """
    Result of a parallel object download, backed by shared memory.

    Parallel download splits a single object into byte ranges and fetches
    them concurrently using multiple workers. Workers write directly into a
    `SharedMemory` segment; the caller receives a memoryview with no copy.

    The caller **must** call `close()` (or use as a context manager) when
    done.
    """

    def __init__(self, shm: shared_memory.SharedMemory, size: int) -> None:
        self._shm = shm
        self._size = size
        # Slice to exact object size — SharedMemory may add alignment padding.
        self._buf: memoryview = shm.buf[:size]

    @property
    def buf(self) -> memoryview:
        """Memoryview of the downloaded content (no extra copy on return)."""
        return self._buf

    @property
    def name(self) -> str:
        """Name of the underlying shared memory segment."""
        return self._shm.name

    def __len__(self) -> int:
        return self._size

    def tobytes(self) -> bytes:
        """Copy content into a new `bytes` object (extra copy)."""
        return bytes(self._buf)

    def close(self) -> None:
        """Release and unlink the underlying shared memory segment.

        Safe to call multiple times; the second call is a no-op.
        """
        shm = self._shm
        if shm is None:
            return
        self._shm = None
        self._buf.release()
        shm.close()
        shm.unlink()

    def __enter__(self) -> "ParallelBuffer":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class RingBuffer(ParallelBuffer):
    """
    Shared-memory ring buffer with per-slot ready Events.

    Extends ParallelBuffer: inherits shm allocation, `buf`, `name`, and `close()`.
    Adds slot indexing and Event-based synchronization for the streaming
    (`create_iter`) path.

    Allocates `num_slots x slot_size` bytes in `/dev/shm`.  Each slot has
    a dedicated `mp.Event` that a worker sets when its data is ready.
    """

    def __init__(self, num_slots: int, slot_size: int) -> None:
        shm = shared_memory.SharedMemory(create=True, size=num_slots * slot_size)
        super().__init__(shm, num_slots * slot_size)
        self.num_slots = num_slots
        self.slot_size = slot_size
        self.slot_ready: List[mp.Event] = [mp.Event() for _ in range(num_slots)]

    def wait_slot(self, slot: int) -> None:
        """Block until the slot is ready, then reset it for reuse."""
        self.slot_ready[slot].wait()
        self.slot_ready[slot].clear()

    def read_slot(self, slot: int, data_len: int) -> bytes:
        """Copy `data_len` bytes out of the slot into a new bytes object."""
        offset = slot * self.slot_size
        return bytes(self._buf[offset : offset + data_len])
