# Content Iterator

## Overview

`content_iterator` implements the data path behind `ObjectReader`.
Two strategies exist; the caller picks one via `Object.get_reader()`:

```python
Object.get_reader(num_workers=None)  →  ContentIterProvider      (sequential)
Object.get_reader(num_workers=N)     →  ParallelContentIterProvider  (parallel)
```

Both implement the same `BaseContentIterProvider` interface:

```python
read_all()    → Union[bytes, ParallelBuffer]
create_iter() → Generator[bytes]
```

`ObjectReader` delegates to whichever provider was selected.
Callers see the same `read_all()` / `__iter__()` API regardless.

---

## Sequential Path (`ContentIterProvider`)

One HTTP request. No shared memory.

```python
read_all()    → resp.content                    → bytes
create_iter() → resp.iter_content(chunk_size)   → Generator[bytes]
```

---

## Parallel Path (`ParallelContentIterProvider`)

### Initialization

```python
def __init__(self, client, chunk_size, num_workers):
    attrs = client.head_v2(...)        # get object size + server chunk size
    self._chunk_ranges = ...           # [(start, end), ...] per chunk
```

Rejects `object_size <= 0` immediately (SharedMemory cannot be zero-sized).

### Workers

All workers run in **forked child processes** (`mp.get_context("fork")`).
Fork avoids the GIL contention and achieves true parallelism for network I/O + memcpy.

Each worker:
1. Reconstructs a session from serialized init args.
2. Issues `GET` with `Range: bytes=start-end` header.
3. Writes response bytes into a shared-memory segment

### Mode 1: `read_all()` — Full-Size ParallelBuffer

```
/dev/shm  ┌─────────────────────────────────────────────────────────────┐
          │               ParallelBuffer (object_size)                  │
          └─────────────────────────────────────────────────────────────┘
           ^ worker 0       ^ worker 1       ^ worker 2
           writes            writes            writes
           [0:chunk]         [chunk:2*chunk]   [2*chunk:...]
```

1. Allocate one `SharedMemory(size=object_size)`.
2. Wrap it in a `ParallelBuffer`.
3. Submit all chunk ranges to `ProcessPoolExecutor`.
4. Each worker writes at its offset inside the shared segment.
5. Return the `ParallelBuffer` (memoryview into shm).

**Return type:** `ParallelBuffer` — caller gets a `memoryview`.
No third copy on the return path.

**Lifecycle:** caller **must** call `close()` or use `with`.

---

### Mode 2: `create_iter()` — Sliding-Window Ring Buffer

```
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
```

1. Allocate `RingBuffer(num_slots=num_workers, slot_size=chunk_size)`.
2. Pre-submit `num_workers` chunks.
3. Loop:
   - `future.result()` — wait for worker, re-raise on error
   - `ring.wait_slot(slot)` — block until Event is set
   - `yield ring.read_slot(slot, n)` — copy slot bytes out
   - Submit next chunk into the freed slot
4. `ring.close()`.

**Return type:** `Generator[bytes]` — standard streaming.

---

## Buffer Class Hierarchy

```
ParallelBuffer                 (wraps SharedMemory + memoryview)
    ├── used by read_all()     (one buffer = full object size)
    │
    └── RingBuffer             (adds slot indexing + mp.Event sync)
         └── used by create_iter()
```

`RingBuffer` inherits `close()`, `buf`, `name` from `ParallelBuffer`.
