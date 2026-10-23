# AIS PyTorch Agent Instructions

Follow the [Python instructions](../../AGENTS.md). Consult the
[README](README.md) only as needed for the change. Read the affected dataset,
sampler, and SDK implementations.

## Verification

Use existing tests in `tests/unit/pytorch/` and `tests/integration/pytorch/`.
For worker changes, exercise real `DataLoader` processes as well as mocks:
check zero and multiple workers, complete sample coverage without duplicates,
and repeated iteration. Keep multiprocessing fixtures picklable under spawn and
forkserver; define dataset subclasses at module scope.

## Dataset Contracts

- Preserve map-style indexing/length and iterable-style reset/worker partitioning
  behavior. Reuse `AISBaseMapDataset` and `AISBaseIterDataset` for shared logic.
- Preserve sample names, payload types, shard grouping, and collation behavior.
  Test empty inputs, incomplete batches, and missing shard components when relevant.
- Keep iteration lazy and memory bounded. Preserve stream cleanup, including when
  iteration stops early; `AISParallelMapDataset` transfers responsibility for
  consuming and closing its `ParallelBuffer` to the caller or collate function.
- For loading performance changes, compare throughput and memory against a baseline
  with the same data, worker counts, batch sizes, and cache state.
- Ensure compatibility with upstream PyTorch; this package extends that library.
