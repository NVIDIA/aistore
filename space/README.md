# Space Cleanup

This package implements **content cleanup** on each target node. The goal is to reclaim
storage space from **work files**, **erasure coding (EC) artifacts**, and **chunked upload leftovers**
while ensuring correctness and avoiding premature deletion.

Cleanup walks local content **per-bucket, per-mountpath**. For objects owned by
another target under the bucket's current placement policy, it consults the cluster
map and verifies the copy with that owner before removing it. Checks of EC artifacts
and chunk leftovers still use local state.

**Table of Contents**

1. [Overview](#1-overview)
2. [Relation to rebalance cleanup mode](#2-relation-to-rebalance-cleanup-mode)
3. [Cleanup Policies](#3-cleanup-policies)
   - [Misplaced objects and peer verification](#misplaced-objects-and-peer-verification)
   - [Long object names](#long-object-names)
4. [Implementation Details](#4-implementation-details)
5. [Corner Cases & Constraints](#5-corner-cases--constraints)
6. [Future Enhancements](#6-future-enhancements)

## 1. Overview

* Scope: for each specified bucket or _all_ buckets:
  - scan all content items under a mountpath's bucket namespace

* Main method:
  - `clnJ.visit()` in turn calling `visitCT` or `visitObj`

* Global recency guard:
  - given configurable (cluster-level) knob (`dont_cleanup_time`) prevents premature deletion during ongoing operations

### Global Recency Guard

Any file with `mtime + dont_cleanup_time > now` is skipped to avoid racing against:

- EC slice => metafile write sequences
- Replica => metafile write sequences
- Other concurrent operations

Invalid entries (malformed FQNs, bucket mismatches) are logged and removed.

## 2. Relation to rebalance cleanup mode

`ais space-cleanup` is a general local-storage cleanup tool. It walks local
mountpaths and removes several classes of safely reclaimable files, including
objects with corrupted or missing local metadata, zero-size objects when
configured, extra local copies, misplaced EC artifacts, local mountpath orphans,
and verified migrated-away leftovers.

AIStore also provides:

```console
$ ais start rebalance --cleanup
```

Rebalance cleanup is narrower and more explicit. It reuses the global rebalance lifecycle and monitoring machinery, does not migrate object payloads, and is
intended specifically for reclaiming source-side copies left after topology changes and regular data-moving rebalance.

In short:

* use `ais start rebalance --cleanup` when the goal is post-rebalance,
  placement-specific cleanup after maintenance, decommission, scale-out, scale-in, or node return (from maintenance);
* use `ais space-cleanup` for broader local-storage hygiene and capacity reclamation.

## 3. Cleanup Policies

### Work Files (`fs.WorkCT`)

- Parsed via `ParseUbase`
- **Invalid encoding** => removed
- **PID mismatch** (from old process) => removed as *old work*

### Erasure Coding (`fs.ECSliceCT`, `fs.ECMetaCT`)

Behavior depends on whether EC is enabled for the bucket:

#### EC Disabled
- All EC slices and metafiles removed as *old work*

#### EC Enabled
- **Slices** (`fs.ECSliceCT`)
  - Missing corresponding metafile → flagged as *misplaced EC*
  - Removal batched under `flagRmMisplacedEC`

- **Metafiles** (`fs.ECMetaCT`)
  - Kept if local slice **OR** replica (`ObjCT`) exists
  - Removed only when **both** slice and replica are missing locally
  - Removal batched as *old work*

> **Note**: All decisions use local perspective only. A metafile orphaned locally may still have valid slices/replicas on other targets.

### Chunked Uploads (`fs.ChunkCT`, `fs.ChunkMetaCT`)

- **Chunks** (`fs.ChunkCT`)
  - Must encode valid `(uploadID, chunkNum)` pair
  - Invalid encodings → removed
  - Valid chunks validated against manifest state in `visitChunk`

- **Manifests** (`fs.ChunkMetaCT`)
  - *Completed* manifests (no extras) kept
  - *Partial* manifests (extras include uploadID) - removed as *old partials*

### Objects (`fs.ObjCT`)

- Handled in `visitObj()`
- For EC-enabled buckets: objects missing corresponding metafiles flagged as *misplaced EC*

### Misplaced objects and peer verification

Starting with v5.2, misplaced copies are verified using head-batch: a request compares
up to 64 objects with their owner by default. Unsupported peers fall back to individual
HEAD requests. A copy is eligible for removal only when the owner confirms the same
object; missing, divergent, busy, and unanswered copies are kept. `--keep-misplaced`
keeps the copies even after successful verification. Other cleanup flags and safety
windows still apply.

Peer checks do not hold the local object lock. Before deletion, cleanup takes the write
lock, reloads the metadata, and checks that it is unchanged since the peer comparison.
Busy or changed objects stay. Local removals are grouped using `space.batch_size`,
independently of the head-batch request size.

For load handling, compatibility fallback, and the `hdb-*` job counters, see
[Batched peer verification](/docs/rebalance.md#batched-peer-verification) and
[Monitoring](/docs/rebalance.md#monitoring). Use `ais show job cleanup-store --all`
to inspect the counters.

### Long object names

Space cleanup currently skips loaded objects that use shortened local filenames,
and ambiguous shortened chunk or manifest names. This can retain reclaimable data.
Objects with literal names resembling shortened filenames are classified by metadata,
rather than skipped solely by name. See [Long object names](/docs/long_names.md#storage-and-maintenance).

## 4. Implementation Details

### Throttling

Space cleanup uses the unified `cmn/load` throttling (`load.Advice`) to avoid I/O and CPU spikes during large scans:

- Each mountpath  keeps a `load.Advice` instance initialized with `FlMem|FlCla|FlDsk` and `RW=false` (metadata-only).
- On every N-th visit (`adv.ShouldCheck(nvisits)`), it refreshes node pressure and may insert a small sleep.
- Under `Critical` memory, CPU, or disk pressure, cleanup backs off; under merely `High` load it keeps progressing but with gentler pacing.

### Time Dependencies
Relies on filesystem mtimes. Clock changes on the operator may influence cleanup decisions.

## 5. Corner Cases & Constraints

- **Race Protection**: Slice => Meta and Replica => Meta sequences covered by global recency guard
- **Scope**: Misplaced object removal uses cluster placement and peer verification; EC and chunk-artifact checks remain local
- **Encoding Requirements**: `fs.WorkCT` tags, chunk uploadIDs, and chunk numbers must never be empty
- **Legacy State**: Partial manifests treated as invalid and always removed

## 6. Future Enhancements

### Generation-Aware EC Cleanup
Delay removal when conflicting generations exist; prefer newest metadata.

### Cluster-Aware Reconciliation
Extend cluster-wide reconciliation to EC and chunk artifacts; misplaced object copies
already use peer verification.

### Quarantine Mode
Move questionable artifacts to quarantine directory instead of immediate deletion.

### Enhanced Telemetry
Add Prometheus counters for:
- Misplaced EC artifacts
- Old work removal
- Invalid FQN detection
- Cleanup performance metrics

### Dry-Run & Reporting
Non-destructive cleanup pass that reports what *would* be removed:

- Categorized reasons (old work, misplaced EC, invalid FQN)
- Output formats: logs, xaction stats, JSON/CSV export
- Integration with monitoring dashboards

### Deep Scrubbing Mode
Extend beyond filename heuristics by loading and validating metadata:
- EC metafile → slice/replica consistency
- Chunk manifest → chunk file validation
- Cross-reference integrity checks
- Detailed mismatch reporting
