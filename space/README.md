# Space Cleanup

This package implements **content cleanup** on each target node. The goal is to reclaim
storage space from **work files**, **erasure coding (EC) artifacts**, and **chunked upload leftovers**
while ensuring correctness and avoiding premature deletion.

Cleanup operates **per-bucket, per-mountpath** and works strictly on **local state** —
it does not coordinate across targets.

**Table of Contents**

1. [Overview](#1-overview)
2. [Cleanup Policies](#2-cleanup-policies)
3. [Implementation Details](#3-implementation-details)
4. [Corner Cases & Constraints](#4-corner-cases--constraints)
5. [Future Enhancements](#5-future-enhancements)

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

## 2. Cleanup Policies

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

## 3. Implementation Details

### Throttling

Space cleanup uses the unified `cmn/load` throttling (`load.Advice`) to avoid I/O and CPU spikes during large scans:

- Each mountpath  keeps a `load.Advice` instance initialized with `FlMem|FlCla|FlDsk` and `RW=false` (metadata-only).
- On every N-th visit (`adv.ShouldCheck(nvisits)`), it refreshes node pressure and may insert a small sleep.
- Under `Critical` memory, CPU, or disk pressure, cleanup backs off; under merely `High` load it keeps progressing but with gentler pacing.

### Time Dependencies
Relies on filesystem mtimes. Clock changes on the operator may influence cleanup decisions.

## 4. Corner Cases & Constraints

- **Race Protection**: Slice => Meta and Replica => Meta sequences covered by global recency guard
- **Local Scope**: Does not consult cluster maps; global orphan detection is out of scope
- **Encoding Requirements**: `fs.WorkCT` tags, chunk uploadIDs, and chunk numbers must never be empty
- **Legacy State**: Partial manifests treated as invalid and always removed

## 5. Future Enhancements

### Generation-Aware EC Cleanup
Delay removal when conflicting generations exist; prefer newest metadata.

### Cluster-Aware Reconciliation
Consult cluster-wide state to distinguish local vs. global orphans.

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
