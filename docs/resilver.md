## Resilver

First, etymology:

> "Resilvering" originally referred to restoring the reflective silver backing of a glass mirror.
> In modern storage systems the word "resilver" is commonly used to mean "rebuild data redundancy / restore intended layout after a device or topology change"
> (you’ll see it prominently in ZFS/OpenZFS documentation and tooling).

In AIStore, resilvering (or simply "resilver") is the mechanism for redistributing objects to their [correct locations](/docs/overview.md#data-protection) after volume changes within a storage target.

When [mountpaths](/docs/overview.md#mountpath) are attached, detached, enabled, or disabled, objects may no longer reside at their proper HRW locations.
Resilver walks all objects and relocates them as needed to restore data placement and redundancy under the current configuration.

> HRW: a variant of consistent hash based on rendezvous (highest‑random‑weight) algorithm by Thaler and Ravishankar.

## Resilver vs. Global Rebalance

AIStore uses the same conceptual model:

At the **cluster level**, rebalance distributes objects across targets using HRW.

At the (local) **node level**, resilver distributes objects across mountpaths using the same HRW algorithm.

This symmetry is intentional. The same reasoning applies at both levels:

* HRW provides deterministic placement.
* Only objects whose placement changes need to move.
* Work can be parallelized.
* Operations are preemptible.
* Misplaced objects remain accessible.

The difference is scope and mechanics: rebalance moves data over the network across targets, while resilver - locally across [mountpaths](/docs/overview.md#mountpath) on the same machine.

This document is structured as follows:

- [When Resilver Runs](#when-resilver-runs)
- [Object Placement](#object-placement)
- [Misplaced Objects](#misplaced-objects)
- [Mirrored Objects](#mirrored-objects)
- [Chunked Objects](#chunked-objects)
- [Monitoring and Progress](#monitoring-and-progress)
- [CLI Usage](#cli-usage)
- [Resilver vs Scrubbing](#resilver-vs-scrubbing)
- [References](#references)

---

Resilver is AIStore’s mechanism for restoring correct object placement and redundancy on a given (or any given) AIS target. The system guarantees that objects end up:

* on their correct mountpaths, and
* with the configured number of replicas (when mirroring is enabled), or
* chunks (when objects are chunked).

Resilver walks the target’s data and performs the minimum work needed to get back to a consistent state:

* **Main replica at the correct HRW mountpath** for the current volume topology.
* **Mirroring restored** to the configured replica count (as much as possible given the number of available mountpaths).
* **Chunked objects validated and repaired** so that all chunks are where the system would look for them under the current topology.

Resilver is a local process - it never moves data between targets and never requires cluster-wide coordination.

---

### When Resilver Runs

Resilver runs on demand (via `ais storage resilver`) and automatically in response to mountpath lifecycle events:

* attaching a new mountpath,
* detaching a mountpath,
* enabling a previously disabled mountpath,
* disabling a mountpath temporarily.

```console
$ ais show job resilver

resilver[MgyTbYxm7]
NODE             ID              KIND            OBJECTS         BYTES           START           END     STATE
VNgt8085         MgyTbYxm7       resilver        268             273.15MiB       20:15:34        -       Running
------------------------------------------------------------------------
resilver[GgZZGERh7]
NODE             ID              KIND            OBJECTS         BYTES           START           END     STATE
VNgt8085         GgZZGERh7       resilver        40              246.49MiB       20:15:35        -       Running
------------------------------------------------------------------------
resilver[GgZZGERh7]
NODE             ID              KIND            OBJECTS         BYTES           START           END     STATE
VNgt8085         GgZZGERh7       resilver        461             1.97GiB         20:15:35        -       Running
```

These events change the set of available mountpaths and therefore change HRW placement decisions. Resilver starts immediately to reconcile existing data with the new volume topology.

Operationally, disable/detach reduce the set of available mountpaths. Resilver’s job is to restore the target’s intended placement and redundancy using only the currently-available mountpaths.

Resilver is also **preemptible**. If a second mountpath event occurs while a resilver is running, the current run is aborted and a new one starts using the updated configuration. This ensures that work is never completed based on stale assumptions.

If a resilver is interrupted — by another mountpath event, a restart, or an abort — AIStore resumes the work later. Resilver is convergent by design: as long as mountpath configuration eventually stabilizes, object placement will converge to the correct state.

You can also trigger resilver manually using the CLI, for example after recovering from disk failures or interrupted maintenance.

---

### Object Placement

Resilver relies on HRW (Highest Random Weight) to determine where objects belong.

For a given object name and a given set of available mountpaths, HRW deterministically selects a single mountpath. Every component in the system computes the same answer independently; no shared state or coordination is required.

When mountpaths are added or removed, HRW placement changes only for a subset of objects. Resilver identifies those objects and relocates them. Objects whose HRW placement does not change are left untouched.

---

### Misplaced Objects

An object is considered *misplaced* if it does not reside on its HRW mountpath under the current configuration.

Misplacement is expected and benign. It occurs naturally when mountpaths change, or when resilvering is interrupted. Reads continue to work: AIStore can locate objects regardless of where they physically reside.

Resilver is the mechanism that restores optimal placement and eliminates long-term imbalance.

---

### Mirrored Objects

Buckets may be configured with N-way mirroring. In that case, each object consists of:

* one **main replica**, placed at the object’s HRW mountpath, and
* additional **copies**, placed on other mountpaths selected using the same HRW principles.

When a mountpath is disabled or detached, copies on that mountpath become unavailable. Resilver removes stale metadata entries for those copies and creates replacements on other mountpaths if possible.

If there are fewer available mountpaths than required copies, resilvering creates as many copies as it can and leaves the system in a degraded but consistent state. When more mountpaths become available later, resilvering completes the replication.

---

### Chunked Objects

Large objects may be stored as multiple chunks. All chunks are equal in structure and size. None of them is special.

What distinguishes `chunk #1` is only its location: it is stored where a non-chunked object of the same name would be stored — at the object’s HRW mountpath. This allows existing lookup logic to locate the object efficiently.

The object itself is defined by its **chunk manifest** - the metadata that describes all chunks and their placement. Each chunk is placed independently using HRW derived from the object name and chunk index.

During resilvering, chunks are verified independently. A chunked object is considered correct only if *all* chunks and the manifest are at their correct locations under the current mountpath configuration. If any part is misplaced, the object is repaired as a unit.

> See also: [Blob Downloader](/docs/blob_downloader.md)

---

### Monitoring and Progress

Resilver runs as a batch job (or [xaction](/docs/overview.md#xaction)).
Progress is visible through standard job monitoring (`ais show job`) command, e.g.:

```console
ais show job resilver
```

The primary progress metric counts **main replicas restored to their HRW locations**. This reflects actual repair work performed, not just objects visited.

Objects skipped due to locking contention are tracked separately. Skipped objects are not lost; they are handled in subsequent resilver runs if needed.

---

### CLI Usage

Resilver is usually triggered implicitly by mountpath operations, but it can also be started manually:

```console
# Resilver all targets
ais storage resilver

# Resilver a specific target
ais start resilver t[XYZ]

# Wait for completion
ais start resilver t[XYZ] --wait --timeout 30m
```

Mountpath lifecycle commands are the most common trigger:

```console
ais storage mountpath disable t[XYZ]=/mnt/disk1
ais storage mountpath enable  t[XYZ]=/mnt/disk1
ais storage mountpath detach  t[XYZ]=/mnt/disk1
ais storage mountpath attach  t[XYZ]=/mnt/newdisk
```

Additionally, there's (an advanced-usage capability) to manipulate mountpaths without triggering resilver - useful when batching multiple changes:

```
# Disable without resilvering
ais storage mountpath disable t[XYZ]=/mnt/disk1 --no-resilver
```

---

### Resilver vs Scrubbing

In ZFS terminology, *scrubbing* means deep validation: reading every block, verifying checksums, detecting silent corruption, and repairing bit rot. It's a thorough health check of data integrity at the physical level.

AIStore's *resilver* is intentionally much narrower in scope - it focuses exclusively on data placement and redundancy under the current mountpath volume, and it does not verify checksums or read object contents.

This makes resilver fast and topology-focused. It runs after mountpath changes (attach/detach/enable/disable) to restore correct layout, not to detect corruption.

AIS provides APIs and configuration to validate checksums during normal I/O operations (reads, writes, copies). Full end-to-end validation - the equivalent of ZFS scrub - would combine resilver's placement checks with explicit checksum verification of all objects. Such functionality could be added in the future.

For now, AIStore separates concerns clearly:

* Resilver handles the _positive_ task: restoring correct placement and redundancy.
* [Space cleanup](https://github.com/NVIDIA/aistore/blob/main/space/README.md) handles the _negative_ task: removing obsolete, orphaned, or no-longer-reachable data:

```console
$ ais space-cleanup --help
NAME:
   ais space-cleanup - (alias for "storage cleanup") Remove:
              - deleted objects and buckets;
              - old/obsolete workfiles;
              - misplaced objects (see command line option below);
              - orphan chunks and partial chunk manifests;
              - optionally, remove zero-size objects as well.

     By default, any stored content with invalid or unrecognized FQN is treated as obsolete and is removed.
     To preserve, use the cluster feature flag 'Keep-Unknown-FQN'.

USAGE:
   ais space-cleanup [BUCKET[/PREFIX]] [PROVIDER] [command options]

OPTIONS:
   force,f         Proceed with removing misplaced objects even if global rebalance (or local resilver) is running or was interrupted,
                   or the node has recently restarted. Does not override the 'dont_cleanup_time' window or other flags
   keep-misplaced  Do not remove misplaced objects (default: remove after 'dont_cleanup_time' grace period)
                   Tip: use 'ais config cluster log.modules space' to enable logging for dry-run visibility
   rm-zero-size    Remove zero size objects (caution: advanced usage only)
   timeout         Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                   valid time units: ns, us (or µs), ms, s (default), m, h
   wait            Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   help, h         Show help
```

---

### References

* [AIStore Overview](/docs/overview.md)
* [Buckets: Design and Operations](/docs/bucket.md)
* [Observability](/docs/monitoring-overview.md)
* [Technical Blog](https://aistore.nvidia.com/blog)
* [Batch Jobs](/docs/batch.md)
* [CLI](/docs/cli.md)
