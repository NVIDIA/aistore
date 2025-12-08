# Filesystem Health Checker (FSHC)

Robust detection and isolation of faulty local storage


**Table of Contents**

- [Overview](#overview)
- [1. Requirements](#1-requirements)
- [2. Configuration](#2-configuration)
- [3. When FSHC Runs (Triggering Conditions)](#3-when-fshc-runs-triggering-conditions)
- [4. Flow Diagram](#4-flow-diagram)
- [5. How FSHC Works](#5-how-fshc-works)
  - [5.1 Failure classification: FAULTED vs DEGRADED](#51-failure-classification-faulted-vs-degraded)
  - [5.2 Per-mountpath state](#52-per-mountpath-state)
  - [5.3 FSHC sequence](#53-fshc-sequence)
  - [5.4 Error Thresholds and Mountpath Disabling Logic](#54-error-thresholds-and-mountpath-disabling-logic)
  - [5.5 Re-enabling](#55-re-enabling)
- [6. Operational Guidance](#6-operational-guidance)
  - [6.1 Production-grade hardware](#61-production-grade-hardware-nvmessd-datacenter-nodes)
  - [6.2 Development, laptops, workstations](#62-development-laptops-workstations)
  - [6.3 Failure modes FSHC catches](#63-failure-modes-fshc-catches)
  - [6.4 Troubleshooting](#64-troubleshooting)
- [7. Interaction with Other AIS Subsystems](#7-interaction-with-other-ais-subsystems)
- [8. CLI](#8-cli)

---

## Overview

The Filesystem Health Checker (FSHC) is AIStore's subsystem responsible for detecting failures of **local mountpaths** (local filesystems used by targets to store objects).
When the data path observes an I/O-class error that suggests potential filesystem damage or disappearance, FSHC runs a sequence of validation tests. If the tests confirm the issue, the corresponding mountpath is **disabled** and removed from all future operations until manually re-enabled.

FSHC is designed to:

* detect hardware or filesystem failures early,
* avoid data loss caused by further writes to an unstable filesystem,
* ensure the target remains operational (albeit with fewer mountpaths),
* localize failures to a single mountpath without disrupting the entire node.

FSHC does **not** attempt automatic recovery of disabled mountpaths. Once a mountpath is disabled, the operator (or orchestration) inspects, repairs, and re-enables it manually.

Correctly tuned, FSHC is one of AIS's important safety mechanisms, ensuring that cluster nodes remain healthy and consistent even under hardware failures.

---

# 1. Requirements

FSHC operates on **local filesystems** mounted on the target node. The mountpath must:

* support basic POSIX operations (`stat`, `open`, `read`, `write`, `fsync`, `rename`, `unlink`),
* allow **O_DIRECT** reads (used during integrity checks),
* be writable (unless intentionally mounted read-only),
* be correctly mounted within the AIS filesystem tree.

Filesystems backed by **network storage** (NFS, SMB, FUSE-based cloud mounts, etc.) are supported, but I/O errors may reflect remote or network issues rather than local disk failures. In such setups, consider relaxing FSHC thresholds or disabling FSHC to avoid false positives.

---

# 2. Configuration

FSHC behavior is controlled via the `fshc` section of cluster configuration:

```json
{
  "fshc": {
    "test_files": 4,
    "error_limit": 2,
    "io_err_limit": 10,
    "io_err_time": "10s",
    "enabled": true
  }
}
```

### 2.1 `enabled`

Boolean. Enables or disables FSHC cluster-wide. Disabling is **not recommended** except in tightly controlled development setups.

### 2.2 `test_files`

Default: 4. Number of files to sample per FSHC run: up to `test_files` files are read (random sampling), and up to `test_files` temporary files are written and fsync'ed.

Increasing this improves confidence at the cost of more I/O during checks.

### 2.3 `error_limit` (`HardErrs`)

Default: 2. Maximum number of combined read/write I/O errors allowed *during* a single FSHC run. Exceeding this threshold disables the mountpath.

> **Note**: Despite the generic name, `error_limit` specifically controls the FSHC run-time behavior (how many sampling errors are tolerated before disabling). It does *not* control what triggers FSHC in the first place - that's governed by `io_err_limit` and `io_err_time` below.

### 2.4 `io_err_limit` + `io_err_time`

These parameters control **how often FSHC is triggered**, not how it behaves once running.

* `io_err_limit`: maximum number of soft I/O errors allowed during `io_err_time`.
* `io_err_time`: sliding time window (e.g., `"10s"`).

These limits prevent a slow trickle of errors from overwhelming a target.

---

# 3. When FSHC Runs (Triggering Conditions)

FSHC is triggered primarily by **I/O-class** errors and mountpath/filesystem identity mismatches. In a few internal paths, it may also be invoked for other unexpected local errors that suggest a possible filesystem issue.

### 3.1 I/O system errors

Detected via `os.SyscallError` wrappers and critical errno values. On Linux, these include:

```
EIO, ENODEV, EUCLEAN, EROFS, ENOTDIR, ENXIO,
EBADF, ESTALE, ENOSPC, EDQUOT, ECANCELED
```

> **Note**: These errno values are Linux-specific. Other platforms may have different error codes.

### 3.2 Filesystem identity mismatches

For example: FSID changed unexpectedly, device disappeared or remounted differently.

### 3.3 Local metadata errors

Errors encountered during XAttr operations, local LOM operations, marker moves, rename operations, commit/flush failures.

### 3.4 Explicit internal triggers

From capacity checks, mpather directory walkers, internal consistency verifications.

### 3.5 Stats-layer soft I/O error rate

If soft I/O errors exceed the configured `io_err_limit` within `io_err_time`, FSHC is invoked.

### Not triggered on

* ENOENT / file-not-found
* Benign EC conditions
* S3 errors
* Backend errors (`ErrBackendTimeout`, `ErrHTTP429`, etc.)
* Rename "move-in-progress" errors (`ErrMv`)
* Synthetic errors used for control flow

---

# 4. Flow Diagram

```
                ┌─────────────────────────────────────────┐
                │           I/O error                     │
                └─────────────────────────────────────────┘
                                 │
                                 ▼
        ┌────────────────────────────────────────────────────────┐
        │  target.FSHC(err, mountpath, fqn)                      │
        └────────────────────────────────────────────────────────┘
                                 │
                                 ▼
        ┌────────────────────────────────────────────────────────┐
        │ Per-mountpath scheduler:                               │
        │ - serialize runs                                       │
        │ - enforce minimum interval between checks              │
        │ - start goroutine                                      │
        └────────────────────────────────────────────────────────┘
                                 │
                                 ▼
        ┌────────────────────────────────────────────────────────┐
        │ Health check sequence:                                 │
        │ 1. stat() on mount root         ─┐                     │
        │ 2. CheckFS() identity check      ├─► FAULTED: disable  │
        │ 3. open() on mount root         ─┘      immediately    │
        │ 4. read sample files            ─┐                     │
        │ 5. write & fsync temp files      ├─► DEGRADED: disable │
        │                                 ─┘      if errors      │
        │                                         exceed limit   │
        └────────────────────────────────────────────────────────┘
```

---

# 5. How FSHC Works

## 5.1 Failure classification: FAULTED vs DEGRADED

FSHC classifies failures into two categories:

**FAULTED** - Critical failures at the mountpath root level that indicate the filesystem is inaccessible or compromised. These immediately trigger disable without further testing:

* `stat()` on mountpath root fails (after retry)
* Filesystem identity check fails (FSID/device mismatch)
* Cannot open the mountpath root directory

**DEGRADED** - I/O errors during file sampling that exceed the configured threshold. The mountpath is still partially functional but unreliable:

* Read errors on sampled files exceed `error_limit`
* Write errors on test files exceed `error_limit`
* Combined read + write errors exceed `error_limit`

## 5.2 Per-mountpath state

An internal `[mpath => ror]` structure tracks:

* `last`: time when last check finished,
* `running`: non-zero if a test is currently running.

FSHC ensures:

* only one concurrent run per mountpath,
* at least 4 minutes (`minTimeBetweenRuns`) between runs by default.

This prevents cascading failures from snowballing into constant disk hammering.

## 5.3 FSHC sequence

> FSHC always retries once before declaring a mountpath FAULTED. It performs two passes (see next section) when checking a mountpath for I/O errors. In addition, it applies a single delayed retry to eliminate spurious errors from network-attached storage. This one-shot retry is used for:
>
> * root `stat` on the mountpath,
> * opening the mountpath root,
> * depth-0 directory operations in the sampler.

### Step 1: fstat on mountpath root

If `stat()` fails, FSHC waits one second and retries. If it fails again, the mountpath is immediately disabled as **FAULTED**.

### Step 2: Filesystem identity check

`Mountpath.CheckFS()` compares the recorded FSID/device ID against current values.

Mismatches imply: device lost, device replaced, filesystem corrupted, or mount remapped. These are **FAULTED** events.

### Step 3: Open root directory

FSHC attempts to open the mountpath root as a file. Failure indicates the filesystem is inaccessible (**FAULTED**).

### Step 4: Random file read tests

FSHC:

* attempts to re-read the problematic FQN (if provided),
* samples random files using recursive directory traversal,
* reads files via **O_DIRECT** to avoid page cache masking,
* stops early if I/O errors reach the limit.

### Step 5: Write tests

FSHC:

* creates a private temp directory under the mountpath,
* writes random files up to `test_files` count,
* uses `FloodWriter` to write real data,
* calls `fsync`,
* removes the temporary data,
* stops early if I/O errors reach the limit.

Write errors often show the earliest signs of underlying corruption.

## 5.4 Error Thresholds and Mountpath Disabling Logic

FSHC runs exactly two passes:

* **Pass 1**: `test_files` samples
* **Pass 2**: increases test count and may raise `maxerrs` slightly

If `readErrors + writeErrors >= error_limit` after both passes, FSHC disables the mountpath as **DEGRADED**.

If errors exist but remain below the limit, FSHC logs a warning but leaves the mountpath enabled.

Upon disable:

* target calls `DisableMpath`,
* mountpath is removed from available mpools,
* `FlagDisabledByFSHC` is set on the mountpath,
* node alert `DiskFault` is raised,
* future FSHC tests skip this mountpath.

The disable reason (FAULTED or DEGRADED) is logged for operator reference.

Disable is **immediate and permanent** until operator intervention.

## 5.5 Re-enabling

Auto-recovery is not implemented.

Re-enable is strictly manual:

```console
ais storage mountpath enable <TARGET>=<MOUNTPATH>
```

This triggers:

* re-check of filesystem identity,
* capacity recalculation,
* clearing of `FlagDisabledByFSHC`,
* making mountpath available again.

---

# 6. Operational Guidance

Because drive quality and workload environments differ, we define two recommended profiles.

## 6.1 Production-grade hardware (NVMe/SSD, datacenter nodes)

Recommended defaults:

* `"test_files": 8` or `16`
* `"error_limit": 2`
* `"io_err_limit": 5`
* `"io_err_time": "10s"`

Rationale: these systems should not produce sporadic I/O errors; strict detection avoids silent data corruption; impact of FSHC overhead is negligible on NVMe.

## 6.2 Development, laptops, workstations

Recommended:

* `"test_files": 3` or `4`
* `"error_limit": 2` (default)
* `"io_err_limit": 20` or `30`
* `"io_err_time": "30s"`

Cheap consumer SSDs/HDDs often emit transient errors during heavy load. FSHC should stay enabled, but thresholds should be tolerant to reduce false positives.

## 6.3 Failure modes FSHC catches

FSHC is specifically designed to catch:

* disappearing devices (`ENODEV`, `EUCLEAN`),
* mountpoints silently remounted read-only,
* filesystem corruption (journal issues),
* dying SSDs that intermittently drop I/O,
* stale NFS-like behavior (which should not be used with AIS),
* RAID/HBA flakiness.

In all these cases, disabling is the safest course of action.

## 6.4 Troubleshooting

### My mountpath was disabled — what now?

1. Check target logs around the time of disable. Look for FAULTED vs DEGRADED to understand severity.
2. Manually probe the device:

   ```console
   dd if=<file> of=/dev/null bs=1M
   fsck <device>                     # for ext4, btrfs, etc.
   xfs_repair <device_or_mountpoint> # for xfs
   dmesg | grep -i error
   ```
3. Unmount / remount if needed.
4. Once fixed:

```console
ais storage mountpath enable <TARGET>=<MOUNTPATH>
```

### Why didn't FSHC attempt to re-enable?

AIS avoids false positives. Only an operator can safely confirm repairs.

---

# 7. Interaction with Other AIS Subsystems

### Capacity subsystem

FSHC does *not* disable mountpaths on OOS (out-of-space). OOS handling has its own logic.

### mpather / walkers

Failures during directory walks trigger FSHC.

### LOM / local metadata

XAttr or rename failures trigger FSHC.

### EC and resilvering

EC does not affect disable decisions. Disable prevents these subsystems from using unstable mountpaths.

---

# 8. CLI

FSHC functionality is accessed through the **mountpath** subcommands under `ais storage`.

### 8.1 Show mountpaths

```console
ais storage mountpath show
ais storage mountpath show <TARGET_ID>
```

### 8.2 Disable a mountpath

Disables the mountpath but keeps it in the node's volume configuration so that it **can** be re-enabled later.

```console
ais storage mountpath disable <TARGET_ID>=<MOUNTPATH>
```

### 8.3 Re-enable a mountpath

After operator intervention (filesystem fix, remount, etc.).

```console
ais storage mountpath enable <TARGET>=<MOUNTPATH>
```

### 8.4 Detach a mountpath

Completely removes the path from the node's volume. This is not an FSHC action, but operators often confuse the two:

```console
ais storage mountpath detach <TARGET_ID>=<MOUNTPATH>
```

### 8.5 Attach a mountpath

Attach a new filesystem to a target node:

```console
ais storage mountpath attach <TARGET_ID>=<MOUNTPATH>
```

### 8.6 Run FSHC manually

Runs the filesystem health checker **on a specific mountpath**, on demand:

```console
ais storage mountpath fshc <TARGET_ID>=<MOUNTPATH>
```

Useful for: validating a mountpath before enabling it, testing a repaired filesystem, confirming a suspected failure.

### 8.7 Rescan underlying disks

Advanced operation (used for debugging or after replacing disks under the same mountpoint):

```console
ais storage mountpath rescan-disks <TARGET_ID>=<MOUNTPATH>
```

### 8.8 Monitoring view

To see disabled or detached mountpaths:

```console
ais storage mountpath
```

With additional views:

* `ais show storage mountpath --help`
* `ais search mountpath` (for discovering command variants)
* `ais search --regex "mountpath|disk"` (a superset of the above including "disk" matches)
