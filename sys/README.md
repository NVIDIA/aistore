# `sys` package

The `sys` package provides lightweight runtime visibility into host and container resources, including:

- CPU count and load estimation
- memory statistics
- container-aware reporting on Linux (cgroup v2 and v1)
- simple, dependency-free fallbacks when preferred sources are unavailable

**Table of Contents**

- [Initialization](#initialization)
- [CPU reporting](#cpu-reporting)
  - [Effective CPU count](#effective-cpu-count)
  - [Utilization model](#utilization-model)
  - [Linux source hierarchy](#linux-source-hierarchy)
  - [Bare-metal /proc/stat parsing](#bare-metal-procstat-parsing)
  - [CPU starvation vs utilization](#cpu-starvation-vs-utilization)
- [Memory reporting](#memory-reporting)
  - [Host memory](#host-memory)
  - [Container memory (cgroup v2)](#container-memory-cgroup-v2)
  - [Container memory (cgroup v1)](#container-memory-cgroup-v1)
- [Container detection](#container-detection)
- [Fallback](#fallback)
- [Example: testing `sys` package inside a constrained container](#example-testing-sys-package-inside-a-constrained-container)
- [Current limitations and future plans](#current-limitations-and-future-plans)
  - [cgroup v1 deprecation](#cgroup-v1-deprecation)
  - [Hardcoded thresholds](#hardcoded-thresholds)
  - [Future work](#future-work)

## Initialization

Package initialization is split into two phases:

- `init()`: sets `NumCPU` to `runtime.NumCPU()` - always safe, no dependencies.
- `Init(contForced bool)`: performs container auto-detection, applies cgroup-aware CPU count, adjusts `GOMAXPROCS`. External modules may skip calling `Init()` and still get a sane `NumCPU()`.

A package-level `cgroupVer` variable (0, 1, or 2) is set once during `Init()` and drives all subsequent CPU and memory reads. This avoids re-probing cgroup paths on every sample.

The `ForceContainerCPUMem` feature flag (in `cmn/feat`) can override failed auto-detection for deployments where the heuristics miss. Requires restart.

## CPU reporting

| Function | Description |
|---|---|
| `NumCPU()` | effective CPU count (container-aware) |
| `MaxParallelism()` | derives internal parallelism from `NumCPU()` |
| `MaxLoad2()` | CPU utilization percentage (0–100, integer) plus `isExtreme` boolean |
| `HighLoadWM()` | high-load watermark derived from CPU count |
| `LoadAverage()` | system load averages (fallback only) |

### Effective CPU count

At startup, the package initializes a process-wide CPU count:

- default: `runtime.NumCPU()`
- container override: cgroup-based CPU quota when detected

The cgroup version is determined once during package's `Init()` call:

1. Try cgroup v2 (`cpu.max`).
2. Fall back to cgroup v1 (`cpu.cfs_quota_us / cpu.cfs_period_us`).
3. If both fail or are missing, keep `runtime.NumCPU()`.

Errors from both paths are aggregated and reported to stderr (nlog is not yet available at init time).

### Utilization model

CPU utilization is sampled as a delta of cumulative CPU time over wall-clock time.
The previous raw sample is stored in the global `cpu` struct. Utilization is computed as:

`(delta_cpu_usage * 100) / (delta_wall_time * NumCPU)`

All values are integer percentages. Utilization and throttling are computed atomically in a single pass from the same time delta.

A minimum wall-clock interval (`minWallIval`) rejects millisecond-range resampling noise. Samples older than `maxSampleAge` are discarded and treated as a reset.

### Linux source hierarchy

The cgroup version is determined at init time and stored in `cgroupVer`. The `cpu.read()` method switches on it - there is no fallback cascade per sample:

| `cgroupVer` | Source | Fields |
|---|---|---|
| 2 | `cpu.stat` | `usage_usec`, `throttled_usec` |
| 1 | `cpuacct.usage` | cumulative nanoseconds |
| 0 | `/proc/stat` | aggregate jiffy line |

If all sources fail at read time, `MaxLoad2()` falls back to `/proc/loadavg` converted to a percentage.

### Bare-metal /proc/stat parsing

The aggregate `cpu` line from `/proc/stat` is parsed using a whitelist of fields:

- `user` (1), `nice` (2), `system` (3), `irq` (6), `softirq` (7), `steal` (8)

Explicitly excluded:

- `idle` (4), `iowait` (5): not active CPU time
- `guest` (9), `guest_nice` (10): already included in `user` and `nice` by the kernel - summing them would double-count

Steal is included because it represents CPU time unavailable to the node, which is what load-based throttling and worker tuning need to make decisions.

### CPU starvation vs utilization

`MaxLoad2()` distinguishes between:

- **high utilization**: CPU is busy
- **CPU starvation**: the container is being throttled

In cgroup v2 environments, `throttled_usec` from `cpu.stat` is tracked as a percentage of wall-clock time. If throttling exceeds the extreme threshold (>10%), the system is reported as under extreme CPU pressure - even if raw utilization appears moderate.

This is intentional: throttling indicates lack of CPU availability, not just high usage.

## Memory reporting

| Function | Description |
|---|---|
| `MemStat.Get()` | populates memory statistics |
| `MemStat.Str()` | formats a compact summary |

`Get()` switches on `cgroupVer` and calls the appropriate stateless reader:

| `cgroupVer` | Reader | Source |
|---|---|---|
| 2 | `readMemCgroupV2()` | `memory.max`, `memory.current`, `memory.stat` |
| 1 | `readMemCgroupV1()` | `memory.limit_in_bytes`, `memory.usage_in_bytes`, `memory.stat` |
| 0 | `readMemHost()` | `/proc/meminfo` |

All readers are stateless free functions returning `(MemStat, error)`.

### Host memory

Host memory is read from `/proc/meminfo`. Fields used:

- `MemTotal`, `MemFree`, `MemAvailable`, `Cached`, `Buffers`, `SwapTotal`, `SwapFree`

If `MemAvailable` is not present (older kernels), `ActualFree` falls back to `MemFree + BuffCache`.

### Container memory (cgroup v2)

1. `memory.max` - limit in bytes, or `"max"` (no limit → fall back to host)
2. `memory.current` - current usage including kernel caches
3. `memory.stat` - `inactive_file` used as reclaimable cache (`BuffCache`)

Derived: `ActualUsed = Used - BuffCache`, `ActualFree = Total - ActualUsed`.

Usage is capped at the limit to handle transient kernel overshoot before OOM.

### Container memory (cgroup v1)

1. `memory.limit_in_bytes` - values > `MaxInt64/2` treated as "no limit" (fall back to host)
2. `memory.usage_in_bytes` - current usage
3. `memory.stat` - `total_cache` used as reclaimable cache

Swap statistics are always host statistics regardless of cgroup version.

## Container detection

Container detection uses a best-effort heuristic at init time:

1. Check for `/.dockerenv`
2. Scan `/proc/1/cgroup` for markers: `docker`, `containerd`, `kubepods`, `kube`, `lxc`, `libpod`, `podman`

If auto-detection fails but the deployment is known to be containerized, set the `ForceContainerCPUMem` feature flag. This forces cgroup-based CPU and memory accounting. Requires restart.

## Fallback

The package follows these rules:

- preferred source first, determined once at init time
- degrade to older or coarser source when the preferred source is unavailable
- for CPU: preserve a usable percentage whenever possible
- for memory: prefer host stats over failing when container-specific files cannot be read

## Example: testing `sys` package inside a constrained container

A simple way to validate container-aware CPU and memory reporting is to compare the same test run on the host and inside a Docker container with explicit CPU and memory limits.

### Host run

```bash
go test -v -tags=debug
```

Example output:

```text
=== RUN   TestNumCPU
--- PASS: TestNumCPU (0.00s)
=== RUN   TestLoadAvg
    sys_test.go:41: Load average: 0.63, 0.49, 0.49
--- PASS: TestLoadAvg (0.00s)
=== RUN   TestMaxProcs
--- PASS: TestMaxProcs (0.00s)
=== RUN   TestMemoryStats
    sys_test.go:76: Memory stats: {used 29GiB, free 2GiB, buffcache 20GiB, actfree 23GiB}
    sys_test.go:79: Either swap is off or failed to read its stats
--- PASS: TestMemoryStats (0.00s)
=== RUN   TestProcAndMaxLoad
    sys_test.go:110: First call: load=0, extreme=false
    ...
    sys_test.go:133: Second call: load=3, extreme=false
    sys_test.go:145: Process CPU usage:   1.85%
--- PASS: TestProcAndMaxLoad (5.69s)
PASS
ok      github.com/NVIDIA/aistore/sys   5.696s
```

### Container run

```bash
docker run --rm \
  --cpus=1.5 \
  --memory=512m \
  -v "$PWD":/src -w /src \
  -v "$HOME/go/pkg/mod":/go/pkg/mod \
  -v "$HOME/.cache/go-build":/root/.cache/go-build \
  golang:1.25 \
  go test ./sys -run . -v -count=1 2>&1
```

Example output:

```text
=== RUN   TestNumCPU
--- PASS: TestNumCPU (0.00s)
=== RUN   TestLoadAvg
    sys_test.go:41: Load average: 0.36, 0.41, 0.47
--- PASS: TestLoadAvg (0.00s)
=== RUN   TestMaxProcs
--- PASS: TestMaxProcs (0.00s)
=== RUN   TestMemoryStats
    sys_test.go:76: Memory stats: {used 29MiB, free 483MiB, buffcache 152KiB, actfree 483MiB}
    sys_test.go:79: Either swap is off or failed to read its stats
--- PASS: TestMemoryStats (0.00s)
=== RUN   TestProcAndMaxLoad
    sys_test.go:110: First call: load=0, extreme=false
    ...
    sys_test.go:133: Second call: load=15, extreme=false
    sys_test.go:145: Process CPU usage:  14.82%
--- PASS: TestProcAndMaxLoad (5.70s)
```

The comparison illustrates several points:

* On the host, `TestMemoryStats` reports host-scale memory totals.
* Inside the container, the same test reports memory bounded by the cgroup limit (`--memory=512m`) rather than the host's physical RAM.
* `TestNumCPU` and `TestMaxProcs` exercise init-time CPU detection and container-aware CPU count.
* `TestProcAndMaxLoad` burns CPU in-process and verifies that `MaxLoad2()` reports non-zero utilization on a subsequent sample.
* Swap may report as zero or be unavailable inside short-lived containers; this is not unusual.

To further confirm container-scoped memory accounting, rerun the container example with a different limit (for example, `--memory=4G`). `TestMemoryStats` should then report a total close to 4 GiB instead of 512 MiB.

> The container example above assumes cgroup v2 - the default on modern Linux distributions and container runtimes.

## Current limitations and future plans

### cgroup v1 deprecation

Note that cgroup v1 support is deprecated and will be removed in a future release after v4.4. All major container runtimes and orchestrators now default to cgroup v2. Deployments still on v1 should plan to migrate.

### Hardcoded thresholds

Several operational constants are compiled in:

- stale CPU sample interval (`maxSampleAge`)
- minimum wall-clock interval (`minWallIval`) - temporary guard against resampling noise
- CPU load thresholds (`ExtremeLoad`, `HighLoad`)
- throttling threshold for extreme CPU starvation

### Future work

1. Replace `(maxSampleAge, minWallIval)` with alpha-weighted exponential averaging over a fixed sample array (similar to disk utilization in `ios` package).
2. Allow callers to pass mono-time into `MaxLoad2()` to avoid redundant clock reads.
3. Add cgroup-aware memory support for Darwin (currently stubs).
4. Consider PSI (Pressure Stall Information) integration via `cpu.pressure` / `/proc/pressure/cpu`.
5. Evaluate dropping cgroup v1 support.
