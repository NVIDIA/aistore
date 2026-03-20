# `sys` package

The `sys` package provides lightweight runtime visibility into host and container resources, including:

- CPU count and load estimation
- memory statistics
- container-aware reporting on Linux
- simple, dependency-free fallbacks when preferred sources are unavailable

**Table of Contents**

- [CPU reporting](#cpu-reporting)
  - [Effective CPU count](#effective-cpu-count)
  - [Utilization model](#utilization-model)
  - [Linux source hierarchy](#linux-source-hierarchy)
  - [CPU starvation vs utilization](#cpu-starvation-vs-utilization)
- [Memory reporting](#memory-reporting)
  - [Host memory](#host-memory)
  - [Container memory](#container-memory)
- [Fallback](#fallback)
- [Current limitations and future plans](#current-limitations-and-future-plans)
  - [Container detection](#container-detection)
  - [cgroup v2 memory](#cgroup-v2-memory)
  - [Hardcoded thresholds and stale samples](#hardcoded-thresholds-and-stale-samples)
  - [Future plans](#future-plans)

## CPU reporting

Public function include:

| Function name | Description |
|---|---|
| `NumCPU()` | returns the effective CPU count used by the process |
| `MaxParallelism()` | derives internal parallelism from `NumCPU()` |
| `GoEnvMaxprocs()` | adjusts `GOMAXPROCS` downward when needed |
| `MaxLoad()` | returns CPU utilization as a percentage |
| `MaxLoad2()` | returns CPU utilization plus an `isExtreme` signal for CPU starvation |
| `HighLoadWM()` | derives a high-load watermark from CPU count |
| `LoadAverage()` | reads system load averages |

### Effective CPU count

At startup, the package initializes a process-wide CPU count:

- default: `runtime.NumCPU()`
- container override: cgroup-based CPU quota when detected

The package employs prioritized capability detection:

1. Attempt cgroup v2 (cpu.max) parsing.
2. Fall back to cgroup v1 (`cpu.cfs_quota_us`).
3. If both fail or are missing, fallback to host runtime.NumCPU().

Errors from both v1 and v2 paths are aggregated and reported to stderr.

### Utilization model

CPU utilization is sampled as a delta of cumulative CPU time over wall-clock time:

- cumulative CPU usage is read from the best available source
- the previous raw sample is cached in a tracker
- utilization is computed as:

`delta_cpu_usage / (delta_wall_time * NumCPU)`

Utilization and throttling are computed atomically in a single pass from the same time delta (specifically, to synchronize "Extreme Load" signal based on throttling).

A sample gap older than the configured stale interval is discarded and treated as a reset. This avoids reporting a long-window average as if it were current load.

### Linux source hierarchy

On Linux, CPU usage currently follows this order:

1. cgroup v2: `cpu.stat`
   - `usage_usec`
   - `throttled_usec`
2. cgroup v1: `cpuacct.usage`
3. host fallback: `/proc/stat`
4. last-resort fallback: `/proc/loadavg`

The cgroup v2 path is preferred because it provides both cumulative usage and throttling information.

### CPU starvation vs utilization

`MaxLoad2()` distinguishes between:

- **high utilization**: CPU is busy
- **CPU starvation**: the process or container is being throttled and cannot get CPU time when needed

In cgroup v2 environments, throttling is read from `cpu.stat` and used only for the extreme/starvation path.
A container may therefore be treated as under extreme CPU pressure even if raw utilization is below the normal extreme-load threshold.

This is intentional: throttling indicates lack of CPU availability, not just high usage.

## Memory reporting

Public functions include:

- `MemStat.Get()` populates memory statistics
- `MemStat.Str()` formats a compact summary

### Host memory

Host memory is read from `/proc/meminfo`.

The implementation uses:

- `MemTotal`
- `MemFree`
- `MemAvailable` when present
- `Cached`
- `Buffers`
- `SwapTotal`
- `SwapFree`

If `MemAvailable` is not present, `ActualFree` falls back to:

`MemFree + BuffCache`

Derived values include:

- `Used`
- `ActualUsed`
- `SwapUsed`

### Container memory

Current Linux container memory support is based on cgroup v1 memory files:

- `memory.limit_in_bytes`
- `memory.usage_in_bytes`
- `memory.stat` (`total_cache`)

Behavior today:

- if not containerized, return host memory
- if containerized but no effective memory limit is configured, return host memory
- if cgroup memory files are unavailable or unreadable, return host memory
- swap statistics are always host statistics

This is intentionally conservative, but it also means that cgroup v2 memory-only environments are not yet handled explicitly.

## Fallback

The package generally follows these rules:

- preferred source first
- degrade to older or coarser source when necessary
- for CPU, preserve a usable percentage whenever possible
- for memory, prefer host stats over failing when container-specific files cannot be read

This makes the package resilient across bare-metal, VMs, containers, and mixed deployment environments, at the cost of occasionally returning an approximation instead of a strict container-scoped answer.

## Current limitations and future plans

### Container detection

Some Linux logic still branches early on a process-wide `containerized` flag derived from simple string matching on /proc/1/cgroup.

This is simple, but it can mis-detect certain deployment environments and steer source selection down the wrong path.

### cgroup v2 memory

CPU support already uses cgroup v2, but memory reporting does not yet have a dedicated cgroup v2 path.

As a result, a container running under cgroup v2 memory controls may currently fall back to host memory statistics.

### Hardcoded thresholds and stale samples

Several operational constants are currently compiled in, including:

- stale CPU sample interval
- CPU load thresholds
- throttling threshold for extreme CPU starvation

In particular, CPU utilization is computed from sampled deltas and depends on periodic sampling.
There is currently no synchronous (or asynchronous) path to refresh utilization sample on demand.

If the previous sample is considered stale, the package will return `0` utilization instead of a current value.

### Future work

Planned follow-ups include:

1. replace init-time container detection with capability-based source selection
   - try cgroup v2 first
   - then cgroup v1
   - then host fallbacks
2. add cgroup v2 memory support
3. tighten error handling for cgroup CPU quota parsing
4. consider environment-variable overrides for selected thresholds and tunables
5. continue reducing parser overhead only where profiling shows it matters
