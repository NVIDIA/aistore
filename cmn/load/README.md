AIStore clusters must remain resilient under varying resource pressure.

The operations — from EC encoding and LRU eviction to multi-object transformations and the get-batch ([ML endpoint](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0#getbatch-api-ml-endpoint)) — must adjust behavior at runtime based on current system load.

The `load` package provides a unified API for assessing the latter and generating an actionable throttling advice - `load.Advice`.

## Overview

The package is built around several motivations:

1.  Load assessment and throttling policy must be independent.
2.  Callers choose which load dimensions matter: CPU, memory, scheduler latency, disk, FDs.
3.  Sleep duration and sampling frequency adjust automatically.
4.  Data I/O throttles aggressively; metadata operations throttle minimally.
5.  Critical memory immediately triggers strong back-off.

The package provides one core abstraction called _throttling advice_ (`load.Advice`) that includes:
-   **How long to sleep**, if at all
-   **How frequently to check** (sample interval / batch size)
-   **What the highest load level is** among monitored load dimensions

Each of the 5 load dimensions - memory, CPU, scheduler latency, disk utilization, number of open descriptors - is independently graded:

``` go
type Load uint8

const (
    Low Load = iota + 1
    Moderate
    High
    Critical
)
```

## Design

### Five-Dimensional Load Vector

The system evaluates load across up to **five dimensions**:

-   `FlMem` --- memory pressure via `memsys.PageMM()`
-   `FlCla` --- CPU load averages
-   `FlSch` --- scheduler latency: p99 time runnable goroutines wait before running (see [Scheduler Latency](#scheduler-latency))
-   `FlDsk` --- disk utilization (per-mountpath or max)
-   `FlFdt` --- file descriptors (reserved for future use)

Internally this is represented as a packed `uint64` (8 bits per
dimension):

    ┌────────┬────────┬────────┬────────┬────────┐
    │  Fdt   │  Sch   │  Mem   │  Cla   │  Dsk   │
    │ (8bit) │ (8bit) │ (8bit) │ (8bit) │ (8bit) │
    └────────┴────────┴────────┴────────┴────────┘
        ↓        ↓        ↓        ↓        ↓
       Low     High   Critical   Low    Moderate

### Priority Ordering

When computing `load.Advice`, the following logic takes place:

1.  **Memory** --- highest priority; Critical memory triggers immediate, aggressive throttling
2.  **Scheduler latency + CPU** --- compute pressure
3.  **Disk utilization** --- I/O saturation
4.  **Workload type (RW)** --- metadata vs. data I/O

### Workload Type: `Extra.RW`

Two classes of workloads behave differently:

#### **Data I/O (`RW: true`)**

Used by: - PUT/GET
- EC encoding
- Mirroring
- Rebalance & tcb
- Chunk transformation, GetBatch, etc.

Behavior: - Under **High** load: sleep 1--10ms, check every 32--512 ops
- Under **Critical** load: sleep 10--100ms, check every 16 ops

#### **Metadata-only (`RW: false`)**

Currently used by subsystems/operations:
- metadata-in-memory eviction
- storage summary
- space cleanup
- LRU eviction

### Scheduler Latency

Scheduler latency is the time a goroutine spends runnable, waiting for the Go scheduler to run it. The runtime samples these waits and records the observations in the cumulative histogram `/sched/latencies:seconds` (`runtime/metrics`).

Scheduler latency replaces the goroutine count used previously. Idle goroutines - blocked on I/O, timers, or idle connections - cost the scheduler nothing, while a few CPU-bound ones can saturate it. The count is a proxy; latency measures the effect.

The design is best-effort and errs on the side of under-estimating the load:

-   grade the p99 over a window: the delta of two consecutive histogram reads
-   take the p99 bucket's lower bound
-   grade windows with fewer than 128 recorded observations as Low
-   at most one histogram read per window, and only on the hot path or the stats runner tick; negligible overhead

Thresholds derive from `timeout.cplane_operation`:

| Grade      | p99                                              | Default (`cplane_operation` = 2s) |
| ---------- | ------------------------------------------------ | --------------------------------- |
| `Critical` | `cplane_operation` / 20, clamped to [50ms, 500ms] | 100ms                             |
| `High`     | `Critical` / 10                                  | 10ms                              |
| `Low`      | below `High`                                     |                                   |

Two independent windows read the same histogram:

```
                 /sched/latencies:seconds (cumulative)
                                  │
              ┌───────────────────┴───────────────────┐
              ▼                                       ▼
   ┌──────────────────────┐                ┌──────────────────────┐
   │ hot path: Sched()    │                │ stats runner         │
   │ window: 1-2 ticks    │                │ window: 1 tick       │
   └──────────┬───────────┘                └──────────┬───────────┘
              │                                       │
       ┌──────┴───────────┐                           ▼
       ▼                  ▼                ┌──────────────────────┐
┌─────────────┐  ┌────────────────┐        │ node alerts          │
│ load.Advice │  │ TuneNumWorkers │        │ yellow: High         │
│ (FlSch)     │  │ (xactions)     │        │ red:    Critical     │
└─────────────┘  └────────────────┘        └──────────────────────┘
```

Both windows run at the `periodic.stats_time` cadence (default 10s, one "tick"):

-   **Hot path** (`load.Sched()`):
    -   the first caller after `periodic.stats_time` reads the histogram; other callers get the last grade
    -   more than two ticks since the previous read: record a new baseline and return Low, so that past load is never reported as current
-   **Stats runner** (`load.SchedWin`):
    -   one window per tick, regardless of traffic
    -   more than two ticks since the previous read (e.g., the runner was blocked): record a new baseline and keep the current alerts
    -   sets and clears the `NumGoroutines` (yellow) and `HighNumGoroutines` (red) node alerts; the flag names are kept for compatibility
    -   while the grade stays High or Critical, logs a warning every 10 minutes with the p99 and the number of goroutines

### Scheduler Latency Alerts

A scheduler latency alert means runnable goroutines wait for a CPU (a Go `P`) longer than the threshold. The common causes:

```
                       ┌──────────────────────────┐
                       │ p99 scheduler latency    │
                       │ alert                    │
                       └────────────┬─────────────┘
                                    │
       ┌────────────────────────────┼────────────────────────────┐
       ▼                            ▼                            ▼
┌──────────────┐            ┌──────────────┐             ┌──────────────┐
│ CPU limit /  │            │ OS thread    │             │ garbage      │
│ throttling   │            │ exhaustion   │             │ collection   │
└──────────────┘            └──────────────┘             └──────────────┘
```

**CPU limit / throttling** - more runnable work than CPUs:

-   a container CPU quota (cgroup `cpu.max`) throttles the process even when the host is idle
-   `GOMAXPROCS` above the CPU quota; at startup, AIS reduces `GOMAXPROCS` to the container's CPU count unless the `GOMAXPROCS` environment variable is set (a warning in the log)
-   other processes on the same node
-   check: CPU load (`ais show cluster`, `LowCPU`/`OOCPU` alerts); cgroup v2 `cpu.stat` (`nr_throttled`, `throttled_usec`); `go_sched_gomaxprocs_threads`

**OS thread exhaustion** - goroutines blocked in system calls hold their OS threads:

-   the runtime starts new threads to keep all `P`s busy; slow thread creation can leave runnable goroutines waiting
-   typical trigger: slow disks, with many goroutines blocked in file I/O
-   hitting cgroup `pids.max` or `ulimit -u` can prevent thread creation and terminate the runtime with a fatal `newosproc` error; a scheduler-latency alert is not guaranteed before that failure
-   check: `go_threads` (vs `go_sched_gomaxprocs_threads`); disk utilization (`ais show storage`, `DiskFault` and capacity alerts)

**Garbage collection** - the GC takes CPU from application goroutines:

-   background mark workers use about 25% of `GOMAXPROCS` while marking
-   mark assists make allocating goroutines do GC work
-   high allocation rate or a heap close to its goal (or `GOMEMLIMIT`) means frequent cycles
-   check: memory pressure (`LowMemory`/`OOM` alerts); with `Enable-Go-Runtime-Metrics`: `rate(go_cpu_classes_gc_total_cpu_seconds_total[5m])`, `rate(go_gc_heap_allocs_bytes_total[5m])`, `go_gc_heap_goal_bytes`, `go_gc_gomemlimit_bytes`

For the p99 over time, see [Go scheduler delay](/docs/monitoring-prometheus.md#go-scheduler-delay).

## Usage

### Basic Pattern

``` go
import "github.com/NVIDIA/aistore/cmn/load"

type myOp struct {
    adv load.Advice
    n   int64
}

func (op *myOp) init(mp *fs.Mountpath, config *cmn.Config) {
    op.adv.Init(
        load.FlMem | load.FlCla | load.FlDsk,
        &load.Extra{
            Mi:  mp,
            Cfg: &config.Disk,
            RW:  true,
        },
    )
}

func (op *myOp) run() {
    for ... {
        op.n++
        if op.adv.ShouldCheck(op.n) {
            op.adv.Refresh()
            if op.adv.Sleep > 0 {
                time.Sleep(op.adv.Sleep)
            }
        }
    }
}
```

### Low-Level API

``` go
if load.Mem() == load.Critical {
    // abort or back off
}

cpu := load.CPU()
sch := load.Sched() // sampled at most once per periodic.stats_time
dsk := load.Dsk(mp, &config.Disk)
```

## Throttling Constants

> Note: These defaults may evolve in future releases _or_ be overridden by configuration.

### Sleep Durations

| **Condition**                 | **Sleep** |
| ----------------------------- | --------- |
| Memory **Critical**           | 100ms     |
| Memory **High**               | 10ms      |
| Sch/CPU **Critical** (both)   | 10–100ms  |
| Sch/CPU **Critical** (either) | 1–10ms    |
| Sch/CPU **High**              | 1ms       |
| Disk **Critical**             | 10–100ms  |
| Disk **High**                 | 1ms       |

### Batch Intervals

| **Level**    | **Mask** | **Check Every** |
| ------------ | -------- | --------------- |
| `maxBatch`   | `0x1fff` | 8192 ops        |
| `dfltBatch`  | `0x1ff`  | 512 ops         |
| `smallBatch` | `0x1f`   | 32 ops          |
| `minBatch`   | `0xf`    | 16 ops          |

## Future Development

-   `FlFdt` --- file-descriptor monitoring
-   Caching of load vector
-   Configurable thresholds
