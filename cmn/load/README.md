AIStore clusters must remain resilient under varying resource pressure.

The operations — from EC encoding and LRU eviction to multi-object transformations and the get-batch ([ML endpoint](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0#getbatch-api-ml-endpoint)) — must adjust behavior at runtime based on current system load.

The `load` package provides a unified API for assessing the latter and generating an actionable throttling advice - `load.Advice`.

## Overview

The package is built around several motivations:

1.  Load assessment and throttling policy must be independent.
2.  Callers choose which load dimensions matter: CPU, memory, goroutines, disk, FDs.
3.  Sleep duration and sampling frequency adjust automatically.
4.  Data I/O throttles aggressively; metadata operations throttle minimally.
5.  Critical memory immediately triggers strong back-off.

The package provides one core abstraction called _throttling advice_ (`load.Advice`) that includes:
-   **How long to sleep**, if at all
-   **How frequently to check** (sample interval / batch size)
-   **What the highest load level is** among monitored load dimensions

Each of the 5 load dimensions - memory, CPU, goroutine count, disk utilization, number of open descriptors - is independently graded:

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
-   `FlGor` --- goroutine count
-   `FlDsk` --- disk utilization (per-mountpath or max)
-   `FlFdt` --- file descriptors (reserved for future use)

Internally this is represented as a packed `uint64` (8 bits per
dimension):

    ┌────────┬────────┬────────┬────────┬────────┐
    │  Fdt   │  Gor   │  Mem   │  Cla   │  Dsk   │
    │ (8bit) │ (8bit) │ (8bit) │ (8bit) │ (8bit) │
    └────────┴────────┴────────┴────────┴────────┘
        ↓        ↓        ↓        ↓        ↓
       Low     High   Critical   Low    Moderate

### Priority Ordering

When computing `load.Advice`, the following logic takes place:

1.  **Memory** --- highest priority; Critical memory triggers immediate, aggressive throttling
2.  **Goroutines + CPU** --- compute pressure
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
gor := load.Gor(runtime.NumGoroutine())
dsk := load.Dsk(mp, &config.Disk)
```

## Throttling Constants

> Note: These defaults may evolve in future releases _or_ be overridden by configuration.

### Sleep Durations

| **Condition**                 | **Sleep** |
| ----------------------------- | --------- |
| Memory **Critical**           | 100ms     |
| Memory **High**               | 10ms      |
| Gor/CPU **Critical** (both)   | 10–100ms  |
| Gor/CPU **Critical** (either) | 1–10ms    |
| Gor/CPU **High**              | 1ms       |
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
