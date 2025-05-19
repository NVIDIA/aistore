# AIStore Observability: Logs

AIStore (AIS) provides comprehensive logging that captures system operations, performance metrics, and error conditions. 

> **Scope.** How to configure, collect, and read AIS logs.

AIS logs are the cluster’s ground truth: every proxy or target writes a chronological stream of events, warnings, and periodic performance snapshots. Well‑rotated logs let operators

* reconstruct incidents (root‑cause analysis),
* correlate client symptoms with internal state changes, and
* spot long‑running jobs without polling the control plane.

---

## Configuring logging

```bash
# show the cluster‑wide logging section (current values)
ais config cluster log
```

| Key          | Purpose                                                                                                                            | Typical prod value |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `level`      | Info verbosity **0-5** (`3` = normal, `4`/`5` = chatty, `<3` disables *info*). `W` and `E` are **always** logged.                  | `3`                |
| `modules`    | Space‑separated list of modules whose *info* lines are forced to **level 5** (e.g., `ec space`). Use `none` to clear the override. | `none`             |
| `max_size`   | Rotate when a single file exceeds this size                                                                                        | `32MiB`            |
| `max_total`  | Upper bound for the entire directory (oldest files deleted first)                                                                  | `1GiB`             |
| `flush_time` | How often each daemon flushes its in‑memory buffer                                                                                 | `10s`              |
| `stats_time` | Interval for automatic performance snapshots                                                                                       | `60s`              |
| `to_stderr`  | Duplicate log lines to stderr (handy for systemd / kubectl logs)                                                                   | `false`            |

```

Show current values:

```bash
ais config cluster log               # cluster‑wide
ais config node   NODE_ID log        # single node (effective)
```

The new value propagates to every node within a second.

### Example (development defaults)

```json
"log": {
    "level": "3",
    "modules": "none",
    "max_size": "4MiB",
    "max_total": "128MiB",
    "flush_time": "1m",
    "stats_time": "1m",
    "to_stderr": false
}
```

### Example (production configuration)

In production environments, settings are typically adjusted for higher retention and less frequent statistics collection:

```
$ ais config cluster log
PROPERTY         VALUE
log.level        3
log.max_size     4MiB
log.max_total    512MiB
log.flush_time   1m
log.stats_time   3m
log.to_stderr    false
```

At startup, AIS logs some of these settings:

```
I 19:28:24.774518 config:2143 log.dir: "/var/log/ais"; l4.proto: tcp; pub port: 51080; verbosity: 3
I 19:28:24.774523 config:2145 config: "/etc/ais/.ais.conf"; stats_time: 10s; authentication: false; backends: [aws]
```

---

## Severity & verbosity

AIS prepends every line with a **severity prefix** and—in the case of informational messages—an **internal numeric level**.

### Severity prefixes

| Prefix | Meaning                              | Printed when             |
| ------ | ------------------------------------ | ------------------------ |
| `E`    | Error – unrecoverable / user‑visible | Always                   |
| `W`    | Warning – succeeded but suspicious   | Always                   |
| `I`    | Informational                        | Only if allowed by level |

### Numeric levels for *I* lines

| Level   | Typical use (examples)                             |
| ------- | -------------------------------------------------- |
| **5**   | Hot‑path trace, request headers, per‑part          |
| **4**   | Verbose progress, retries, caching stats           |
| **3**   | Startup, shutdown, xaction summaries **(default)** |
| **2‑0** | Progressively quieter; at `≤2` almost silent       |

> **Tip.** Temporarily crank a node:
>
> ```bash
> ais config node set TARGET_ID log.level 4
> ais config cluster log.modules ec xs   # focus on EC & xactions
> ```

### Per‑module overrides (`log.modules`)

`log.modules` lets you boost just a subset of subsystems to level 5 without flooding the whole cluster.

```bash
# Elevate erasure‑coding (ec) and xaction scheduler (xs):
ais config cluster log.modules ec xs

# Revert to normal
ais config cluster log.modules none
```

---

## Log file layout & rotation

| Environment | Where the bytes land | Notes                            |
| ----------- | -------------------- | -------------------------------- |
| Bare‑metal  | `/var/log/ais/` | One file per daemon              |
| Kubernetes  | Container **stdout** | Collected by runtime / log agent |

Log files rotate when `max_size` is hit; AIS deletes the oldest ones to abide by the configured `max_total` limitation (no compression inside AIS cluster itself).

---

## Log format cheat‑sheet

```
I 2025‑05‑19 13:42:17.791884 cpu:60 Reducing GOMAXPROCS (prev=256) to 32
│ │            │      │              └─ message
│ │            │      └─ Go file:line inside AIS source
│ │            └─ timestamp (µs precision)
│ └─ severity prefix
└─ ‘I’ for INFO
```

Common prefixes:

* `config:`   – effective runtime configuration
* `x-<name>:` – extended (batch) action lifecycle
* `nvmeXnY:` – per‑disk I/O snapshot
* `kvstats:` – cluster‑wide key‑value metrics (see below)

---

## Log file layout & rotation

| Environment | Where logs appear                   | Notes                                |
| ----------- | ----------------------------------- | ------------------------------------ |
| Bare‑metal  | `/var/log/ais/<node>.log`         | One file per daemon (proxy / target) |
| Kubernetes  | container **stdout** (kubectl logs) | Collected by CRI‑O / containerd      |

File names include the node ID plus a sequence number (`target‑A43c.log.3`). Rotation is triggered by `max_size`; retention is enforced by `max_total`.

---

## Severity & verbosity

AIS prints every line with a **severity prefix** and, for *info* lines, a **numeric verbosity level**.

### Severity prefixes

| Prefix | Meaning                               |
| ------ | ------------------------------------- |
| `E`    | Error – non‑recoverable failure       |
| `W`    | Warning – operation succeeded but odd |
| `I`    | Informational                         |

### Info verbosity (`log.level`)

The cluster‑wide `log.level` flag controls *only* informational lines:

* `5` – maximum detail (hot‑path trace, request headers)
* `4` – verbose (xaction progress, retry notices)
* `3` – **default** (startup, shutdown, job summaries)
* `<3` – suppress *I* lines entirely (still logs **W** and **E**)

Temporarily change a single node:

```bash
ais config node set target‑A43c log.level 4
```

\---------|--------|----------------------------------------|
\| `0`     | `E`    | Error – non‑recoverable failure        |
\| `1`     | `W`    | Warning – operation succeeded but odd  |
\| `2‑5`   | `I`    | Info – increasing detail as number ↑   |

> **Tip.** Temporarily increase verbosity on a single node when debugging:
>
> ```bash
> ais config node set target‑A43c log.level 4
> ```


## Log Format and Structure

AIS logs follow a consistent format with the following components:

- **Log level**: Single character indicating severity (I, W, E)
- **Timestamp**: Time with microsecond precision
- **Module and line**: Source code location
- **Message**: The actual log content

### Actual Log Entry Examples

```
I 19:28:24.774540 daemon:311 Version 3.28.2ec8b22, build 2025-05-13T19:20:12+0000, CPUs(32, runtime=256), containerized
W 19:28:24.774555 cpu:60 Reducing GOMAXPROCS (prev = 256) to 32
I 19:28:24.775140 k8s:68 Checking pod: "ais-proxy-15"
```

The default log level is 3 (Info).

## Accessing Logs

### Via CLI

The AIS CLI provides commands to view and collect logs. While the exact commands may vary by version, typical operations include:

```bash
# View logs from a specific node
ais log show [NODE_ID]

# Filter logs by severity
ais log show [NODE_ID] --severity error

# Collect logs from all nodes
ais log get --help
```

### Directly in Kubernetes

In Kubernetes deployments, access logs using kubectl:

```bash
kubectl logs -n ais ais-proxy-15
kubectl logs -n ais ais-target-13
```

## Common Log Patterns

### Startup Sequence

The startup sequence provides important information about the AIS node configuration:

```
Started up at 2025/05/13 19:28:24, host ais-proxy-15, go1.24.3 for linux/amd64
W 19:28:24.774364 config:1506 control and data share the same intra-cluster network: ais-proxy-15.ais-proxy.ais.svc.cluster.local
I 19:28:24.774518 config:2143 log.dir: "/var/log/ais"; l4.proto: tcp; pub port: 51080; verbosity: 3
I 19:28:24.774523 config:2145 config: "/etc/ais/.ais.conf"; stats_time: 10s; authentication: false; backends: [aws]
I 19:28:24.774540 daemon:311 Version 3.28.2ec8b22, build 2025-05-13T19:20:12+0000, CPUs(32, runtime=256), containerized
```

### Operation Logs

AIS logs details about operations such as list, put, get:

```
I 21:00:43.063816 base:211 x-list[ApJcaebM5]-ais://yodas-21:00:03.062994-00:00:00.000000 finished
I 21:00:44.482430 base:211 x-list[J1qpgaWbxG]-ais://yodas-21:00:04.481894-00:00:00.000000 finished
```

### Performance Metrics

AIS regularly logs performance metrics in two formats:

1. Disk-specific performance:

```
I 21:00:48.784074 nvme3n1: 54MiB/s, 119KiB, 0B/s, 0B, 26%
I 21:00:48.784078 nvme9n1: 41MiB/s, 119KiB, 0B/s, 0B, 20%
I 21:00:48.784080 nvme10n1: 38MiB/s, 112KiB, 0B/s, 0B, 18%
```

2. Comprehensive key-value statistics (at regular intervals defined by `stats_time`):

```
I 18:06:18.785011 {aws.head.n:114227,aws.head.ns.total:16799090100532,del.n:109,err.get.n:296108,err.ren.n:1,etl.offline.n:1136785,etl.offline.ns.total:73240613148414,get.bps:219094016,get.n:1006219,get.ns:425545477639,get.ns.total:3041645043551487925,get.redir.ns:3262104,get.size:126049578974910,lcache.evicted.n:1211669,lcache.flush.cold.n:491970,lst.n:8824,put.n:286,put.ns.total:1706491834824,put.size:101717912588,ren.n:103,state.flags:32774,stream.in.n:441,stream.in.size:96717189120,stream.out.n:446,stream.out.size:84119388160,disk.nvme7n1.read.bps:35240346...}
```

## Key Performance Metrics

The key-value statistics contain valuable operational metrics:

| Key / pattern | Description |
|--------|-------------|
| `get.n` | Number of GET operations |
| `put.n` | Number of PUT operations |
| `get.size`, `put.size` | Cumulative bytes, GET and PUT respectively |
| `get.bps` | Bytes per second for GET operations |
| `aws.<name>.ns.total`         | Cumulative latency against a cloud backend (AWS S3, in this example) |
| `aws.head.n` | Number of HEAD requests to AWS S3 |
| `err.get.n` | Number of GET errors |
| `err.io.get.n` | Number of in-cluster GET I/O errors (excluding network and remote backend errors, e.g. "broken pipe", "connection reset") |
| `disk.<device>.read.bps` | Read throughput for specific disk |
| `disk.[device>.util` | Device utilization (percentage) |

## Log Rotation

AIS implements automatic log rotation as indicated by the header:

```
Rotated at 2025/05/14 21:00:38, host ais-target-13, go1.24.3 for linux/amd64
```

When logs are rotated, new log files are created and old ones are typically compressed or archived according to the retention policy.

## Kubernetes-specific Information

In Kubernetes deployments, AIS logs include pod and cluster-specific details:

```
I 19:28:24.786264 k8s:93 Pod info: name ais-proxy-15 ,namespace ais ,node 10.49.41.55 ,hostname ais-proxy-15 ,host_network false
I 19:28:24.786281 k8s:101   ais-ais-state (&PersistentVolumeClaimVolumeSource{ClaimName:ais-ais-state-ais-proxy-15,ReadOnly:false,})
I 19:28:24.786304 k8s:103   config-template
I 19:28:24.786306 k8s:103   config-mount
```

## Troubleshooting checklist

1. Scan for **^E** & **^W** lines around the timeframe.
2. Look for spikes in `err.<name>.n` counters; pay special attention to `err.io.<name>` error counters.
3. Watch disk `util` > 80 % or sustained `read.bps` plateaus.
4. Temporarily raise `log.level` or `log.modules` on a single node to capture more detail.

For advanced log analysis, consider forwarding logs to external systems for aggregation and visualization.

## Operational tips

* Watch K8s dashboard and/or `ais show cluster` for any alerts.
* Pay special attention to `err.io.<name>` error counters.
* Keep `log.level=3` in production; raise to `4` or `5` only while debugging.
* Per-module verbosity is controlled via `log.modules`. Lower log level to `2` or below if you truly need silence.
* Raise `stats_time` (≥ 60 s) if logs get noisy on busy systems.
* Ship rotated logs off‑host weekly.
* Always attach `ais cluster download-logs` tarball to GitHub issues.

## Related Documentation

| Document | Description |
|----------|-------------|
| [Observability: Overview](/docs/00-overview.md) | Introduction to AIS observability approaches |
| [Observability: CLI](/docs/10-cli.md) | Command-line monitoring tools |
| [Observability: Prometheus](/docs/30-prometheus.md) | Configuring Prometheus with AIS |
| [Observability: Metrics Reference](/docs/31-metrics-reference.md) | Complete metrics catalog |
| [Observability: Grafana](/docs/40-grafana.md) | Visualizing AIS metrics with Grafana |
| [Observability: Kubernetes](/docs/50-k8s.md) | Working with Kubernetes monitoring stacks |
