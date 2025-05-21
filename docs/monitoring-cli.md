# AIStore Observability: CLI

> The CLI is the fastest way to interrogate an AIS cluster from a terminal. This page is a jump‑table to the handful of commands every SRE or developer uses when triaging performance or capacity issues. For full syntax hit <kbd>--help</kbd> on any command or see the separate [CLI reference](/docs/cli.md).

## Table of Contents
- [Installation](#installation)
- [Cluster Status](#cluster-status)
  - [Example: Node-level Alerts](#example-node-level-alerts)
- [Node Alerts](#node-alerts)
- [Live Performance Monitoring](#live-performance-monitoring)
  - [Key Flags](#key-flags)
- [Log Management](#log-management)
- [Common Command Examples](#common-command-examples)
- [Best Practices](#best-practices)
- [Troubleshooting Common Issues](#troubleshooting-common-issues)
- [CLI Resources](#cli-resources)
- [Related Documentation](#related-documentation)

## Installation

There are several ways to install AIS CLI:

1. Using the installation script (recommended):

```console
./scripts/install_from_binaries.sh --help
```

This script installs [aisloader](/docs/aisloader.md) and [CLI](/docs/cli.md) from the latest or previous GitHub [release](https://github.com/NVIDIA/aistore/releases) and enables CLI auto-completions.

2. Follow the [quick-start](/docs/getting_started.md#quick-start) instructions.

3. For detailed introduction (including installation) and usage, see the [CLI Overview](/docs/cli.md).

After installation, configure your AIS endpoint via the `ais config cli` command or environment variables:

```console
## HTTP
export AIS_ENDPOINT=http://your-ais-cluster-endpoint:port

## or HTTPS
export AIS_ENDPOINT=https://your-ais-cluster-endpoint:port
```

## Cluster Status

| Question                            | Command                 | Typical flags   |
| ----------------------------------- | ----------------------- | --------------- |
| Nodes and their respective health? Any alerts? Out of space? Out of memory? | `ais show cluster`  | `--refresh 1m` |
| How much space is left?             | `ais storage summary`   | `--cached`, `--units`, `--prefix`, `--refresh`  |
| Are any mountpaths down?            | `ais storage mountpath` | `--fshc` (to run filesystem health checker), `--rescan-disks` |

```console
# Get summary of cluster membership, capacity, and health
ais show cluster

# As always, this (and all other) command's options are available via `--help`
ais show cluster --help
```

### Example: Node-level Alerts

```console
$ ais show cluster

PROXY            MEM AVAIL  LOAD AVERAGE    UPTIME      STATUS  ALERT
p[KKFpNjqo][P]   127.77GiB  [5.2 7.2 3.1]   108h30m40s  online  **tls-cert-will-soon-expire**
...

TARGET           MEM AVAIL  CAP USED(%)     CAP AVAIL   LOAD AVERAGE    UPTIME      STATUS  ALERT
t[pDztYhhb]      98.02GiB   16%             960.824GiB  [9.1 13.4 8.3]  108h30m1s  online   **tls-cert-will-soon-expire**
...
...
```

## Node Alerts

AIStore node states are categorized into three severity levels:

1. **Red Alerts** - Critical issues requiring immediate attention:
   - `OOS` - Out of space condition
   - `OOM` - Out of memory condition
   - `OOCPU` - Out of CPU resources
   - `DiskFault` - Disk failures detected
   - `NoMountpaths` - No available mountpaths
   - `NumGoroutines` - Excessive number of goroutines
   - `CertificateExpired` - TLS certificate has expired
   - `CertificateInvalid` - TLS certificate is invalid

2. **Warning Alerts** - Potential issues that may require attention:
   - `Rebalancing` - Rebalance operation in progress
   - `RebalanceInterrupted` - Rebalance was interrupted
   - `Resilvering` - Resilvering operation in progress
   - `ResilverInterrupted` - Resilver was interrupted
   - `NodeRestarted` - Node was restarted (powercycle, crash)
   - `MaintenanceMode` - Node is in maintenance mode
   - `LowCapacity` - Low storage capacity (OOS possible soon)
   - `LowMemory` - Low memory condition (OOM possible soon)
   - `LowCPU` - Low CPU availability
   - `CertWillSoonExpire` - TLS certificate will expire soon
   - `KeepAliveErrors` - Recent keep-alive errors detected

3. **Information States** - Normal operational states:
   - `ClusterStarted` - Cluster has started (primary) or node has joined cluster
   - `NodeStarted` - Node has started (may not have joined cluster yet)
   - `VoteInProgress` - Voting process is in progress

Node state flags are also exposed via Prometheus metrics - for details, see:

* [Node Alerts in AIStore Prometheus docs](/docs/monitoring-prometheus.md#node-alerts).

## Live Performance Monitoring

`ais performance` (alias `ais show performance`) exposes five sub‑commands. The two most used are **throughput** and **latency**.

```console
# 30‑second rolling throughput for all targets
$ ais performance throughput --refresh 30

# 10‑second latency slice, filter to GET operations
$ ais performance latency --refresh 10 --regex "get"
```

### Key Flags

| Flag              | Meaning                               |
| ----------------- | ------------------------------------- |
| `--refresh <dur>` | Continuous mode; prints every *dur*   |
| `--count <n>`     | Stop after *n* refreshes              |
| `--regex <re>`    | Show only columns matching the regexp |
| `--no‑headers`    | Suppress table headers                |

> See [`cli-performance.md`](/docs/cli/performance.md) for sub‑command specifics.

## Log Management

| Task                                   | Command                                     |
| -------------------------------------- | ------------------------------------------- |
| Tail a given node's log                | `ais log show --refresh DURATION --help`    |
| Download all logs for a support bundle | `ais cluster download-logs`                 |
| Rotate logs on one node                | `ais advanced rotate-logs <NODE_ID>`        |

For more details on log configuration and analysis, see [Observability: Logs](/docs/monitoring-logs.md).

## Common Command Examples

Here are some frequently used command combinations for everyday operations:

```console
# Daily capacity & health snapshot
ais show cluster && ais storage summary

# Watch GET latency for a single target
ais performance latency t[EkMt8081] --refresh 30 --regex "get(\(t\)|cold)"

# Verify no misplaced objects in GCS buckets (non‑recursive)
ais scrub gs --nr --refresh 20s --count 3
```

> Flags such as `--refresh <duration>`, `--count <n>`, `--regex <re>`, `--no-headers`, and `--units` are accepted by most monitoring commands; see `--help` for the definitive list.

## Best Practices

- **Regular Health Checks**: Run `ais show cluster` and `ais storage summary` daily to ensure cluster health and capacity
- **Performance Baselines**: Establish baseline performance with `ais performance show` after initial deployment
- **Monitoring Script**: Create a shell script with key monitoring commands for daily checks
- **Alert Integration**: Pipe CLI output to monitoring systems for automated alerting
- **Log Collection**: To collect logs, integrate with a Kubernetes monitoring stack or (at least) use `ais cluster download-logs`

## Troubleshooting Common Issues

| Issue | CLI Command | What to Look For |
|-------|------------|------------------|
| Node experiencing problems or went offline | `ais show cluster` | Check the ALERT column (example above) |
| Disk failures | `ais storage mountpath` | Look for disabled or detached mountpaths |
| Performance degradation | `ais performance --refresh 30s` | Compare against baseline numbers |
| Failed operations | `ais log show --severity error` | Common error patterns |
| Network issues | `ais status network` | High latency or timeout errors |

## CLI Resources

- [`ais help`](/docs/cli/help.md)
- [Reference guide](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md#cli-reference)
- [Monitoring](/docs/cli/show.md)
  - [`ais show cluster`](/docs/cli/show.md)
  - [`ais show performance`](/docs/cli/show.md)
  - [`ais show job`](/docs/cli/show.md)
  - [`ais show config`](/docs/cli/show.md)
- [Cluster and node management](/docs/cli/cluster.md)
- [Mountpath (disk) management](/docs/cli/storage.md)
- [Attach, detach, and monitor remote clusters](/docs/cli/cluster.md)
- [Start, stop, and monitor downloads](/docs/cli/download.md)
- [Distributed shuffle](/docs/cli/dsort.md)
- [User account and access management](/docs/cli/auth.md)
- [Jobs](/docs/cli/job.md)
- [AIS CLI Reference](/docs/cli.md)

## Related Documentation

| Document | Description |
|----------|-------------|
| [Overview](/docs/monitoring-overview.md) | Introduction to AIS observability |
| [Logs](/docs/monitoring-logs.md) | Configuring, accessing, and utilizing AIS logs |
| [Prometheus](/docs/monitoring-prometheus.md) | Configuring Prometheus with AIS |
| [Metrics Reference](/docs/monitoring-metrics.md) | Complete metrics catalog |
| [Grafana](/docs/monitoring-grafana.md) | Visualizing AIS metrics with Grafana |
| [Kubernetes](/docs/monitoring-kubernetes.md) | Working with Kubernetes monitoring stacks |
