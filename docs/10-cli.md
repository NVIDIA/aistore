# Observability: CLI

The Command Line Interface (CLI) provides powerful tools for monitoring and troubleshooting your AIStore (AIS) deployment. This document covers CLI commands specifically focused on observability.

## Install

There are several ways to install AIS CLI. The easiest maybe is using:

```console
./scripts/install_from_binaries.sh --help
```

The script installs [aisloader](/docs/aisloader.md) and [CLI](/docs/cli.md) from the latest or previous GitHub [release](https://github.com/NVIDIA/aistore/releases) and enables CLI auto-completions.

Or, you could also follow this [quick-start](/docs/getting_started.md(#quick-start) instruction.

Finally, for detailed introduction (including installation) and usage, see the [CLI Overview](/docs/cli.md).

When done with installing CLI, do not forget to configure AIS endpoint - via `ais config cli` command or environment:

```console
## HTTP
export AIS_ENDPOINT=http://your-ais-cluster-endpoint:port

## or HTTPS
export AIS_ENDPOINT=https://your-ais-cluster-endpoint:port
```

## Cluster Status

```console
# Get summary of cluster membership, capacity, and health
ais show cluster

# As always, this (and all other) command's options are available via `--help`
ais show cluster --help
```

### Example: node-level alerts

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

## Best Practices

- **Regular Health Checks**: Run `ais show cluster` and `ais storage summary` daily to ensure cluster health and capacity
- **Performance Baselines**: Establish baseline performance with `ais performance show` after initial deployment
- **Monitoring Script**: Create a shell script with key monitoring commands for daily checks
- **Alert Integration**: Pipe CLI output to monitoring systems for automated alerting
- **Log Collection**: To collect logs, Integrate with Kubernetes monitoring stack or (at least) use `ais cluster download-logs`

## Troubleshooting Common Issues

| Issue | CLI Command | What to Look For |
|-------|------------|------------------|
| Node experiencing problems, transitioned to maintenance mode, or went offline | `ais show cluster` | Check the ALERT column (example above) |
| Disk failures | `ais storage mountpath` | Look for disabled or detached mountpaths |
| Performance degradation | `ais performance --refresh 30s` | Compare against baseline numbers |
| Failed operations | `ais log show --severity error` | Common error patterns |
| Network issues | `ais status network` | High latency or timeout errors |

## Resources

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
