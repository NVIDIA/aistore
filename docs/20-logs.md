# AIStore Observability: Logs

AIStore (AIS) provides comprehensive logging that captures system operations, performance metrics, and error conditions. This document explains how to configure, access, and utilize AIS logs for observability and troubleshooting.

## Log Configuration

AIS log settings can be configured in the cluster configuration and viewed using the CLI command `ais config cluster log`. Key settings include:

- **level**: Controls verbosity (from 0 to 5)
- **max_size**: Maximum size of a single log file before rotation
- **max_total**: Maximum total size of all log files
- **flush_time**: How frequently logs are flushed to disk
- **stats_time**: How frequently performance statistics are logged
- **to_stderr**: Whether to also output logs to stderr

### Example Configuration

Here's the default configuration from an AIS Local Playground development environment:

```json
    "log": {
        "level": "3",
        "max_size": "4MiB",
        "max_total": "128MiB",
        "flush_time": "1m",
        "stats_time": "1m",
        "to_stderr": false
    }
```

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

## Log Levels

AIS uses the following log levels:

| Level | Character | Description |
|-------|-----------|-------------|
| 0 | E | Error conditions and critical issues |
| 1 | W | Warning conditions that don't stop operations |
| 2-3 | I | Informational messages (default) |
| 4-5 | D | Debug and trace information |

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

| Metric | Description |
|--------|-------------|
| `get.n` | Number of GET operations |
| `put.n` | Number of PUT operations |
| `get.size` | Total size of GET operations |
| `get.bps` | Bytes per second for GET operations |
| `aws.head.n` | Number of HEAD requests to AWS S3 |
| `err.get.n` | Number of GET errors |
| `disk.[device].read.bps` | Read throughput for specific disk |
| `disk.[device].util` | Utilization percentage for specific disk |

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

## Analyzing Logs for Troubleshooting

When troubleshooting issues, focus on:

1. Error (E) and Warning (W) log entries
2. Unusual patterns in performance metrics
3. Operation failures indicated by error counters
4. Disk utilization rates above expected thresholds

For advanced log analysis, consider forwarding logs to external systems for aggregation and visualization.

## Related Documentation

| Document | Description |
|----------|-------------|
| [Observability: Overview](00-overview.md) | Introduction to AIS observability approaches |
| [Observability: CLI](10-cli.md) | Command-line monitoring tools |
| [Observability: Prometheus](30-prometheus.md) | Configuring Prometheus with AIS |
| [Observability: Metrics Reference](31-metrics-reference.md) | Complete metrics catalog |
| [Observability: Grafana](40-grafana.md) | Visualizing AIS metrics with Grafana |
| [Observability: Kubernetes](50-k8s.md) | Working with Kubernetes monitoring stacks |
