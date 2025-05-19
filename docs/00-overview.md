# AIStore Observability

This document provides an overview of AIStore (AIS) observability features, tools, and practices. AIS offers comprehensive observability through logs, metrics, and a CLI interface, enabling users to monitor, debug, and optimize their deployments.

## Observability Architecture

AIS provides multiple _layers_ of observability:

```
┌─────────────────────────────────┐
│       Visualization Layer       │
│  ┌───────────┐    ┌───────────┐ │
│  │  Grafana  │    │   Custom  │ │
│  │ Dashboard │    │   UIs     │ │
│  └───────────┘    └───────────┘ │
├─────────────────────────────────┤
│       Collection Layer          │
│  ┌───────────┐    ┌───────────┐ │
│  │ Prometheus│    │  StatsD*  │ │
│  │           │    │           │ │
│  └───────────┘    └───────────┘ │
├─────────────────────────────────┤
│       Instrumentation Layer     │
│  ┌───────────┐    ┌───────────┐ │
│  │  Metrics  │    │   Logs    │ │
│  │ Endpoints │    │           │ │
│  └───────────┘    └───────────┘ │
├─────────────────────────────────┤
│          Access Layer           │
│  ┌───────────┐    ┌───────────┐ │
│  │    CLI    │    │   REST    │ │
│  │ Interface │    │   APIs    │ │
│  └───────────┘    └───────────┘ │
└─────────────────────────────────┘
```

> `(*)` StatsD support will _likely_ be removed in late 2025.

## Transition from StatsD to Prometheus

AIS began with StatsD for metrics collection but has evolved to primarily use Prometheus. Key points about this transition:

- **Prometheus** (and Grafana) is now the recommended monitoring system
- All new metric implementations use Prometheus exclusively
- The transition provides better scalability, more detailed metrics, variable labels for advanced filtering, and improved integration with modern observability stacks

## Observability Methods

| Method | Description | Use Cases | Documentation |
|--------|-------------|-----------|---------------|
| **CLI** | Command-line tools for monitoring and troubleshooting | Quick checks, diagnostics, interactive troubleshooting | [Observability: CLI](/docs/10-cli.md) |
| **Logs** | Detailed event logs with configurable verbosity | Debugging, audit trails, understanding system behavior | [Observability: Logs](/docs/20-logs.md) |
| **Prometheus** | Time-series metrics exposed via HTTP endpoints | Performance monitoring, alerting, trend analysis | [Observability: Prometheus](/docs/30-prometheus.md) |
| **Metrics Reference** | Metric groups, names, and descriptions | Quick search for specific metric | [Observability: Metrics Reference](/docs/31-metrics-reference.md) |
| **Grafana** | Visualization dashboards for AIS metrics | Visual monitoring, sharing operational status | [Observability: Grafana](/docs/40-grafana.md) |
| **Kubernetes** |  Kubernetes deployments | Working with Kubernetes monitoring stacks | [Observability: Kubernetes](/docs/50-k8s.md) |

## Kubernetes Integration

For Kubernetes deployments, AIS provides additional observability features designed to integrate with Kubernetes monitoring stacks.

There's a dedicated (and separate) GitHub [repository](https://github.com/NVIDIA/ais-k8s) that, in particular, provides Helm charts for AIS Cluster monitoring.

See the [Kubernetes Observability](50-k8s.md) document for details.

## Key Metrics Categories

AIS exposes metrics across several categories:

- **Cluster Health**: Node status, membership changes
- **Resource Usage**: CPU, memory, disk utilization
- **Performance**: Throughput, latency, error counts
- **Storage Operations**: GET/PUT rates, object counts, error counts
- **Errors**: Network errors ("broken pipe", "connection reset"), timeouts ("deadline exceeded"), retries ("too-many-requests"), disk faults, OOM, out-of-space, and more

In addition, all supported [jobs](/docs/batch.md) that read or write data report their respective progress in terms of objects and bytes (counts).

Briefly, two CLI examples:

#### Cluster performance: operation counts and latency

```console
$ ais performance latency --refresh 10 --regex get

| TARGET | AWS-GET(n) | AWS-GET(t) | GET(n) | GET(t) | GET(total/avg size) | RATELIM-RETRY-GET(n) | RATELIM-RETRY-GET(t) |
|:------:|:----------:|:----------:|:------:|:------:|:--------------------:|:---------------------:|:---------------------:|
| T1     | 800        | 180ms      | 3200   | 25ms   | 12GB / 3.75MB       | 50                    | 240ms                |
| T2     | 1000       | 150ms      | 4000   | 28ms   | 15GB / 3.75MB       | 70                    | 230ms                |
| T3     | 700        | 200ms      | 2800   | 32ms   | 10GB / 3.57MB       | 40                    | 215ms                |

- **AWS-GET(n)** / **AWS-GET(t)**: Number and average latency of GET requests that actually hit the AWS backend.
- **GET(n)** / **GET(t)**: Number and average latency of *all* GET requests (including those served from local cache or in-cluster data).
- **GET(total/avg size)**: Approximate total data read and corresponding average object size.
- **RATELIM-RETRY-GET(n)** / **RATELIM-RETRY-GET(t)**: Number and average latency of GET requests retried due to hitting the rate limit.
```

#### Batch job: Prefetch

```console
$ ais show job prefetch --refresh 10

prefetch-objects[MV4ex8u6h] (run options: prefix:10, workers: 16, parallelism: w[16] chan-full[8,32])
NODE             ID              KIND                    BUCKET          OBJECTS         BYTES           START           END     STATE
KactABCD         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 27              27.00MiB        18:28:55        -       Running
XXytEFGH         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 23              23.00MiB        18:28:55        -       Running
YMjtIJKL         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 41              41.00MiB        18:28:55        -       Running
oJXtMNOP         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 34              34.00MiB        18:28:55        -       Running
vWrtQRST         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 23              23.00MiB        18:28:55        -       Running
ybTtUVWX         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 31              31.00MiB        18:28:55        -       Running
                                Total:                                  179             179.00MiB ✓
```

## Best Practices

- Configure appropriate [log levels](/docs/cli/config.md) based on your deployment stage (development or production).
- Set up alerting for critical metrics using [Prometheus AlertManager](https://prometheus.io/docs/alerting/latest/alertmanager/) to proactively monitor system health.
- Implement regular [dashboard reviews](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/README.md) to analyze short- and long-term statistics and identify performance trends.
- View or download logs via [Loki](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/loki/README.md). You can also use the CLI commands `ais log` or `ais cluster download-logs` (use `--help` for details) to access logs for troubleshooting and analysis.

## Further Reading

- [Performance Tuning](https://github.com/NVIDIA/aistore/blob/main/docs/performance.md)
- [AIS K8s Playbooks: host configuration](https://github.com/NVIDIA/ais-k8s/tree/main/playbooks/host-config)
- [Prometheus Documentation](https://prometheus.io/docs/introduction/overview/)
- [Grafana Documentation](https://grafana.com/docs/)
