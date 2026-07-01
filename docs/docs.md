---
layout: post
title: DOCUMENTATION
description: "Complete AIStore documentation: architecture, CLI, Python SDK, HTTP API, ETL, deployment, and operations guides."
permalink: /docs
redirect_from:
 - /docs.md/
 - /docs/docs.md/
---

AIStore (AIS) is a lightweight distributed storage stack for AI workloads. It
can run on a single Linux machine or across production clusters, with native
support for in-cluster storage, remote cloud buckets, batch jobs, SDKs, and
observability.

Use this page as a quick orientation and navigation map:

- New to AIS: start with [About AIStore](#about-aistore), then follow
  [Get Started](#get-started).
- Managing data and jobs: use [Use AIStore](#use-aistore).
- Running clusters: use [Operations](#operations), [Observability](#observability),
  and [Performance](#performance).
- Looking up APIs, SDKs, compatibility, security, protocols, or limits: use
  [Reference](#reference).
- Debugging a problem: use [Troubleshooting](#troubleshooting).

## About AIStore

- [Main README](/README.md)
- [In-depth overview](/docs/overview.md)
- [Terminology and core abstractions](/docs/terminology.md)
- [Networking model](/docs/networking.md)
- [Traffic patterns](/docs/traffic_patterns.md)
- [Backend providers](/docs/providers.md)
- [Storage services](/docs/storage_svcs.md)
- [Buckets: design, operations, namespaces, and system buckets](/docs/bucket.md)
- [On-disk layout](/docs/on_disk_layout.md)
- [Highly available control plane](/docs/ha.md)

## Get Started

- [Getting Started](/docs/getting_started.md)
- [Docker](/docs/docker_main.md)
- [AIS in containerized environments](/docs/containerized.md)
- [Configuration](/docs/configuration.md)
- [Environment variables](/docs/environment-vars.md)
- [HTTPS and certificates](/docs/https.md)
- [Switching a cluster to HTTPS](/docs/switch_https.md)

## Use AIStore

- [CLI overview](/docs/cli.md)
- [`ais help`](/docs/cli/help.md)
- [Bucket operations](/docs/cli/bucket.md)
- [Show cluster, bucket, and object details](/docs/cli/show.md)
- [Cluster and remote-cluster management](/docs/cli/cluster.md)
- [Storage and mountpath management](/docs/cli/storage.md)
- [Downloads](/docs/cli/download.md)
- [Evict buckets or cached data](/docs/cli/evicting_buckets_andor_data.md)
- [Jobs](/docs/cli/job.md)
- [ETL overview](/docs/etl.md)
- [ETL CLI docs](/docs/cli/etl.md)
- [Archives: read, write, and list](/docs/archive.md)
- [Downloader](/docs/downloader.md)
- [Blob Downloader](/docs/blob_downloader.md)
- [Batch object retrieval (get-batch)](/docs/get_batch.md)
- [Batch operations](/docs/batch.md)
- [Virtual directories](/docs/howto_virt_dirs.md)
- [Machine learning workloads](/docs/cli/ml.md)
- [CLI authentication and access control](/docs/cli/auth.md)
- [Configuration via CLI](/docs/cli/config.md)
- [GCP credentials via CLI](/docs/cli/gcp_creds.md)
- [TLS certificate management via CLI](/docs/cli/x509.md)

## Operations

- [Production deployment](https://github.com/NVIDIA/aistore/blob/main/deploy/README.md)
- [AIStore on Kubernetes](https://github.com/NVIDIA/ais-k8s)
- [Kubernetes Operator](https://github.com/NVIDIA/ais-k8s/blob/main/operator/README.md)
- [Ansible playbooks](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/README.md)
- [Helm charts](https://github.com/NVIDIA/ais-k8s/tree/main/helm)
- [Deployment monitoring](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/README.md)
- [Node lifecycle: maintenance, shutdown, decommission](/docs/lifecycle_node.md)
- [Global rebalance](/docs/rebalance.md)
- [Resilver](/docs/resilver.md)
- [Information Center (IC)](/docs/ic.md)
- [Out-of-band updates](/docs/out_of_band.md)
- [Native Bucket Inventory (NBI)](/docs/nbi.md)
- [System files](/docs/sysfiles.md)

## Observability

- [Observability overview](/docs/monitoring-overview.md)
- [Monitoring with CLI](/docs/monitoring-cli.md)
- [Logs](/docs/monitoring-logs.md)
- [Prometheus integration](/docs/monitoring-prometheus.md)
- [Metrics reference](/docs/monitoring-metrics.md)
- [Grafana dashboards](/docs/monitoring-grafana.md)
- [Kubernetes monitoring](/docs/monitoring-kubernetes.md)
- [Distributed tracing](/docs/distributed-tracing.md)
- [Monitoring get-batch](/docs/monitoring-get-batch.md)

## Performance

- [AIS load generator (`aisloader`)](/docs/aisloader.md)
- [Benchmarking AIStore](/docs/howto_benchmark.md)
- [Performance tuning and testing](/docs/performance.md)
- [Performance monitoring via CLI](/docs/cli/performance.md)
- [Rate limiting](/docs/rate_limit.md)
- [Checksumming](/docs/checksum.md)

## Reference

### APIs and SDKs

- [CLI reference guide](/docs/cli.md#cli-reference)
- [Go API](https://github.com/NVIDIA/aistore/tree/main/api)
- [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore)
- [PyPI package](https://pypi.org/project/aistore/)
- [Python SDK reference guide](https://docs.nvidia.com/aistore/python/aistore/sdk)
- [PyTorch integration](https://docs.nvidia.com/aistore/python/aistore/pytorch)
- [TensorFlow integration](/docs/tensorflow.md)
- [HTTP API reference](https://aistore.nvidia.com/docs/http-api)
- [curl examples](/docs/http_api.md)
- [Easy URL](/docs/easy_url.md)

### Compatibility

- [S3 compatibility](/docs/s3compat.md)
- [`s3cmd` quick start](/docs/s3compat.md#quick-start-with-s3cmd)
- [Presigned S3 requests](/docs/s3compat.md#presigned-s3-requests)
- [Boto3 support](https://docs.nvidia.com/aistore/python/aistore/botocore_patch)

### Security and protocols

- [Feature flags](/docs/feature_flags.md)
- [AuthN service and access control](/docs/authn.md)
- [Authentication validation](/docs/auth_validation.md)
- [MessagePack protocol](/docs/msgp.md)
- [Idle connections](/docs/idle_connections.md)

### Names, formats, and limits

- [Shard Index](/docs/shard_index.md)
- [Unicode and special symbols in object and bucket names](/docs/unicode.md)
- [Extremely long object names](/docs/long_names.md)

### Runtime and build

- [Build tags](/docs/build_tags.md)
- [`aisnode` command line](/docs/command_line.md)

## Troubleshooting

- [Troubleshooting](/docs/troubleshooting.md)

## Resources

- [Technical Blog](https://aistore.nvidia.com/blog)
- [Development guide](/docs/development.md)
- [Tools and utilities](https://github.com/NVIDIA/aistore/blob/main/cmd/README.md)
- [Extended actions (xactions)](https://github.com/NVIDIA/aistore/blob/main/xact/README.md)
- [ETL Python SDK examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/ais-etl)
- [Custom transformers](https://github.com/NVIDIA/ais-etl/tree/main/transformers)
- [ETL Python webserver SDK](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl/webserver/README.md)
- [ETL Go webserver package](https://github.com/NVIDIA/aistore/blob/main/ext/etl/webserver/README.md)
- [Initial sharding utility (`ishard`)](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md)
