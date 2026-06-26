---
layout: post
title: DOCUMENTATION
description: "Complete AIStore documentation: architecture, CLI, Python SDK, HTTP API, ETL, deployment, and operations guides."
permalink: /docs
redirect_from:
 - /docs.md/
 - /docs/docs.md/
---

AIStore documentation is organized with the NVIDIA Template Library information
architecture: About, Get Started, Use AIStore, Operations, Reference,
Troubleshooting, and Resources. During the Fern migration, this page is the
source for the generated Fern documentation navigation.

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

## Release Notes

- [AIStore 5.0 draft release notes](/docs/relnotes/5.0-draft.md)
- [AIStore 4.7 release notes](/docs/relnotes/4.7.md)
- [AIStore 4.6 release notes](/docs/relnotes/4.6.md)
- [AIStore 4.5 release notes](/docs/relnotes/4.5.md)
- [AIStore 4.4 release notes](/docs/relnotes/4.4.md)
- [AIStore 4.3 release notes](/docs/relnotes/4.3.md)
- [AIStore 4.2 release notes](/docs/relnotes/4.2.md)
- [AIStore 4.1 release notes](/docs/relnotes/4.1.md)
- [AIStore 4.0 release notes](/docs/relnotes/4.0.md)
- [AIStore 3.30 release notes](/docs/relnotes/3.30.md)

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
- [Bucket operations](/docs/cli/bucket.md)
- [Cluster and remote-cluster management](/docs/cli/cluster.md)
- [Storage and mountpath management](/docs/cli/storage.md)
- [Downloads](/docs/cli/download.md)
- [Jobs](/docs/cli/job.md)
- [ETL overview](/docs/etl.md)
- [ETL CLI docs](/docs/cli/etl.md)
- [Archives: read, write, and list](/docs/archive.md)
- [Distributed shuffle (`dsort`)](/docs/dsort.md)
- [Downloader](/docs/downloader.md)
- [Blob Downloader](/docs/blob_downloader.md)
- [Batch object retrieval (get-batch)](/docs/get_batch.md)
- [Batch operations](/docs/batch.md)
- [Virtual directories](/docs/howto_virt_dirs.md)

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

## Observability and Performance

- [Observability overview](/docs/monitoring-overview.md)
- [Monitoring with CLI](/docs/monitoring-cli.md)
- [Logs](/docs/monitoring-logs.md)
- [Prometheus integration](/docs/monitoring-prometheus.md)
- [Metrics reference](/docs/monitoring-metrics.md)
- [Grafana dashboards](/docs/monitoring-grafana.md)
- [Kubernetes monitoring](/docs/monitoring-kubernetes.md)
- [Distributed tracing](/docs/distributed-tracing.md)
- [Monitoring get-batch](/docs/monitoring-get-batch.md)
- [AIS load generator (`aisloader`)](/docs/aisloader.md)
- [Benchmarking AIStore](/docs/howto_benchmark.md)
- [Performance tuning and testing](/docs/performance.md)
- [Performance monitoring via CLI](/docs/cli/performance.md)
- [Rate limiting](/docs/rate_limit.md)
- [Checksumming](/docs/checksum.md)
- [Filesystem Health Checker (FSHC)](/docs/fshc.md)

## Reference

- [`ais help`](/docs/cli/help.md)
- [CLI reference guide](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md#cli-reference)
- [Go API](https://github.com/NVIDIA/aistore/tree/main/api)
- [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore)
- [PyPI package](https://pypi.org/project/aistore/)
- [Python SDK reference guide](https://docs.nvidia.com/aistore/python/aistore/sdk)
- [PyTorch integration](https://docs.nvidia.com/aistore/python/aistore/pytorch)
- [TensorFlow integration](/docs/tensorflow.md)
- [HTTP API reference](https://aistore.nvidia.com/docs/http-api)
- [curl examples](/docs/http_api.md)
- [Easy URL](/docs/easy_url.md)
- [S3 compatibility](/docs/s3compat.md)
- [`s3cmd` quick start](/docs/s3compat.md#quick-start-with-s3cmd)
- [Presigned S3 requests](/docs/s3compat.md#presigned-s3-requests)
- [Boto3 support](https://docs.nvidia.com/aistore/python/aistore/botocore_patch)
- [Authentication and access control](/docs/cli/auth.md)
- [Configuration via CLI](/docs/cli/config.md)
- [GCP credentials](/docs/cli/gcp_creds.md)
- [TLS certificate management](/docs/cli/x509.md)
- [Feature flags](/docs/feature_flags.md)
- [AuthN and access control](/docs/authn.md)
- [Authentication validation](/docs/auth_validation.md)
- [MessagePack protocol](/docs/msgp.md)
- [Idle connections](/docs/idle_connections.md)
- [Shard Index](/docs/shard_index.md)
- [Unicode and special symbols in object and bucket names](/docs/unicode.md)
- [Extremely long object names](/docs/long_names.md)
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
