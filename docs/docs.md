---
layout: post
title: DOCUMENTATION
permalink: /docs
redirect_from:
 - /docs.md/
 - /docs/docs.md/
---

## Introduction

- [Main README](/README.md)
- [In-depth Overview](/docs/overview.md)
- [Getting Started](/docs/getting_started.md)
- [CLI: overview](/docs/cli.md)
- [CLI: reference guide](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md#cli-reference)
- [Technical Blog](https://aistore.nvidia.com/blog)
- [How to deploy AIStore (useful links)](https://github.com/NVIDIA/aistore/blob/main/deploy/README.md)

## API and SDKs

- [Go (language) API](https://github.com/NVIDIA/aistore/tree/main/api)
- [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore), and also:
  - [pip package](https://pypi.org/project/aistore/)
  - [reference guide](/docs/python_sdk.md)
- [REST API](/docs/http_api.md)
  - [Easy URL](https://github.com/NVIDIA/aistore/blob/main/docs/easy_url.md)
- Amazon S3
  - [`s3cmd` client](/docs/s3cmd.md)
  - [S3 compatibility](/docs/s3compat.md)
  - [Presigned S3 requests](/docs/s3compat.md#presigned-s3-requests)
  - [Boto3 support](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch)

## CLI

- [Overview](/docs/cli.md)
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

## Storage Management

- [Storage Services](/docs/storage_svcs.md)
- [Buckets and Bucket Management](/docs/bucket.md)
- [CLI: Operations on Buckets](/docs/cli/bucket.md)
- [CLI: `ais show storage` and subcommands](/docs/cli/show.md)
- [CLI: `ais storage` and subcommands](/docs/cli/storage.md)
- [On-disk layout](/docs/on_disk_layout.md)
- [CLI: Three Ways to Evict Remote Bucket](https://github.com/NVIDIA/aistore/blob/main/docs/cli/evicting_buckets_andor_data.md)
- [Backend Providers](/docs/providers.md)
- [Virtual directories](https://github.com/NVIDIA/aistore/blob/main/docs/howto_virt_dirs.md)
- [System files](/docs/sysfiles.md)

## Cluster Administration

- [Node lifecycle: maintenance mode, rebalance/rebuild, shutdown, decommission](/docs/lifecycle_node.md)
- [CLI: `ais cluster` and subcommands](/docs/cli/show.md)
- [Joining AIS cluster](/docs/join_cluster.md)
- [Leaving AIS cluster](/docs/leave_cluster.md)
- [Global Rebalance](/docs/rebalance.md)
- [Highly available control plane](/docs/ha.md)
- [Start/stop maintenance mode, shutdown, decommission, and related operations](/docs/lifecycle_node.md)
- [Out-of-band updates](/docs/out_of_band.md)
- [Troubleshooting](/docs/troubleshooting.md)

## Configuration and Security

- [Configuration](/docs/configuration.md)
- [Environment variables](/docs/environment-vars.md)
- [CLI: `ais config`](/docs/cli/config.md)
- [Feature flags](/docs/feature_flags.md)
- [Security and Access Control](/docs/authn.md)
  - [Authentication Server (AuthN)](/docs/authn.md)
- [HTTPS: loading, reloading, and generating certificates; switching cluster between HTTP and HTTPS](/docs/https.md)
  - [Managing TLS Certificates](/docs/cli/x509.md)

## ETL - Extract, Transform, Load

- [AIStore ETL Overview](/docs/etl.md)
- [ETL Transformer Examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/ais-etl)
- [ETL Python Webserver SDK](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl/webserver/README.md)
- [ETL Golang Webserver Package](https://github.com/NVIDIA/aistore/blob/main/ext/etl/webserver/README.md)

## Advanced Features and Tools

- [Reading, writing, and listing *archives*](/docs/archive.md)
- [Distributed Shuffle (`dsort`)](/docs/dsort.md)
- [Initial Sharding utility (`ishard`)](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md)
- [Downloader](/docs/downloader.md)
- [Tools and utilities](/docs/tools.md)
- [Batch operations](/docs/batch.md)
- [eXtended Actions (xactions)](https://github.com/NVIDIA/aistore/blob/main/xact/README.md)
- [CLI: `ais job`](/docs/cli/job.md) and [`ais show job`](/docs/cli/show.md), including:
  - [prefetch remote datasets](/docs/cli/object.md#prefetch-objects)
  - [copy (list, range, and/or prefix) selected objects or entire (in-cluster or remote) buckets](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)
  - [download remote BLOBs](/docs/cli/blob-downloader.md)
  - [promote NFS or SMB share](https://aistore.nvidia.com/blog/2022/03/17/promote)

## Performance and Resilience

- [AIS Load Generator: integrated benchmark tool](/docs/aisloader.md)
- [How to benchmark](/docs/howto_benchmark.md)
- [Performance tuning and testing](/docs/performance.md)
- [Performance monitoring](/docs/cli/performance.md)
- [Using Rate Limits to Enhance Reliability and Performance](https://aistore.nvidia.com/blog/2025/03/19/rate-limit-blog)
- [Checksumming: brief theory of operations](/docs/checksum.md)
- [Blog: Maintaining Resilient Connectivity During Lifecycle Events](https://aistore.nvidia.com/blog/2025/04/02/python-retry)

## Observability and Monitoring

- [Prometheus & Metrics](/docs/metrics.md)
- [Prometheus](/docs/prometheus.md)
- [Reference: all supported metrics](/docs/metrics-reference.md)
- [CLI: `ais performance` command](/docs/cli/performance.md)

## Developer Resources

- [Getting Started](/docs/getting_started.md)
- [How to deploy AIStore (useful links)](https://github.com/NVIDIA/aistore/blob/main/deploy/README.md)
- [Docker](/docs/docker_main.md)
- [Assorted scripts; profiling](/docs/development.md)
- [`aisnode` command line](/docs/command_line.md)

## Production Deployment

- [AIStore on Kubernetes](https://github.com/NVIDIA/ais-k8s)
- [Kubernetes Operator](https://github.com/NVIDIA/ais-k8s/blob/main/operator/README.md)
- [Ansible Playbooks](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/README.md)
- [Helm Charts](https://github.com/NVIDIA/ais-k8s/tree/main/helm)
- [Monitoring](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/README.md)

## Special Cases and Advanced Topics

- [Unicode and Special Symbols in Object Names](https://github.com/NVIDIA/aistore/blob/main/docs/unicode.md)
- [Extremely Long Object Names](https://github.com/NVIDIA/aistore/blob/main/docs/long_names.md)
- [Blog: Split-brain is Inevitable](https://aistore.nvidia.com/blog/2025/02/16/split-brain-blog)
- [Blog: Comparing OCI's Native Object Storage and S3 API Backends](https://aistore.nvidia.com/blog/2025/02/26/oci-object-native-vs-s3-api)
- [Blog: AIStore Python SDK: Maintaining Resilient Connectivity During Lifecycle Events](https://aistore.nvidia.com/blog/2025/04/02/python-retry)
- [Blog: Adding Data to AIStore -- PUT Performance](https://aistore.nvidia.com/blog/2024/11/22/put-performance)
- [Traffic patterns](/docs/traffic_patterns.md)
