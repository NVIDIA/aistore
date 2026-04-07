---
layout: post
title: DOCUMENTATION
description: "Complete AIStore documentation: architecture, CLI, Python SDK, HTTP API, ETL, deployment, and operations guides."
permalink: /docs
redirect_from:
 - /docs.md/
 - /docs/docs.md/
---

AIStore documentation is organized by task: getting started, accessing AIS via CLI/API/SDK, managing storage and clusters, deploying in production, and exploring advanced features.

## Core Documentation

- [Main README](/README.md)
- [In-depth Overview](/docs/overview.md)
- [Terminology and core abstractions](/docs/terminology.md)
- [Getting Started](/docs/getting_started.md)
- [Networking model](/docs/networking.md)
- [Buckets: design, operations, namespaces, and system buckets](/docs/bucket.md)
- [Observability overview](/docs/monitoring-overview.md)
- [CLI overview](/docs/cli.md)
- [Production deployment](https://github.com/NVIDIA/aistore/blob/main/deploy/README.md)
- [Technical Blog](https://aistore.nvidia.com/blog)

## APIs, SDKs, and Compatibility

- [Go API](https://github.com/NVIDIA/aistore/tree/main/api)
- [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore)
  - [PyPI package](https://pypi.org/project/aistore/)
  - [Python SDK reference guide](/docs/python_sdk.md)
- [PyTorch integration](/docs/pytorch.md)
- [TensorFlow integration](/docs/tensorflow.md)
- [HTTP API reference](https://aistore.nvidia.com/docs/http-api)
  - [curl examples](/docs/http_api.md)
  - [Easy URL](/docs/easy_url.md)
- [S3 compatibility](/docs/s3compat.md)
  - [`s3cmd` quick start](/docs/s3compat.md#quick-start-with-s3cmd)
  - [Presigned S3 requests](/docs/s3compat.md#presigned-s3-requests)
  - [Boto3 support](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch)

## Command-Line Interface

- [CLI overview](/docs/cli.md)
- [`ais help`](/docs/cli/help.md)
- [CLI reference guide](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md#cli-reference)
- [Bucket operations](/docs/cli/bucket.md)
- [Cluster and remote-cluster management](/docs/cli/cluster.md)
- [Storage and mountpath management](/docs/cli/storage.md)
- [Monitoring and `ais show`](/docs/cli/show.md)
- [Downloads](/docs/cli/download.md)
- [Jobs](/docs/cli/job.md)
- [Authentication and access control](/docs/cli/auth.md)
- [Configuration via CLI](/docs/cli/config.md)
- [ETL CLI](/docs/cli/etl.md)
- [Distributed shuffle CLI](/docs/cli/dsort.md)
- [ML / get-batch CLI](/docs/cli/ml.md)
- [GCP credentials](/docs/cli/gcp_creds.md)
- [TLS certificate management](/docs/cli/x509.md)

## Storage and Data Management

- [Storage services](/docs/storage_svcs.md)
- [Buckets: design, operations, namespaces, and system buckets](/docs/bucket.md)
- [Native Bucket Inventory (NBI)](/docs/nbi.md)
- [Backend providers](/docs/providers.md)
- [On-disk layout](/docs/on_disk_layout.md)
- [Virtual directories](/docs/howto_virt_dirs.md)
- [System files](/docs/sysfiles.md)
- [Evicting remote buckets and cached data](/docs/cli/evicting_buckets_andor_data.md)

## Cluster Operations

- [Node lifecycle: maintenance, shutdown, decommission](/docs/lifecycle_node.md)
- [Global rebalance](/docs/rebalance.md)
- [Resilver](/docs/resilver.md)
- [AIS in Containerized Environments](/docs/containerized.md)
- [Highly available control plane](/docs/ha.md)
- [Information Center (IC)](/docs/ic.md)
- [Out-of-band updates](/docs/out_of_band.md)
- [Troubleshooting](/docs/troubleshooting.md)

## Configuration and Security

- [Configuration](/docs/configuration.md)
- [Environment variables](/docs/environment-vars.md)
- [Feature flags](/docs/feature_flags.md)
- [AuthN and access control](/docs/authn.md)
- [Authentication validation](/docs/auth_validation.md)
- [HTTPS and certificates](/docs/https.md)
  - [Switching a cluster to HTTPS](/docs/switch_https.md)

## ETL and Advanced Workflows

- [ETL overview](/docs/etl.md)
- [ETL CLI docs](/docs/cli/etl.md)
- [ETL Python SDK examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/ais-etl)
- [Custom transformers](https://github.com/NVIDIA/ais-etl/tree/main/transformers)
- [ETL Python webserver SDK](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl/webserver/README.md)
- [ETL Go webserver package](https://github.com/NVIDIA/aistore/blob/main/ext/etl/webserver/README.md)
- [Archives: read, write, and list](/docs/archive.md)
- [Distributed shuffle (`dsort`)](/docs/dsort.md)
- [Initial sharding utility (`ishard`)](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md)
- [Downloader](/docs/downloader.md)
- [Blob Downloader](/docs/blob_downloader.md)
- [Batch object retrieval (get-batch)](/docs/get_batch.md)
- [Batch operations](/docs/batch.md)
- [Tools and utilities](https://github.com/NVIDIA/aistore/blob/main/cmd/README.md)
- [Extended actions (xactions)](https://github.com/NVIDIA/aistore/blob/main/xact/README.md)

## Observability, Monitoring, and Performance

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
- [Traffic patterns](/docs/traffic_patterns.md)

## Networking

- [Networking: multi-homing, network separation, IPv6](/docs/networking.md)
- [HTTPS configuration](/docs/https.md)
- [Switching to HTTPS](/docs/switch_https.md)
- [Idle connections](/docs/idle_connections.md)
- [MessagePack protocol](/docs/msgp.md)

## Deployment

- [AIStore on Kubernetes](https://github.com/NVIDIA/ais-k8s)
- [Kubernetes Operator](https://github.com/NVIDIA/ais-k8s/blob/main/operator/README.md)
- [Ansible playbooks](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/README.md)
- [Helm charts](https://github.com/NVIDIA/ais-k8s/tree/main/helm)
- [Deployment monitoring](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/README.md)
- [Docker](/docs/docker_main.md)

## Developer Resources

- [Development guide](/docs/development.md)
- [`aisnode` command line](/docs/command_line.md)
- [Build tags](/docs/build_tags.md)

## Object and Bucket Naming

- [Unicode and special symbols in object and bucket names](/docs/unicode.md)
- [Extremely long object names](/docs/long_names.md)
