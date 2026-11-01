**AIStore: High-Performance, Scalable Storage for AI Workloads**

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-v5.2-green.svg)

AIStore (AIS) is a lightweight distributed storage stack tailored for AI applications. It's an elastic cluster that can grow and shrink at runtime and can be ad-hoc deployed, with or without Kubernetes, anywhere from a single Linux machine to a bare-metal cluster of any size. Built from scratch, AIS provides linear scale-out, consistent performance, and a flexible deployment model.

AIS is a reliable storage cluster that can natively operate on both in-cluster and remote data, without treating either as a cache.

AIS consistently shows [balanced I/O distribution and linear scalability](https://aistore.nvidia.com/blog/2025/07/26/smooth-max-line-speed) across an arbitrary number of clustered nodes.

## Features

* ✅ **Multi-Cloud Access:** Seamlessly access and manage content across multiple [cloud backends](/docs/overview.md#at-a-glance) (including AWS S3, GCS, Azure, and OCI), with fast-tier performance, configurable redundancy, and namespace-aware bucket identity (same-name buckets can coexist across accounts, endpoints, and providers).
* ✅ **Deploy Anywhere:** AIS runs on Linux, on physical or virtual machines, with or without Kubernetes. See [deployment options](https://github.com/NVIDIA/aistore/blob/main/deploy/README.md).
* ✅ **High Availability:** Redundant control and data planes. Self-healing, end-to-end protection, n-way mirroring, and erasure coding. Arbitrary number of lightweight access points (AIS proxies).
* ✅ **HTTP-based API:** A feature-rich, native API (with user-friendly SDKs for Go and Python), and an [S3-compatible API](/docs/s3compat.md) for existing S3 tools and SDKs.
* ✅ **kTLS Transmit Offload:** Offload HTTPS record encryption to the Linux kernel or capable NICs, allowing eligible object GETs to use `sendfile(2)` and skip the copy through user space. Enabled by default for HTTPS targets in v5.2, with automatic fallback to user-space TLS. See [kTLS transmit offload](/docs/https.md#ktls-transmit-offload).
* ✅ **Monitoring:** Comprehensive observability with integrated Prometheus metrics, Grafana dashboards, detailed logs with configurable verbosity, and CLI-based performance tracking for complete cluster visibility and troubleshooting. See [AIStore Observability](/docs/monitoring-overview.md) for details.
* ✅ **Chunked Objects:** High-performance chunked object representation, with independently retrievable chunks and checksum-protected manifests. Supports rechunking, parallel reads, and seamless integration with [Get-Batch](/docs/get_batch.md), [blob-downloader](/docs/blob_downloader.md), and multipart uploads to supported cloud backends.
* ✅ **JWT Authentication and Authorization:** [Validates request JWTs](/docs/auth_validation.md) to provide cluster- and bucket-level access control using static keys or dynamic OIDC issuer JWKS lookup.
* ✅ **Intra-Cluster Request Signing:** Per-node Ed25519 identities sign and verify intra-cluster requests, signed redirects, and transport streams, with verifying keys distributed through the cluster map (v5.1+, when `auth.intra_cluster.request_auth` is enabled; see [authentication boundaries](/docs/auth_validation.md#authentication-boundaries)).
* ✅ **Load-Aware Throttling:** Dynamic request throttling based on a multi-dimensional load vector (CPU, memory, disk, file descriptors, goroutines) to protect AIS clusters under stress.
* ✅ **Unified Namespace:** Attach AIS clusters together to provide unified access to datasets across independent clusters, allowing users to reference shared buckets with cluster-specific identifiers.
* ✅ **Turn-key Cache:** Per-bucket configurable LRU-based cache with eviction thresholds and storage capacity watermarks.
* ✅ **ETL Offload:** Execute I/O intensive data transformations [close to the data](/docs/etl.md), either inline (on-the-fly as part of each read request) or offline (batch processing, with the destination bucket populated with transformed results). Requires an ETL-enabled build (`etl` build tag or `-etl` images).
* ✅ **Get-Batch:** Retrieve multiple objects and/or [archived files](/docs/archive.md) with a single call. Designed for ML/AI pipelines, [Get-Batch](/docs/get_batch.md) fetches an entire training batch in one operation, assembling a TAR (or other supported [serialization formats](/docs/archive.md)) that contains all requested items in the exact user-specified order ([paper](https://arxiv.org/abs/2602.22434)).
* ✅ **Data Consistency:** Guaranteed [consistency](/docs/terminology.md#read-after-write-consistency) across all gateways, with [write-through](/docs/terminology.md#write-through) semantics in presence of [remote backends](/docs/terminology.md#backend-provider).
* ✅ **Serialization & Sharding:** Native, first-class support for TAR, TGZ, TAR.LZ4, and ZIP [archives](/docs/archive.md) for efficient storage and processing of small-file datasets. Features include seamless integration with existing unmodified workflows across all APIs and subsystems.
* ✅ **Batch Jobs:** More than 30 [asynchronous jobs](/docs/batch.md) that you can monitor and manage. Most are user-started (copy, prefetch, archive, ETL, ...); others run on demand with regular traffic (EC, mirroring, list, Get-Batch) or in response to cluster events (rebalance, resilver). The list currently includes:

```console
$ ais show job --help

NAME:
    archive              blob-download       cleanup        copy-bucket   copy-objects      create-inventory
    delete-objects       download            dsort          ec-bucket     ec-get            ec-put
    ec-resp              elect-primary       etl-bucket     etl-inline    etl-objects       evict-objects
    evict-remote-bucket  get-batch           list           lru-eviction  mirror            prefetch-objects
    promote-files        put-copies          rebalance      rechunk       rename-bucket     resilver
    shard-index          shard-index-demand  shard-summary  summary       warm-up-metadata
```

> The feature set continues to grow and also includes: [shard index](/docs/shard_index.md); [placement weights](/docs/placement.md) (experimental); [native bucket inventory (NBI)](/docs/nbi.md); [blob-downloader](/docs/blob_downloader.md); [AuthN - authentication and authorization server](/docs/authn.md); runtime management of [TLS certificates](/docs/cli/x509.md); full support for [adding/removing nodes at runtime](/docs/lifecycle_node.md); adaptive [rate limiting](/docs/rate_limit.md); and more.

> For the original **white paper** and design philosophy, please see [AIStore Overview](/docs/overview.md), which also includes high-level block diagram, terminology, APIs, CLI, and more.
> For our 2024 KubeCon presentation, please see [AIStore: Enhancing petascale Deep Learning across Cloud backends](https://www.youtube.com/watch?v=N-d9cbROndg).

## Quick Start

Choose a deployment path:

* **Production:** [ais-k8s](https://github.com/NVIDIA/ais-k8s) - AIS K8s Operator, Helm charts, Ansible host playbooks, and deployment guides. Start with [prerequisites](https://github.com/NVIDIA/ais-k8s/blob/main/docs/prerequisites.md) and the [production deployment guide](https://github.com/NVIDIA/ais-k8s/blob/main/docs/README.md).
* [Local playground](/docs/getting_started.md#local-playground): Non-containerized, source-based development and evaluation on a workstation, laptop, or VM.
* [Local Kubernetes](/deploy/dev/k8s/README.md): KinD or Minikube development and testing, without the operator; not for production.
* [Minimal Compose](/docs/getting_started.md#minimal-compose-deployment): One-proxy, one-target container smoke test without TLS or AuthN; not for production.
* Multi-node development and testing: [Ansible](/deploy/dev/ansible/README.md), [Slurm](/deploy/dev/slurm/README.md), [Docker](/deploy/dev/docker/README.md).

For a summary of all options, see [deployment options](/deploy/README.md); for Kubernetes specifically, see [choose a Kubernetes deployment](/docs/kubernetes.md).

After starting your cluster, [create your first object with Python](/docs/python_sdk.md).

## CLI

AIS includes an integrated, scriptable [CLI](/docs/cli.md) for managing clusters, buckets, and objects, running and monitoring batch jobs, viewing and downloading logs, generating performance reports, and more:

```console
$ ais <TAB-TAB>

advanced         cluster          etl              ls               prefetch         search           tls
alias            config           evict            ml               put              show             wait
archive          cp               get              mpu              remote-cluster   space-cleanup
auth             create           help             nbi              rmb              start
blob-download    download         job              object           rmo              stop
bucket           dsort            log              performance      scrub            storage
```

## Developer Tools

AIS stores objects as plain files ([on-disk layout](/docs/on_disk_layout.md)) - thus, the freedom to copy or move your data at any time using familiar Linux `tar(1)`, `scp(1)`, `rsync(1)` and similar.

> Exceptions are chunked objects (chunks plus a manifest) and very long object names (hashed on disk).

For developers and data scientists, there's also:

* [Go API](https://github.com/NVIDIA/aistore/tree/main/api) used in [CLI](/docs/cli.md) and [benchmarking tools](/docs/aisloader.md)
* [Python first-object tutorial](/docs/python_sdk.md), [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk), and [Reference Guide](https://docs.nvidia.com/aistore/api-reference/python/aistore/sdk)
* [PyTorch integration](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch) and usage examples
* [Boto3 support](https://docs.nvidia.com/aistore/api-reference/python/aistore/botocore_patch)

## Existing Datasets

AIS supports multiple ingestion modes:

* ✅ **On Demand:** Transparent cloud access during workloads.
* ✅ **PUT:** Locally accessible files and directories.
* ✅ **Promote:** Import local target directories and/or NFS/SMB shares mounted on AIS targets.
* ✅ **Copy:** Full buckets, virtual subdirectories (recursively or non-recursively), lists or ranges (via Bash expansion).
* ✅ **Download:** HTTP(S)-accessible datasets and objects.
* ✅ **Prefetch:** Remote buckets or selected objects (from remote buckets), including subdirectories, lists, and/or ranges.
* ✅ **Archive:** [Group and store](https://aistore.nvidia.com/blog/2024/08/16/ishard) related small files from an original dataset.

## Install from Release Binaries

You can install the CLI and benchmarking tools using:

```console
./scripts/install_from_binaries.sh --help
```

The script installs [aisloader](/docs/aisloader.md) and [CLI](/docs/cli.md) from the latest or previous GitHub [release](https://github.com/NVIDIA/aistore/releases) and enables CLI auto-completions.

## PyTorch integration

PyTorch integration is a growing set of datasets (both iterable and map-style), samplers, and dataloaders:

* [Taxonomy of abstractions and API reference](https://docs.nvidia.com/aistore/api-reference/python/aistore/pytorch)
* [AIS plugin for PyTorch: usage examples](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch/README.md)
* [Jupyter notebook examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/pytorch/)

## AIStore Badge

Let others know your project is powered by high-performance AI storage:

[![aistore](https://img.shields.io/badge/powered%20by-AIStore-76B900?style=flat&labelColor=000000)](https://github.com/NVIDIA/aistore)

```markdown
[![aistore](https://img.shields.io/badge/powered%20by-AIStore-76B900?style=flat&labelColor=000000)](https://github.com/NVIDIA/aistore)
```

## More Docs & Guides

* [Overview and Design](/docs/overview.md)
* [Terminology and Core Abstractions](/docs/terminology.md)
* [Networking Model](/docs/networking.md)
* [Getting Started](/docs/getting_started.md)
* [First AIStore object with Python](/docs/python_sdk.md)
* [Kubernetes deployment chooser](/docs/kubernetes.md)
* [AIS Buckets: Design and Operations](/docs/bucket.md)
* [Observability](/docs/monitoring-overview.md)
* [Technical Blog](https://aistore.nvidia.com/blog)
* [S3 Compatibility](/docs/s3compat.md)
* [Batch Jobs](/docs/batch.md)
* [Performance](/docs/performance.md) and [CLI: performance](/docs/cli/performance.md)
* [Host Tuning Playbooks](https://github.com/NVIDIA/ais-k8s/tree/main/playbooks/host-config)
* [CLI Reference](/docs/cli.md)
* [Authentication](/docs/authn.md)
* [Production Deployment: Kubernetes Operator, Ansible Playbooks, Helm Charts, Monitoring](https://github.com/NVIDIA/ais-k8s)

### How to find information

* See [Extended Index](/docs/docs.md)
* Use CLI `search` command, e.g.: `ais search copy`
* Clone the repository and run `git grep`, e.g.: `git grep -n out-of-band -- "*.md"`

## License

MIT

## Author

Alex Aizman (NVIDIA)
