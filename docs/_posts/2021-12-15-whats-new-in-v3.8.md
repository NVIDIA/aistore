---
layout: post
title:  "What's new in AIS v3.8"
date:   Dec 15, 2021
author: Alex Aizman
categories: aistore
---

AIStore v3.8 is a significant upgrade delivering [long-awaited features, stabilization fixes, and performance improvements](https://github.com/NVIDIA/aistore/releases/tag/3.8). There's also the cumulative effect of continuous functional and stress testing combined with (continuous) refactoring to optimize and reinforce the codebase.

In other words, a certain achieved *milestone* that includes:

## ETL

AIS-ETL is designed around the idea to run custom *transforming* containers directly on AIS target nodes. Typical flow includes the following steps:

1. User initiates ETL workload by executing one of the documented API calls
   and providing either the corresponding docker image or a *transforming function* (e.g. Python script);
2. AIS gateway coordinates the deployment of ETL containers (aka K8s pods) on AIS targets: one container per target;
3. Each target creates a local `communicator` instance for the specified `communication type`.

Prior to 3.8, [supported communication types](/docs/etl.md) were all HTTP-based. For instance, existing ["hpull://"](/docs/etl.md#communication-mechanisms) facilitates HTTP-redirect type communication with AIS target redirecting original read requests to the local ETL container. Version 3.8 adds a non-HTTP communicator (denoted as "io://") and removes the requirement to wrap your custom transforming logic into some sort of HTTP processing.

The new "io://" communicator acts as a simple executor of external commands *by* the ETL container. On its end, AIS target resorts to capturing resulting standard output (containing transformed bytes) and standard error. This is maybe not the most performant solution but certainly the easiest one to implement.

Additionally, v3.8 integrates ETL (jobs) with [xactions](/docs/batch.md) thus providing consistency in terms of starting/stopping and managing/monitoring. All existing APIs and [CLIs](/docs/cli/job.md) that are common for all [xactions](/docs/batch.md) are supported out of the box.

Finally, v3.8 introduces persistent ETL metadata as a new replicated-versioned-and-protected metadata type. The implementation leverages existing mechanism to keep clustered nodes in-sync with added, removed, and updated ETL specifications. The ultimate objective is to be able to run an arbitrary mix of inline and offline ETLs while simultaneously viewing and *editing* their (persistent) specs.

Further reading:
- [Using AIS/PyTorch connector to transform ImageNet](https://aiatscale.org/blog/2021/10/22/ais-etl-2)
- [Using WebDataset to train on a sharded dataset](https://aiatscale.org/blog/2021/10/29/ais-etl-3)

## Storage cleanup

Cleanup, as the name implies, is tasked with safely removing already deleted objects (that we keep for a while to support future [undeletion](https://en.wikipedia.org/wiki/Undeletion)). Subject to being cleaned up also are:

* workfiles resulting from interrupted workloads
* unfinished erasure-coded slices
* misplaced replicas left behind during global rebalancing

and similar. In short, all sorts of "artifacts" of distributed migration, replication, and erasure coding.

Like LRU-based cluster-wide eviction, cleanup runs automatically or [administratively](/docs/cli/storage.md). Cleanup triggers automatically when the system exceeds 65% (or configured) of total used capacity. But note:

> Automatic cleanup always runs _prior_ to automatic LRU eviction, so that the latter would take into account updated used and available percentages.

> LRU eviction is separately configured on a per-bucket basis with cluster-wide inheritable defaults set as follows: enabled for Cloud buckets, disabled for AIS buckets that have no remote backend.

## Custom object metadata

AIS now differentiates between:

* its own system metadata (size, access time, checksum, number of copies, etc.)
* Cloud object metadata (source, version, MD5, ETag), and
* custom metadata comprising user-defined key/values

All metadata from all sources is now preserved and checksum-protected, stored persistently and maintained across all intra-cluster migrations and replications. There's also an improved check for local <=> remote equality in the context of cold GETs and [downloads](/docs/downloader.md) - the check that takes into account size, version (if available), ETag (if available), and checksum(s) - all of the above.

## Volume

Multi-disk volume in AIS is a collection of [mountpaths](/docs/overview.md#terminology). The corresponding metadata (called VMD) is versioned, persistent, and protected (i.e., checksummed and replicated). Version 3.8 reinforces ais volume (function) in presence of unlikely but nevertheless critical *scenarios* that include the usual:

* faulted drives, degraded drives, missing (unmounted or detached) drives
* old, missing, or corrupted VMD instances

At startup, AIS target performs mini-bootstrapping sequence to load and cross-check VMD against other its stored replicas and persistent configuration, both. At runtime, there's a revised, amended, and fully-supported capability to gracefully detach and attach mountpaths.

In fact, any mountpath can be temporarily disabled and (re)enabled, permanently detached and later re-attached. As long as there's enough space on the remaining mountpaths to carry out volume resilvering all the 4 (four) verbs can be used at any time.

> Needless to say, it'd make sense _not_ to power cycle the target during resilvering.

## Easy URL

The feature codenamed "easy URL" is a simple alternative mapping of the AIS API to handle URLs paths that look as follows:

| URL Path | Cloud |
| --- | --- |
| /gs/mybucket/myobject | Google Cloud buckets |
| /az/mybucket/myobject | Azure Blob Storage |
| /ais/mybucket/myobject | AIS |

In other words, easy URL is a convenience that allows reading, writing, deleting, and listing as follows:

```console
# Example: GET
$ curl -L -X GET 'http://aistore/gs/my-google-bucket/abc-train-0001.tar'

# Example: PUT
$ curl -L -X PUT 'http://aistore/gs/my-google-bucket/abc-train-9999.tar -T /tmp/9999.tar'

# Example: LIST
$ curl -L -X GET 'http://aistore/gs/my-google-bucket'
```

Note, however:

> There's a reason that Amazon S3 is missing in the list (above) that includes GCP and Azure. That's because AIS provides full [S3 compatibility](/docs/s3compat.md) layer via its "/s3" endpoint. [S3 compatibility](/docs/s3compat.md) shall not be confused with a simple alternative ("easy URL") mapping of HTTP requests.


## TL;DR

Other v3.8 additions include:

- target *standby* mode !4688, !4689, !4691
- amended and improved performance monitoring !4792, !4793, !4794, !4798, !4800, !4810, !4812
- ais targets with no disks !4825
- Kubernetes Operator [v0.9](https://github.com/NVIDIA/ais-k8s/releases/tag/v0.9)
- and more.

Some of those might be described later in a separate posting.
