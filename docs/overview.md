---
layout: post
title: OVERVIEW
permalink: /docs/overview
redirect_from:
 - /overview.md/
 - /docs/overview.md/
---

## Introduction

Training deep learning (DL) models on petascale datasets is essential for achieving competitive and state-of-the-art performance in applications such as speech, video analytics, and object recognition. However, existing distributed filesystems were not developed for the access patterns and usability requirements of DL jobs.

In this [white paper](https://arxiv.org/abs/2001.01858) we describe AIStore (AIS) and components, and then compare system performance experimentally using image classification workloads and storing training data on a variety of backends.

See also:

* [blog](https://aistore.nvidia.com/blog)
* [white paper](https://arxiv.org/abs/2001.01858)
* [at-a-glance poster](https://storagetarget.files.wordpress.com/2019/12/deep-learning-large-scale-phys-poster-1.pdf)

The rest of this document is structured as follows:

- [At a glance](#at-a-glance)
- [Terminology](#terminology)
- [Design Philosophy](#design-philosophy)
- [Key Concepts and Diagrams](#key-concepts-and-diagrams)
- [AIStore API](#aistore-api)
- [Traffic Patterns](#traffic-patterns)
- [Read-after-write consistency](#read-after-write-consistency)
- [Open Format](#open-format)
- [Existing Datasets](#existing-datasets)
- [Data Protection](#data-protection)
  - [Erasure Coding vs IO Performance](#erasure-coding-vs-io-performance)
- [Scale-Out](#scale-out)
- [Networking](#networking)
- [HA](#ha)
- [Other Services](#other-services)
- [dSort](#dsort)
- [CLI](#cli)
- [ETL](#etl)
- [_No limitations_ principle](#no-limitations-principle)

## At a glance

Following is a high-level **block diagram** with an emphasis on supported frontend and backend APIs, and the capability to scale-out horizontally. The picture also tries to make the point that AIS aggregates an arbitrary numbers of storage servers ("targets") with local or locally accessible drives, whereby each drive is formatted with a local filesystem of choice (e.g., xfs or zfs).

![At-a-Glance](images/cluster-block-v3.26.png)

In any aistore cluster, there are **two kinds of nodes**: proxies (a.k.a. gateways) and storage nodes (targets):

![Proxies and Targets](images/proxy-target-block-2024.png)

Proxies provide access points ("endpoints") for the frontend API. At any point in time there is a single **primary** proxy that also controls versioning and distribution of the current cluster map. When and if the primary fails, another proxy is majority-elected to perform the (primary) role.

All user data is equally distributed (or [balanced](/docs/rebalance.md)) across all storage nodes ("targets"). Which, combined with zero (I/O routing and metadata processing) overhead, provides for linear scale with no limitation on the total number of aggregated storage drives.

## Terminology

* **Target** - a storage node. To store user data, targets utilize **mountpaths** (see next). In the docs and the code, instead of saying something like "storage node in an aistore cluster" we simply say: "target."

* **Proxy** - a **gateway** providing [API](#aistore-api) access point. Proxies are diskless - they do not have direct access to user data, and do not "see" user data in-flight. One of the proxies is elected, or designated, as the _primary_ (or leader) of the cluster. There may be any number of ais proxies/gateways (but only one _primary_ at any given time).

> AIS proxy/gateway implements RESTful APIs, both [native](#aistore-api) and S3 compatible. Upon _primary_ failure, remaining proxies collaborate with each other to perform majority-voted HA failover. The terms "proxy" and "gateway" are used interchangeably.

> In AIS cluster, there is no correlation between the numbers of proxies and targets, although for symmetry we usually deploy one proxy for each target (storage) node.

* [Mountpath](configuration.md) - a single disk **or** a volume (a RAID) formatted with a local filesystem of choice, **and** a local directory that AIS can fully own and utilize (to store user data and system metadata). Note that any given disk (or RAID) can have (at most) one mountpath - meaning **no disk sharing**. Secondly, mountpath directories cannot be nested. Further:
   - a mountpath can be temporarily disabled and (re)enabled;
   - a mountpath can also be detached and (re)attached, thus effectively supporting growth and "shrinkage" of local capacity;
   - it is safe to execute the 4 listed operations (enable, disable, attach, detach) at any point during runtime;
   - in a typical deployment, the total number of mountpaths would compute as a direct product of (number of storage targets) x (number of disks in each target).

* [Backend Provider](providers.md) - an abstraction, and simultaneously an API-supported option, that allows to delineate between "remote" and "local" buckets with respect to a given AIS cluster.

* [Unified Global Namespace](providers.md) - AIS clusters *attached* to each other, effectively, form a super-cluster providing unified global namespace whereby all buckets and all objects of all included clusters are uniformly accessible via any and all individual access points (of those clusters).

* [Xaction](https://github.com/NVIDIA/aistore/blob/main/xact/README.md) - asynchronous batch operations that may take many seconds (minutes, hours, etc.) to execute - are called *eXtended actions* or simply *xactions*. CLI and [CLI documentation](/docs/cli) refers to such operations as **jobs** - the more familiar term that can be used interchangeably. Examples include erasure coding or n-way mirroring a dataset, resharding and reshuffling a dataset, archiving multiple objects, copying buckets, and many more. All [eXtended actions](https://github.com/NVIDIA/aistore/blob/main/xact/README.md) support generic [API](/api/xaction.go) and [CLI](/docs/cli/job.md#show-job-statistics) to show both common counters (byte and object numbers) as well as operation-specific extended statistics.

## Design Philosophy

It is often more optimal to let applications control how and whether the stored content is stored in chunks. That's the simple truth that holds, in particular, for AI datasets that are often pre-sharded with content and boundaries of those shards based on application-specific optimization criteria. More exactly, the datasets could be pre-sharded, post-sharded, and otherwise transformed to facilitate training, inference, and simulation by the AI apps.

The corollary of this statement is two-fold:

- Breaking objects into pieces (often called chunks but also slices, segments, fragments, and blocks) and the related functionality does not necessarily belong to an AI-optimized storage system per se;
- Instead of chunking the objects and then reassembling them with the help of cluster-wide metadata (that must be maintained with extreme care), the storage system could alternatively focus on providing assistance to simplify and accelerate dataset transformations.

Notice that the same exact approach works for the other side of the spectrum - the proverbial [small-file problem](https://www.quora.com/What-is-the-small-file-problem-in-Hadoop). Here again, instead of optimizing small-size IOPS, we focus on application-specific (re)sharding, whereby each shard would have a desirable size, contain a batch of the original (small) files, and where the files (aka samples) would be sorted to optimizes subsequent computation.

## Key Concepts and Diagrams

In this section: high-level diagrams that introduce key concepts and architecture, as well as possible deployment options.

AIS cluster *comprises* arbitrary (and not necessarily equal) numbers of **gateways** and **storage targets**. Targets utilize local disks while gateways are HTTP **proxies** that provide most of the control plane and never touch the data.

> The terms *gateway* and *proxy* are used interchangeably throughout this README and other sources in the repository.

Both **gateways** and **targets** are userspace daemons that join (and, by joining, form) a storage cluster at their respective startup times, or upon user request. AIStore can be deployed on any commodity hardware with pretty much any Linux distribution (although we do recommend 4.x kernel). There are no designed-in size/scale type limitations. There are no dependencies on special hardware capabilities. The code itself is free, open, and MIT-licensed.

The diagram depicting AIS clustered node follows below, and makes the point that gateways and storage targets can be colocated in a single machine (or a VM) but not necessarily:

![One AIS machine](images/ais-host-20-block.png)

AIS can be deployed as a self-contained standalone persistent storage cluster or a fast tier in front of any of the supported backends including Amazon S3 and Google Cloud (GCP). The built-in caching mechanism provides LRU replacement policy on a per-bucket basis while taking into account configurable high and low capacity watermarks (see [LRU](storage_svcs.md#lru) for details). AWS/GCP integration is *turnkey* and boils down to provisioning AIS targets with credentials to access Cloud-based buckets.

If (compute + storage) rack is a *unit of deployment*, it may as well look as follows:

![One rack](images/ais-rack-20-block.png)

Finally, AIS target provides a number of storage services with [S3-like RESTful API](http_api.md) on top and a MapReduce layer that we call [dSort](#dsort).

![AIS target block diagram](images/ais-target-20-block.png)

## AIStore API

In addition to industry-standard [S3](/docs/s3compat.md), AIS provides its own (value-added) native API that can be (conveniently) called directly from Go and Python programs:

- [Go API](https://github.com/NVIDIA/aistore/tree/main/api)
- [Python API](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk)
- [HTTP REST](/docs/http_api.md)

For Amazon S3 compatibility and related topics, see also:
  - [`s3cmd` client](/docs/s3cmd.md)
  - [S3 compatibility](/docs/s3compat.md)
  - [Presigned S3 requests](/docs/s3compat.md#presigned-s3-requests)
  - [Boto3 support](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch)

## Traffic Patterns

In AIS, all inter- and intra-cluster networking is based on HTTP/1.1 (with HTTP/2 option currently under development).
HTTP(S) clients execute RESTful operations vis-à-vis AIS gateways and data then moves **directly** between the clients and storage targets with no metadata servers and no extra processing in-between:

![Datapath schematics](images/dp-schematics-direct.png)

> MDS in the diagram above stands for the metadata server(s) or service(s).

In the picture, a client on the left side makes an I/O request which is then fully serviced by the *left* target - one of the nodes in the AIS cluster (not shown).
Symmetrically, the *right* client engages with the *right* AIS target for its own GET or PUT object transaction.
In each case, the entire transaction is executed via a single TCP session that connects the requesting client directly to one of the clustered nodes.
As far as the datapath is concerned, there are no extra hops in the line of communications.

> For detailed traffic patterns diagrams, please refer to [this readme](traffic_patterns.md).

Distribution of objects across AIS cluster is done via (lightning fast) two-dimensional consistent-hash whereby objects get distributed across all storage targets and, within each target, all local disks.

## Read-after-write consistency

`PUT(object)` is a transaction. New object (or new version of the object) becomes visible/accessible only when aistore finishes writing the first replica and its metadata.

For S3 or any other remote [backend](/docs/providers.md), the latter includes:

* remote PUT via vendor's SDK library;
* local write under a temp name;
* getting successful remote response that carries remote metadata;
* simultaneously, computing checksum (per bucket config);
* optionally, checksum validation, if configured;
* finally, writing combined object metadata, at which point the object becomes visible and accessible.

But _not_ prior to that point!

If configured, additional copies and EC slices are added asynchronously. E.g., given a bucket with 3-way replication you may already read the first replica when the other two (copies) are still pending.

It is worth emphasizing that the same rules of data protection and consistency are universally enforced across the board for all _data writing_ scenarios, including (but not limited to):

* RESTful PUT (above);
* cold GET (as in: `ais get s3://abc/xyz /dev/null` when S3 has `abc/xyz` while aistore doesn't);
* copy bucket; transform bucket;
* multi-object copy; multi-object transform; multi-object archive;
* prefetch remote bucket;
* download very large remote objects (blobs);
* rename bucket;
* promote NFS share

and more.

## Open Format

AIS targets utilize local Linux filesystems including (but not limited to) xfs, ext4, and openzfs. User data is checksummed and stored *as is* without any alteration (that also allows us to support direct client <=> disk datapath). AIS on-disk format is, therefore, largely defined by local filesystem(s) chosen at deployment time.

Notwithstanding, AIS stores and then maintains object replicas, erasure-coded slices, bucket metadata - in short, a variety of local and global-scope (persistent) structures - for details, please refer to:

- [On-Disk Layout](on_disk_layout.md)

> **You can access your data with and without AIS, and without any need to *convert* or *export/import*, etc. - at any time! Your data is stored in its original native format using user-given object names. Your data can be migrated out of AIS at any time as well, and, again, without any dependency whatsoever on the AIS itself.**

> Your own data is [unlocked](https://en.wikipedia.org/wiki/Vendor_lock-in) and immediately available at all times.

## Existing Datasets

Common way to use AIStore include the most fundamental and, often, the very first step: populating AIS cluster with an existing dataset, or datasets. Those (datasets) can come from remote buckets (AWS, Google Cloud, Azure), HDFS directories, NFS shares, local files, or any vanilla HTTP(S) locations.

To this end, AIS provides 6 (six) easy ways ranging from the conventional on-demand caching to *promoting* colocated files and directories.

> Related references and examples include this [technical blog](https://aistore.nvidia.com/blog/2021/12/07/cp-files-to-ais) that shows how to copy a file-based dataset in two easy steps.

1. [Cold GET](#existing-datasets-cold-get)
2. [Prefetch](#existing-datasets-batch-prefetch)
3. [Internet Downloader](#existing-datasets-integrated-downloader)
4. [HTTP(S) Datasets](#existing-datasets-https-datasets)
5. [Promote local or shared files](#promote-local-or-shared-files)
6. [Backend Bucket](bucket.md#backend-bucket)
7. [Download very large objects (BLOBs)](/docs/cli/blob-downloader.md)
8. [Copy remote bucket](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)
9. [Copy multiple remote objects](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)

In particular:

### Existing Datasets: Cold GET

If the dataset in question is accessible via S3-like object API, start working with it via GET primitive of the [AIS API](http_api.md). Just make sure to provision AIS with the corresponding credentials to access the dataset's bucket in the Cloud.

> As far as supported S3-like backends, AIS currently supports Amazon S3, Google Cloud, Microsoft Azure, and Oracle OCI.

> AIS executes *cold GET* from the Cloud if and only if the object is not stored (by AIS), **or** the object has a bad checksum, **or** the object's version is outdated.

In all other cases, AIS will service the GET request without going to Cloud.

### Existing Datasets: Batch Prefetch

Alternatively or in parallel, you can also *prefetch* a flexibly-defined *list* or *range* of objects from any given remote bucket, as described in [this readme](batch.md).

For CLI usage, see:

* [CLI: prefetch](/docs/cli/object.md#prefetch-objects)

### Existing Datasets: integrated Downloader

But what if the dataset in question exists in the form of (vanilla) HTTP/HTTPS URL(s)? What if there's a popular bucket in, say, Google Cloud that contains images that you'd like to bring over into your Data Center and make available locally for AI researchers?

For these and similar use cases we have [AIS Downloader](/docs/downloader.md) - an integrated tool that can execute massive download requests, track their progress, and populate AIStore directly from the Internet.

### Promote local or shared files

AIS can also `promote` files and directories to objects. The operation entails synchronous or asynchronus massively-parallel downloading of any accessible file source, including:

- a local directory (or directories) of any target node (or nodes);
- a file share mounted on one or several (or all) target nodes in the cluster.

You can now use `promote` ([CLI](/docs/cli/object.md#promote-files-and-directories), API) to populate AIS datasets with **any external file source**.

Originally (experimentally) introduced in the v3.0 to handle "files and directories colocated within AIS storage target machines", `promote` has been redefined, extended (in terms of supported options and permutations), and completely reworked in the v3.9.

## Data Protection

AIS supports end-to-end checksumming and two distinct [storage services](storage_svcs.md) - N-way mirroring and erasure coding - providing for data redundancy.

The functionality that we denote as end-to-end checksumming further entails:

  - self-healing upon detecting corruption,
  - optimizing-out redundant writes upon detecting existence of the destination object,
  - utilizing client-provided checksum (iff provided) to perform end-to-end checksum validation,
  - utilizing Cloud checksum of an object that originated in a Cloud bucket, and
  - utilizing its version to perform so-called "cold" GET when object exists both in AIS and in the Cloud,

and more.

Needless to say, each of these sub-topics may require additional discussion of:

* [configurable options](configuration.md),
* [default settings](bucket.md), and
* the corresponding performance tradeoffs.

### Erasure Coding vs IO Performance

When an AIS bucket is EC-configured as (D, P), where D is the number of data slices and P - the number of parity slices, the corresponding space utilization ratio is not `(D + P)/D`, as one would assume.

It is, actually, `1 + (D + P)/D`.

This is because AIS was created to perform and scale in the first place. AIS always keeps one full replica at its [HRW location](traffic_patterns.md).

AIS will utilize EC to automatically self-heal upon detecting corruption (of the full replica). When a client performs a read on a non-existing (or not found) name, AIS will check with EC - assuming, obviously, that the bucket is erasure coded.

EC-related philosophy can be summarized as one word: **recovery**. EC plays no part in the fast path.

## Scale-Out

The scale-out category includes balanced and fair distribution of objects where each storage target will store (via a variant of the consistent hashing) 1/Nth of the entire namespace where (the number of objects) N is unlimited by design.

> AIS cluster capability to **scale-out is truly unlimited**. The real-life limitations can only be imposed by the environment - capacity of a given Data Center, for instance.

Similar to the AIS gateways, AIS storage targets can join and leave at any moment causing the cluster to rebalance itself in the background and without downtime.

## Networking

Architecture-wise, aistore is built to support 3 (three) logical networks:
* user-facing public and, possibly, **multi-home**) network interface
* intra-cluster control, and
* intra-cluster data

The way the corresponding config may look in production (e.g.) follows:

```console
$ ais config node t[nKfooBE] local h... <TAB-TAB>
host_net.hostname                 host_net.port_intra_control       host_net.hostname_intra_control
host_net.port                     host_net.port_intra_data          host_net.hostname_intra_data

$ ais config node t[nKfooBE] local host_net --json

    "host_net": {
        "hostname": "10.50.56.205",
        "hostname_intra_control": "ais-target-27.ais.svc.cluster.local",
        "hostname_intra_data": "ais-target-27.ais.svc.cluster.local",
        "port": "51081",
        "port_intra_control": "51082",
        "port_intra_data": "51083"
    }
```

The fact that there are 3 logical networks is not a limitation - i.e, not a requirement to have exactly 3 (networks).

Using the example above, here's a small deployment-time change to run a single one:

```console
    "host_net": {
        "hostname": "10.50.56.205",
        "hostname_intra_control": "ais-target-27.ais.svc.cluster.local",
        "hostname_intra_data": "ais-target-27.ais.svc.cluster.local",
        "port": "51081",
        "port_intra_control": "51081,   # <<<<<< notice the same port
        "port_intra_data": "51081"      # <<<<<< ditto
    }
```

Ideally though, production clusters are deployed over 3 physically different and isolated networks, whereby intense data traffic, for instance, does not introduce additional latency for the control one, etc.

Separately, there's a **multi-homing** capability motivated by the fact that today's server systems may often have, say, two 50Gbps network adapters. To deliver the entire 100Gbps _without_ LACP trunking and (static) teaming, we could simply have something like:

```console
    "host_net": {
        "hostname": "10.50.56.205, 10.50.56.206",
        "hostname_intra_control": "ais-target-27.ais.svc.cluster.local",
        "hostname_intra_data": "ais-target-27.ais.svc.cluster.local",
        "port": "51081",
        "port_intra_control": "51082",
        "port_intra_data": "51083"
    }
```

And that's all: add the second NIC (second IPv4 addr `10.50.56.206` above) with **no** other changes.

See also:

* [aistore configuration](configuration.md)

## HA

AIS features a [highly-available control plane](ha.md) where all gateways are absolutely identical in terms of their (client-accessible) data and control plane [APIs](http_api.md).

Gateways can be ad hoc added and removed, deployed remotely and/or locally to the compute clients (the latter option will eliminate one network roundtrip to resolve object locations).

## Fast Tier
AIS can be deployed as a fast tier in front of any of the multiple supported [backends](providers.md).

As a fast tier, AIS populates itself on demand (via *cold* GETs) and/or via its own *prefetch* API (see [List/Range Operations](batch.md#listrange-operations)) that runs in the background to download batches of objects.

## Other Services

The (quickly growing) list of services includes (but is not limited to):
* [health monitoring and recovery](https://github.com/NVIDIA/aistore/blob/main/fs/health/README.md)
* [range read](http_api.md)
* [dry-run (to measure raw network and disk performance)](performance.md#performance-testing)
* performance and capacity monitoring with full observability via StatsD/Grafana
* load balancing

> Load balancing consists in optimal selection of a local object replica and, therefore, requires buckets configured for [local mirroring](storage_svcs.md#read-load-balancing).

Most notably, AIStore provides **[dSort](/docs/dsort.md)** - a MapReduce layer that performs a wide variety of user-defined merge/sort *transformations* on large datasets used for/by deep learning applications.

## dSort

Dsort “views” AIS objects as named shards that comprise archived key/value data. In its 1.0 realization, dSort supports tar, zip, and tar-gzip formats and a variety of built-in sorting algorithms; it is designed, though, to incorporate other popular archival formats including `tf.Record` and `tf.Example` ([TensorFlow](https://www.tensorflow.org/tutorials/load_data/tfrecord)) and [MessagePack](https://msgpack.org/index.html). The user runs dSort by specifying an input dataset, by-key or by-value (i.e., by content) sorting algorithm, and a desired size of the resulting shards. The rest is done automatically and in parallel by the AIS storage targets, with no part of the processing that’d involve a single-host centralization and with dSort stage and progress-within-stage that can be monitored via user-friendly statistics.

By design, dSort tightly integrates with the AIS-object to take full advantage of the combined clustered CPU and IOPS. Each dSort job (note that multiple jobs can execute in parallel) generates a massively-parallel intra-cluster workload where each AIS target communicates with all other targets and executes a proportional "piece" of a job. This ultimately results in a *transformed* dataset optimized for subsequent training and inference by deep learning apps.

## CLI

AIStore includes an easy-to-use management-and-monitoring facility called [AIS CLI](/docs/cli.md). Once [installed](/docs/cli.md#getting-started), to start using it, simply execute:

 ```console
$ export AIS_ENDPOINT=http://ais-gateway:port
$ ais --help
 ```

where `ais-gateway:port` (above) denotes a `hostname:port` address of any AIS gateway (for developers it'll often be `localhost:8080`). Needless to say, the "exporting" must be done only once.

One salient feature of AIS CLI is its Bash style [auto-completions](/docs/cli.md#ais-cli-shell-auto-complete) that allow users to easily navigate supported operations and options by simply pressing the TAB key:

![CLI-tab](images/cli-overview.gif)

AIS CLI is currently quickly developing. For more information, please see the project's own [README](/docs/cli.md).

## ETL

AIStore is a hyper-converged architecture tailored specifically to run [extract-transform-load](/ext/etl/README.md) workloads - run them close to data and on (and by) all storage nodes in parallel:

![etl-v3.3](images/etl-v3.3.png)

For background and further references, see:

* [Extract, Transform, Load with AIStore](etl.md)
* [AIS-ETL introduction and a Jupyter notebook walk-through](https://www.youtube.com/watch?v=4PHkqTSE0ls)

## _No limitations_ principle

There are **no** designed-in limitations on the:

* object sizes
* total number of objects and buckets in AIS cluster
* number of objects in a single AIS bucket
* numbers of gateways (proxies) and storage targets in AIS cluster
* object name lengths

Ultimately, the limit on object size may be imposed by a local filesystem of choice and a physical disk capacity. While limit on the cluster size - by the capacity of the hosting AIStore Data Center.

In v3.26, AIStore has removed the basename and pathname limitations.

> On a typical Linux system, you will find that the relevant header(s) define:

```console
#define NAME_MAX 255
#define PATH_MAX 4096
```

Starting v3.26, AIStore supports object names of any length. See also:

* [Examples using extremely long names](https://github.com/NVIDIA/aistore/blob/main/docs/long_names.md)
