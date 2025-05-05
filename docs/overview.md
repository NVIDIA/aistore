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

The rest of this document is structured as follows:

- [At a glance](#at-a-glance)
- [Terminology](#terminology)
  - [Target](#target)
  - [Proxy](#proxy)
  - [Backend Provider](#backend provider)
  - [Unified Namespace](#unified namespace)
  - [Xaction](#xaction)
  - [Shard](#shard)
  - [Mountpath](#mountpath)
  - [Read-after-Write Consistency](#read-after-write consistency)
  - [Write-through](#write-through)
- [Design Philosophy](#design-philosophy)
- [Original Diagrams](#original-diagrams)
- [AIStore API](#aistore-api)
- [Traffic Patterns](#traffic-patterns)
- [Open Format](#open-format)
- [Existing Datasets](#existing-datasets)
- [Data Protection](#data-protection)
  - [Erasure Coding vs IO Performance](#erasure-coding-vs-io-performance)
- [Scale-Out](#scale-out)
- [Networking](#networking)
- [HA](#ha)
- [Other Services](#other-services)
- [Sharding extensions: dSort and iShard](#sharding-extensions-dsort-and-ishard)
- [CLI](#cli)
- [ETL](#etl)
- [_No limitations_ principle](#no-limitations-principle)

## At a glance

Following is a high-level **block diagram** with an emphasis on supported frontend and backend APIs, and the capability to scale-out horizontally. The picture also tries to make the point that AIS aggregates an arbitrary numbers of storage servers ("targets") with local or locally accessible drives, whereby each drive is formatted with a local filesystem of choice (e.g., xfs or zfs).

![At-a-Glance](images/cluster-block-v3.26.png)

In any AIS cluster, there are **two kinds of nodes**: proxies (a.k.a. gateways) and targets (storage nodes):

![Proxies and Targets](images/proxy-target-block-2024.png)

Proxies provide access points ("endpoints") for the frontend API. At any point in time there is a single **primary** proxy that also controls versioning and distribution of the current cluster map. When and if the primary fails, another proxy is majority-elected to perform the (primary) role.

All user data is equally distributed (or [balanced](/docs/rebalance.md)) across all storage nodes ("targets"). Which, combined with zero (I/O routing and metadata processing) overhead, provides for linear scale with no limitation on the total number of aggregated storage drives.

## Terminology

### Target

A storage node. In the documentation and code, we refer to a "target" simply as "target" instead of saying "storage node in an AIS cluster."

To store user data, targets utilize any number [mountpaths](#mountpath).

### Proxy

A **gateway** providing [API](#aistore-api) access point. Proxies are diskless, mesaning they do not have direct access to user data, and do not "see" user data in-flight. One of the proxies is elected, or designated, as the _primary_ (or leader) of the cluster. There may be any number of ais proxies/gateways (but only one _primary_ at a time). AIS proxy/gateway provides HTTP-based APIs, both [native](#aistore-api) and S3 compatible option.

In the event of a failure of the current _primary_ the remaining proxies collaborate to perform a majority-voted HA failover. The terms "proxy" and "gateway" are used interchangeably.

> In a cluster, there is no direct correlation between the number of proxies and targets; however, for symmetry, we typically deploy one proxy for each target node.

### Backend Provider

Backend provider (or simply **backend**) is an abstraction, and simultaneously an API-supported option that differentiates between "remote" (e.g., `s3://`) and "local" (`ais://`) buckets with respect to a given AIS cluster. AIS [supports multiple storage backends](images/supported-backends.png) including its own.

> See the [providers](providers.md) readme for the most recently updated list of supported cloud storage options; instructions for running AIS as a backend for another AIS cluster are also included.

### Unified Namespace

AIS clusters, when *attached* to each other, form a super-cluster providing unified global namespace whereby all buckets and all objects of all included clusters are uniformly accessible via any and all individual access points (of those clusters).

Users can use a single AIS endpoint while referencing shared buckets with cluster-specific identifiers.

### Xaction

Extended action (or simply *xaction*) is asynchronous batch operations that may take many seconds (minutes, hours, etc.) to execute.

CLI and [CLI documentation](/docs/cli) refers to such operations as **jobs**, a more familiar term that we use interchangeably. Examples include erasure coding, n-way mirroring a dataset, resharding and reshuffling a dataset, archiving multiple objects, copying buckets, and more.

All [eXtended actions](https://github.com/NVIDIA/aistore/blob/main/xact/README.md) support generic [API](/api/xaction.go) and [CLI](/docs/cli/job.md) for displaying both common counters (byte and object numbers) and operation-specific extended statistics.

The list of supported jobs can be viewed via the CLI `show` command and currently includes:

```console
$ ais show job --help

NAME:
   ais show job - Show running and/or finished jobs:

     archive        blob-download  cleanup     copy-bucket       copy-objects      delete-objects
     download       dsort          ec-bucket   ec-get            ec-put            ec-resp
     elect-primary  etl-bucket     etl-inline  etl-objects       evict-objects     evict-remote-bucket
     list           lru-eviction   mirror      prefetch-objects  promote-files     put-copies
     rebalance      rename-bucket  resilver    summary           warm-up-metadata
```

### Shard

In AIStore, _sharding_ refers to the process of serializing original files and/or objects (such as images and labels) into larger-sized objects formatted as TAR, TGZ, ZIP, or TAR.LZ4 archives.

The benefits of serialization are well-established: iterable formats like TAR enable purely sequential I/O operations, significantly improving performance on local drives. In the context of machine learning, sharding enhances data shuffling and eliminates bias, allowing for global shuffling of shard names and the use of a shuffle buffer on the client side to ensure adequate randomization of training data.

Additionally, a shard is an object that can follow a specific convention, where related files (such as abc.jpeg, abc.cls, and abc.json) are packaged together in a single TAR archive. While AIS can read, write, and list any TAR, using this convention ensures that the necessary components for learning are kept together. For more information on the WebDataset format, see the [Hugging Face documentation](https://huggingface.co/docs/hub/en/datasets-webdataset).

For more details, see:

* [AIS can natively read, write, append, and list archives](/docs/archive.md)
* [CLI: archive](/docs/cli/archive.md)
* [Initial Sharding Tool (`ishard`)](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md)
* [Distributed Shuffle](/docs/cli/dsort.md)

### Mountpath

An AIS mountpath is defined as:

* A single disk or a volume (such as a RAID) formatted with a local filesystem of your choice, and
* A local directory that AIS can fully own and utilize to store user data and system metadata.

Please note that any given disk (or RAID) can have at most one AIS mountpath — disk sharing is not permitted.

Additionally, mountpath directories cannot be nested.

Furthermore:

* A mountpath can be temporarily disabled and (re)enabled.
* A mountpath can also be detached and (re)attached, effectively supporting the growth and "shrinkage" of local capacity.
* It is safe to execute the four listed operations (enable, disable, attach, detach) at any point during runtime.
* In a typical deployment, the total number of mountpaths is calculated as the direct product of the number of storage targets and the number of disks in each [target](#target).

### Read-after-Write Consistency

The `PUT(object)` operation is a transaction. A new object (or a new version of an existing object) becomes visible and accessible only after AIS has finished writing the first replica and its associated metadata.

For S3 or any other remote [backends](#backend-provider), this process includes:

* Performing a remote PUT via the vendor's SDK library.
* Executing a local write (that won't be visible to the user).
* Receiving a successful remote response that carries the remote metadata.
* Simultaneously computing the checksum (as per bucket configuration).
* Optionally validating the checksum, if configured.
* Finally, writing the combined object metadata, at which point the object becomes visible and accessible.

Once the first object replica is finalized, subsequent reads are guaranteed to view the same content, regardless of the AIS gateway used. Additional copies or erasure-coded slices are added asynchronously, if configured. For example, in a bucket with 3-way replication, you may be able to read the first replica while the other two copies are still being processed.

It is important to emphasize that the same rules of data protection and consistency are universally enforced across all _writing_ scenarios, including (but not limited to):

* Cold GET
* Copy bucket
* Transform bucket
* Multi-object copy, multi-object transform, multi-object archive
* Prefetch remote bucket
* Download large remote objects (blobs)
* Rename bucket
* Promote NFS share

And more.

### Write-through

In presence of remote backends, AIS executes remote writes (using the vendor's SDK) as part of the same [transaction](#read-after-write-consistency) that finalizes storing the object's first replica in the cluster. If the remote write fails, the entire write operation will also fail (with subsequent associated cleanup).

## Design Philosophy

It is often more optimal to let applications control how and whether the stored content is stored in chunks. That's the simple truth that holds, in particular, for AI datasets that are often pre-sharded with content and boundaries of those shards based on application-specific optimization criteria. More exactly, the datasets could be pre-sharded, post-sharded, and otherwise transformed to facilitate training, inference, and simulation by the AI apps.

The corollary of this statement is two-fold:

- Breaking objects into pieces (often called chunks but also slices, segments, fragments, and blocks) and the related functionality does not necessarily belong to an AI-optimized storage system per se;
- Instead of chunking the objects and then reassembling them with the help of cluster-wide metadata (that must be maintained with extreme care), the storage system could alternatively focus on providing assistance to simplify and accelerate dataset transformations.

Notice that the same exact approach works for the other side of the spectrum - the proverbial [small-file problem](https://www.quora.com/What-is-the-small-file-problem-in-Hadoop). Here again, instead of optimizing small-size IOPS, we focus on application-specific (re)sharding, whereby each shard would have a desirable size, contain a batch of the original (small) files, and where the files (aka samples) would be sorted to optimizes subsequent computation.

## Original Diagrams

AIS cluster *comprises* arbitrary (and not necessarily equal) numbers of **gateways** and **storage targets**. Targets utilize local disks while gateways are HTTP **proxies** that provide most of the control plane and never touch the data.

> The terms *gateway* and *proxy* are used interchangeably throughout this README and other sources in the repository.

Both **gateways** and **targets** are userspace daemons that join (and, by joining, form) a storage cluster at their respective startup times, or upon user request. AIStore can be deployed on any commodity hardware with pretty much any Linux distribution (although we do recommend 4.x kernel). There are no designed-in size/scale type limitations. There are no dependencies on special hardware capabilities. The code itself is free, open, and MIT-licensed.

AIS can be deployed as a self-contained standalone persistent storage cluster or a fast tier in front of any of the supported backends including Amazon S3 and Google Cloud (GCP). The built-in caching mechanism provides LRU replacement policy on a per-bucket basis while taking into account configurable high and low capacity watermarks (see [LRU](storage_svcs.md#lru) for details). AWS/GCP integration is *turnkey* and boils down to provisioning AIS targets with credentials to access Cloud-based buckets.

If (compute + storage) rack is a *unit of deployment*, it may as well look as follows:

![One rack](images/ais-rack-20-block.png)

Here's an older (and simpler) diagram that depicts AIS target node:

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

## Open Format

AIS targets utilize local Linux filesystems including (but not limited to) xfs, ext4, and openzfs. User data is checksummed and stored *as is* without any alteration (that also allows us to support direct client <=> disk datapath). AIS on-disk format is, therefore, largely defined by local filesystem(s) chosen at deployment time.

Notwithstanding, AIS stores and then maintains object replicas, erasure-coded slices, bucket metadata - in short, a variety of local and global-scope (persistent) structures - for details, please refer to:

- [On-Disk Layout](on_disk_layout.md)

> **You can access your data with and without AIS, and without any need to *convert* or *export/import*, etc. - at any time! Your data is stored in its original native format using user-given object names. Your data can be migrated out of AIS at any time as well, and, again, without any dependency whatsoever on the AIS itself.**

> Your own data is [unlocked](https://en.wikipedia.org/wiki/Vendor_lock-in) and immediately available at all times.

## Existing Datasets

Common way to use AIStore include the most fundamental and, often, the very first step: populating AIS cluster with an existing dataset, or datasets. Those (datasets) can come from remote buckets (AWS, Google Cloud, Azure), NFS shares, local files, or any vanilla HTTP(S) locations.

> In the context of populating AIS cluster - note: the terms **"in-cluster objects" and "cached objects" are used interchangeably** throughout the entire documentation.

> While the terms 'in-cluster objects' and 'cached objects' are used interchangeably, it is also important to note that AIStore is not a caching solution. Although it can function as an LRU cache when LRU (feature) is enabled, AIS is designed to provide reliable storage, equipped with a comprehensive set of data protection features and configurable data redundancy options on a per-bucket basis.

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

## Sharding extensions: dSort and iShard

Distributed shuffle (codename dSort) “views” AIS data as named shards that comprise archived key/value data. The service supports tar, zip, tar-gzip, and tar-lz4 formats and a variety of built-in sorting algorithms; it is designed, though, to incorporate other popular archival formats including `tf.Record` and `tf.Example` ([TensorFlow](https://www.tensorflow.org/tutorials/load_data/tfrecord)) and [MessagePack](https://msgpack.org/index.html).

User runs dSort by specifying an input dataset, by-key or by-value (i.e., by content) sorting algorithm, and a desired size of the resulting shards. The rest is done automatically and in parallel by the AIS storage targets, with no part of the processing that’d involve a single-host centralization and with dSort stage and progress-within-stage that can be monitored via user-friendly statistics.

Each dSort job (note that multiple jobs can execute in parallel) generates a massively-parallel intra-cluster workload where each AIS target communicates with all other targets and executes a proportional "piece" of a job. This ultimately results in a *transformed* dataset optimized for subsequent training and inference by deep learning apps.

Initial Sharding (`ishard`) utility will generate WebDataset-formatted shards from the original file-based dataset without spliting computable samples. The ultimate goal is to allow users to treat AIStore as a vast data lake, where they can easily upload training data in its raw format, regardless of size and directory structure.

For more details, see:
* [dSort](/docs/dsort.md)
* [ishard](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md)

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
