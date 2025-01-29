---
layout: post
title: BATCH
permalink: /docs/batch
redirect_from:
 - /batch.md/
 - /docs/batch.md/
---

## Introduction

Extended actions (_xactions_) are batch operations, or jobs, that run asynchronously, report satistics (viewable at runtime and later), can be waited upon, and can be stopped.

Terminology-wise, in the code we mostly call it _xaction_ by the name of the corresponding software abstraction. But elsewhere, it is simply a _job_ - the two terms are interchangeable.

> In the source code, all supported *xactions* are enumerated [here](https://github.com/NVIDIA/aistore/blob/main/api/apc/actmsg.go).

For users, there's an API to start, stop, and wait for a job:

* [Go API: xaction](https://github.com/NVIDIA/aistore/blob/main/api/xaction.go)
* [Python API: job](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/job.py)

In CLI, there's `ais job` command and its subcommands (`<TAB-TAB>` completions):

```commandline
$ ais job
start   stop    wait    rm      show

$ ais start
prefetch           lru                cleanup            copy-bck
blob-download      etl                mirror             move-bck
download           rebalance          ec-encode
dsort              resilver           warm-up-metadata
```

> Note that `ais start` is an [alias](/docs/cli/alias.md) for the `ais job start` command - both (versions) can be used interchangeably.

**Not all supported jobs are _startable_**.

Not all supported jobs can be started via `ais start` or by the corresponding Go or Python API call. Example, the job to copy or (ETL) transform datasets has its own dedicated API (both Python and Go) and CLI.

> See e.g., `ais cp --help`

Complete and most recently updated list of supported jobs can be found in this [table of job descriptors](https://github.com/NVIDIA/aistore/blob/main/xact/api.go#L111-L116).

Last (but not the least) is - time. Job execution may take many seconds, sometimes minutes or hours.

Examples include erasure coding or n-way mirroring a dataset, resharding and reshuffling a dataset and more.

Global rebalance gets (automatically) triggered by any membership changes (nodes joining, leaving, powercycling, etc.) that can be further visualized via `ais show rebalance` CLI.

Another example would be _primary election_. AIS proxies provide access points ("endpoints") for the frontend API. At any point in time there is a single **primary** proxy that also controls versioning and distribution of the current cluster map. When and if the primary fails, another proxy is majority-elected to perform the (primary) role.

This (election by simple majority) is also a _job_ that cannot be started via `ais start` or the corresponding API. Similar to global rebalance, it is _event-driven_. Similar to rebalance, there's a separate dedicated API to run it administratively.

> Rebalance and a few other AIS jobs have their own CLI extensions. Generally, though, you can always monitor *xactions* via `ais show job xaction` command that also supports verbose mode and other options.

AIS subsystems integrate subsystem-specific stats - e.g.:

* [dSort](/docs/dsort.md)
* [Downloader](/docs/downloader.md)
* [ETL](/docs/etl.md)

Related CLI documentation:

* [CLI: `ais show job`](/docs/cli/job.md)
* [CLI: multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges-and-entire-buckets)
* [CLI: reading, writing, and listing archives](/docs/cli/object.md)
* [CLI: copying buckets](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)

## Table of Contents
- [Operations on multiple selected objects](#operations-on-multiple-selected-objects)
  - [List](#list)
  - [Range](#range)
  - [Examples](#examples)

## Operations on multiple selected objects

AIStore provides APIs to operate on *batches* of objects:

| API Message (apc.ActionMsg) | Description |
| --- | --- |
| `apc.ActCopyObjects`     | copy multiple objects |
| `apc.ActDeleteObjects`   | delete --/-- |
| `apc.ActETLObjects`      | etl (transform) --/-- |
| `apc.ActEvictObjects`    | evict --/-- |
| `apc.ActPrefetchObjects` | prefetch --/-- |
| `apc.ActArchive`         | archive --/-- |

For CLI documentation and examples, please see [Operations on Lists and Ranges (and entire buckets)](cli/object.md#operations-on-lists-and-ranges-and-entire-buckets).

There are two distinct ways to specify the objects: **list** them (ie., the names) explicitly, or specify a **template**.

Supported template syntax includes 3 standalone variations - 3 alternative formats:

1. bash (or shell) brace expansion:
   * `prefix-{0..100}-suffix`
   * `prefix-{00001..00010..2}-gap-{001..100..2}-suffix`
2. at style:
   * `prefix-@100-suffix`
   * `prefix-@00001-gap-@100-suffix`
3. fmt style:
   * `prefix-%06d-suffix`

In all cases, prefix and/or suffix are optional.

#### List

List APIs take a JSON array of object names, and initiate the operation on those objects.

| Parameter | Description |
| --- | --- |
| objnames | JSON array of object names |

#### Range

| Parameter | Description |
| --- | --- |
| template | The object name template with optional range parts. If a range is omitted the template is used as an object name prefix |

#### Examples

All the following examples assume that the action is `delete` and the bucket name is `bck`, so only the value part of the request is shown:

`"value": {"list": "["obj1","dir/obj2"]"}` - deletes objects `obj1` and `dir/obj2` from the bucket `bck`

`"value": {"template": "obj-{07..10}"}` - removes the following objects from `bck`(note leading zeroes in object names):

- obj-07
- obj-08
- obj-09
- obj-10

`"value": {"template": "dir-{0..1}/obj-{07..08}"}` - template can contain more than one range, this example removes the following objects from `bck`(note leading zeroes in object names):

- dir-0/obj-07
- dir-0/obj-08
- dir-1/obj-07
- dir-1/obj-08

`"value": {"template": "dir-10/"}` - the template defines no ranges, so the request deletes all objects which names start with `dir-10/`
