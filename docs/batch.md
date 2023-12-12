---
layout: post
title: BATCH
permalink: /docs/batch
redirect_from:
 - /batch.md/
 - /docs/batch.md/
---

## Introduction

By definition,  *eXtended actions* (aka *xactions*) are batch operations that run asynchronously, are monitorable, can be aborted, and - most importantly - may take many seconds (minutes, sometimes hours) to execute.

Examples include erasure coding or n-way mirroring a dataset, resharding and reshuffling a dataset, and many more.

> In the source code, all supported - and the most recently updated - *xactions* are enumerated [here](https://github.com/NVIDIA/aistore/blob/main/xaction/table.go).

All [eXtended actions](/xact/README.md) support generic [API](/api/xaction.go) and [CLI](/docs/cli/job.md#show-job-statistics) to show both common counters (byte and object numbers) as well as operation-specific extended statistics.

Global rebalance that gets triggered by any membership changes (nodes joining, leaving, powercycling, etc.) can be further visualized via `ais show rebalance` CLI.

> Rebalance and a few other AIS jobs have their own CLI extensions. Generally, though, you can always monitor *xactions* via `ais show job xaction` command that also supports verbose mode and other options.

AIS subsystems integrate subsystem-specific stats - e.g.:

* [dSort](/docs/dsort.md)
* [Downloader](/docs/downloader.md)
* [ETL](/docs/etl.md)

Related CLI documentation:

* [CLI: `ais show job`](/docs/cli/job.md)
* [CLI: multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges)
* [CLI: reading, writing, and listing archives](/docs/cli/object.md)
* [CLI: copying buckets](/docs/cli/bucket.md#copy-bucket)

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

For CLI documentation and examples, please see [Operations on Lists and Ranges](cli/object.md#operations-on-lists-and-ranges).

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
