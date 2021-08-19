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

All [eXtended actions](/xaction/README.md) support generic [API](/api/xaction.go) and [CLI](/docs/cli/job.md#show-job-statistics) to show both common counters (byte and object numbers) as well as operation-specific extended statistics.

Global rebalance that gets triggered by any membership changes (nodes joining, leaving, powercycling, etc.) can be further visualized via `ais show rebalance` CLI.

AIS subsystems integrate subsystem-specific stats - e.g.:

* [dSort](/docs/dsort.md)
* [Downloader](/docs/downloader.md)
* [ETL](/docs/etl.md)

See also:

> [CLI: multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges).
> [CLI: reading, writing, and listing archives](/docs/cli/object.md).
> [CLI: copying buckets](/docs/cli/bucket.md#copy-bucket)

## Table of Contents
- [List/Range Operations](#listrange-operations)
  - [List](#list)
  - [Range](#range)
  - [Examples](#examples)

## List/Range Operations

AIStore provides two APIs to operate on groups of objects: List, and Template.

For CLI documentation and examples, please see [Operations on Lists and Ranges](cli/object.md#operations-on-lists-and-ranges).

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
