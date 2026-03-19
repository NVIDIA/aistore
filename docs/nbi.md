# Native Bucket Inventory (NBI)

Native bucket inventory (NBI) lets AIS create and serve a **local snapshot** of a remote bucket's namespace.

It is designed for **very large, read-mostly buckets** - for instance, large training and inference datasets - where repeatedly walking the remote backend to list the same objects is expensive, slow, and operationally noisy.

In v4.3, NBI status is **experimental**. The code is new, has little production mileage, and inventories are currently created **manually**. There is no periodic refresh or automatic resynchronization yet.

This document references Go types and constants from the AIS codebase. For SDK usage, see the [Go API](https://github.com/NVIDIA/aistore/tree/main/api) and [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk) documentation.

## Table of Contents

- [Motivation](#motivation)
- [Supported buckets](#supported-buckets)
- [Overview](#overview)
  - [Creating](#creating)
  - [Listing](#listing)
- [System buckets](#system-buckets)
- [Creating an inventory](#creating-an-inventory)
- [Monitoring inventory creation](#monitoring-inventory-creation)
- [Showing inventories](#showing-inventories)
- [Listing objects using inventory](#listing-objects-using-inventory)
- [How inventory-backed listing works](#how-inventory-backed-listing-works)
- [Inventory metadata](#inventory-metadata)
- [Small buckets and empty local inventories](#small-buckets-and-empty-local-inventories)
- [Current limitations](#current-limitations)
- [Legacy --s3-inventory](#legacy---s3-inventory)
- [Examples](#examples)

## Motivation

NBI is intended to replace the older S3-inventory integration.

Compared to the legacy S3-specific path, NBI is:

- **AIS-native**: created and consumed via AIS API/CLI and xactions
- **backend-agnostic**: works with remote buckets generally, not just S3
- **format-independent**: does not depend on provider-generated inventory formats such as CSV or Parquet
- **operationally simpler**: no external scripting or AWS CLI required

## Supported buckets

NBI applies to **remote buckets** only: cloud buckets (S3, GCS, Azure, OCI) and remote AIS buckets. It is not currently supported for in-cluster `ais://` buckets (with little motivation to add such support in the future).

## Overview

At a high level, NBI works in two phases: **create** and **list**.

### Creating

1. A user explicitly creates an inventory for a remote bucket.
2. The proxy initiates a 2PC (two-phase commit) transaction across all storage nodes.
3. During the Begin phase, each target validates the request, checks capacity, verifies that no other `create-inventory` job is running for the same bucket, and enforces the one-inventory-per-bucket limit (removing any prior inventory if `--force` is specified).
4. On Commit, each target runs a `create-inventory` xaction: it lists the backend bucket, keeps only the object names that belong to it - where "belong" is uniquely defined by the triplet (cluster map, bucket, object name) - and stores them locally as inventory chunks.

> **Note:** In the current (v4.3) implementation, all targets independently list the remote backend. A future optimization will use a designated target to list once and distribute the results - the same approach that regular `list-objects` already uses.

### Listing

1. A list-objects request with `--inventory` (the `LsNBI` flag) arrives at the proxy.
2. The proxy broadcasts to all targets, each of which reads its local inventory chunks and returns its portion.
3. The proxy merges the per-target responses into a single sorted listing stream.

Here is a quick example - ~48M objects in an S3 bucket, 24 targets:

```console
## create
$ ais nbi create s3://training-data-v5
...

## list using the inventory
$ time ais ls s3://training-data-v5 --inventory | tail
zzy/shard-0489132.tar         128.00MiB       yes
zzy/shard-0489133.tar         128.00MiB       yes
zzz/shard-0491718.tar         128.00MiB       yes
Listed 48,017,395 names

real    0m14.238s
user    0m9.671s
sys     0m3.102s
```

The same `ais ls` without `--inventory` produces identical results but walks the remote backend each time.

## System buckets

Inventories are stored as chunked objects in a designated **system bucket** - a special AIS bucket whose name starts with a dot (`.`).

> System buckets are AIS infrastructure and are not intended for direct user access. The dot-prefix naming convention (`.sys-*`) is reserved.
> For details on naming rules, visibility, and future plans, see [System Buckets](/docs/bucket.md#system-buckets).

The first (and currently only) system bucket is `ais://.sys-inventory`. It is created on the fly upon the very first inventory creation request. There is no need to create it manually.

Creating an inventory requires admin permissions. When AIStore is deployed with the [Authentication Server (AuthN)](/docs/authn.md), the operation requires `admin` [permissions](/docs/authn.md#permissions). AuthN is a deployment-time option; clusters without AuthN do not enforce this check.

```console
$ ais ls ais://.sys-inventory
NAME                                              SIZE
aws/@#/training-data-v5/inv-Tp4nR7kWx             1.28GiB

$ ais ls ais://.sys-inventory --props chunked,size
NAME                                              CHUNKED         SIZE
aws/@#/training-data-v5/inv-Tp4nR7kWx             yes             1.28GiB
```

System buckets are AIS infrastructure and are not intended for direct user access. The dot-prefix naming convention (`.sys-*`) is reserved.

## Creating an inventory

Use `ais nbi create` to create a native bucket inventory for a remote bucket:

```console
ais nbi create s3://my-bucket
ais nbi create gs://my-bucket --inv-name my-first-inventory
ais nbi create oci://my-bucket --prefix images/
ais nbi create ais://@remote-cluster/my-bucket --props name,size,cached
```

### Notes

* Inventory creation is **manual**.
* Inventory names are optional, but when provided must be unique for the bucket.
* `--force` removes any existing inventory first and then proceeds with creation.
* `--names-per-chunk` is an advanced tuning knob that overrides the default chunk size.

Internally, the control message is:

```go
type CreateNBIMsg struct {
    Name string `json:"name,omitempty"` // inventory name (optional; must be unique for a given bucket)
    LsoMsg

    // Number of object names to store in each inventory chunk.
    // Requested properties are stored alongside each name.
    // Advanced usage only - non-zero overrides system default.
    NamesPerChunk int64 `json:"names_per_chunk,omitempty"`

    // Remove all existing inventories, if any, and proceed to create the new one
    // (only one inventory per bucket is supported).
    Force bool `json:"force,omitempty"`
}
```

### Defaults and validation

Inventory creation validates and normalizes the embedded `LsoMsg`.

Current behavior includes:

* `continuation_token` must be empty
* `start_after` is not supported
* several list flags are rejected because they do not make sense for inventory creation
* `LsNoDirs` is always set internally
* default stored properties are:

  * `name`
  * `size`
  * `cached`

If properties are explicitly requested, AIS still adds `cached`.

Advanced tunables:

* default `names_per_chunk`: `2 * MaxPageSizeAIS` (20K)
* minimum `names_per_chunk`: `2`
* maximum `names_per_chunk`: `64 * MaxPageSizeAIS` (640K)

## Monitoring inventory creation

Inventory creation is a distributed xaction, so it shows up in regular job monitoring:

```console
$ ais show job create-inventory --all
create-inventory[inv-Tp4nR7kWx] (ctl: s3://training-data-v5, props:name,size,cached, flags:no-dirs)
NODE             ID              KIND                    BUCKET                  OBJECTS         BYTES   START           END             STATE
TnKtfgHp         inv-Tp4nR7kWx   create-inventory        s3://training-data-v5   2001384         -       02:15:07        03:01:38        Finished
VpRtorrn         inv-Tp4nR7kWx   create-inventory        s3://training-data-v5   1998017         -       02:15:07        03:02:11        Finished
WqBtywqm         inv-Tp4nR7kWx   create-inventory        s3://training-data-v5   2000891         -       02:15:07        03:01:54        Finished
...
nFYteelz         inv-Tp4nR7kWx   create-inventory        s3://training-data-v5   2001562         -       02:15:07        03:02:29        Finished
                                Total:                                          48017395        -
```

Note the per-target object counts - each target stores only the names that properly belong to it (as determined by the cluster map, bucket, and object name). The total (48,017,395) is the full bucket namespace.

## Showing inventories

Use `ais show nbi` to inspect inventories:

```console
ais show nbi gs://my-bucket
ais show nbi s3://my-bucket --inv-name my-first-inventory
ais show nbi ais://@remote-cluster/my-bucket --verbose
```

Default output shows a compact view, including object count:

```text
BUCKET  NAME  SIZE  OBJECTS  STARTED  FINISHED  PREFIX
```

Verbose output includes additional metadata:

```text
BUCKET  NAME  OBJECT  SIZE  OBJECTS  CHUNKS  TARGETS  SMAP  STARTED  FINISHED  PREFIX
```

For example:

```console
$ ais show nbi s3://training-data-v5 --verbose

BUCKET                 NAME           OBJECT                                 SIZE      OBJECTS     CHUNKS  TARGETS  SMAP  STARTED               FINISHED              PREFIX
s3://training-data-v5  inv-Tp4nR7kWx  aws/@#/training-data-v5/inv-Tp4nR7kWx  1.28GiB   48017395    101     24       v138  2026-03-18T02:15:07Z  2026-03-18T03:02:29Z  -
```

## Listing objects using inventory

Once an inventory has been created, use it by adding `--inventory` to a regular `ais ls` command:

```console
ais ls gs://abc --inventory
ais ls oci://abc --inventory --paged --prefix=subdir
ais ls s3://abc --inventory --inv-name my-first-inventory
```

This is still a normal list-objects request. The difference is that AIS sets the `LsNBI` flag internally and switches to the inventory-backed path.

### Pagination

When listing via NBI, `page-size` is **best-effort and approximate**.

Because NBI listing is distributed across targets, each target returns an approximate share of the requested page size, subject to local chunking and minimum bounds.
This behavior is intentional and helps to reduce roundtrips, optimize list-objects latency.

## How inventory-backed listing works

See [Overview - Listing](#listing) for the high-level flow.

The `LsNBI` flag is what switches a list-objects request to the inventory-backed path. When set, AIS reads pre-stored inventory chunks from local storage on each target rather than walking the remote backend. The proxy merges and sorts the per-target results into a single listing stream.

The result is that repeated listing avoids re-walking the remote backend entirely.

## Inventory metadata

AIS stores metadata alongside each inventory:

```go
type NBIMeta struct {
    Prefix   string `json:"prefix,omitempty"`   // lsmsg.Prefix
    Started  int64  `json:"started,omitempty"`  // time started creating (ns)
    Finished int64  `json:"finished,omitempty"` // finished (ns)
    Ntotal   int64  `json:"ntotal,omitempty"`   // total number of names in the inventory
    SmapVer  int64  `json:"smap_ver,omitempty"` // cluster map when writing inventory
    Chunks   int32  `json:"chunks,omitempty"`   // number of chunks (manifest.Count())
    Nat      int32  `json:"nat,omitempty"`      // number of active (not in maintenance) targets
}
```

This metadata is surfaced by `ais show nbi`, especially in verbose mode.

## Small buckets and empty local inventories

When a bucket is smaller than the cluster, not every target will necessarily own any names for the inventory.

This is expected. In that case, a target stores a valid local inventory object with zero size, zero chunks, and valid NBI metadata. This allows AIS to distinguish "inventory exists but is empty on this target" from "inventory does not exist" - a distinction that matters for correct EOF behavior during distributed listing.

## Current limitations

NBI in v4.3 is intentionally narrower than general remote listing.

### Listing via NBI

| Option / behavior | Description |
|---|---|
| `start_after` | Not supported for inventory-backed listing. Use prefix and continuation-based pagination instead. |
| `--not-cached` (`LsNotCached`) | Not applicable. NBI serves a stored snapshot rather than computing "not present in cluster" against the live backend. |
| `LsMissing` | Not supported. Inventory listing is not a missing-copy inspection mode. |
| `LsDeleted` | Not supported. Inventory snapshots do not currently track objects marked for deletion. |
| `--archive` / `LsArchDir` | NBI lists stored object entries, not archive contents. |
| `lsWantOnlyRemoteProps` | NBI is not a pass-through remote-property listing path. |
| `--non-recursive` (`LsNoRecursion`) | Inventories are stored as flat snapshots. |
| `--diff` (`LsDiff`) | NBI does not currently perform remote-versus-cluster diff during listing. |

In short, listing via NBI is optimized for fast, repeated listing of a previously captured bucket snapshot.

### Creating NBI

| Option / behavior | Description |
|---|---|
| non-empty `continuation_token` | Inventory creation always starts from the beginning of the bucket namespace. |
| `start_after` | Not supported during inventory generation. |
| `--cached` (`LsCached`) | Not applicable. NBI creation walks the remote bucket namespace, not only currently cached content. |
| `--not-cached` (`LsNotCached`) | Not applicable for the same reason. |
| `LsMissing` | Not supported during creation. |
| `LsDeleted` | Not supported during creation. |
| `--archive` / `LsArchDir` | Inventory creation does not expand archives. |
| `LsBckPresent` | Not applicable. Inventory creation operates on a concrete remote bucket request. |
| `LsDontHeadRemote` | Inventory creation requires normal remote bucket validation. |
| `LsDontAddRemote` | The bucket must participate in normal AIS metadata flow. |
| `lsWantOnlyRemoteProps` | NBI creation stores selected inventory properties rather than pass-through backend metadata only. |
| `--non-recursive` (`LsNoRecursion`) | NBI snapshots are flat. |
| `--diff` (`LsDiff`) | Inventory creation is not a diff job. |
| `LsIsS3` | Not applicable. |

## Legacy `--s3-inventory`

AIS currently supports both:

* `--inventory` for native bucket inventory (NBI)
* `--s3-inventory` for the older S3-specific inventory path

The legacy S3 path is deprecated and scheduled for removal.

Unlike the legacy S3 inventory support, NBI is not limited to S3, is not tied to provider-generated inventory formats, does not require external tooling such as AWS CLI scripts, and is created and consumed entirely through AIS.

## Examples

### Create, inspect, and list

```console
## create an inventory for a GCS bucket
$ ais nbi create gs://imagenet --inv-name trainset-v1

## inspect
$ ais show nbi gs://imagenet --inv-name trainset-v1
$ ais show nbi gs://imagenet --inv-name trainset-v1 --verbose

## list via inventory
$ ais ls gs://imagenet --inventory --inv-name trainset-v1
$ ais ls gs://imagenet/train/ --inventory --paged --page-size 1000
$ ais ls gs://imagenet --inventory --prefix train/dogs/
```

### Remote AIS bucket

```console
$ ais nbi create ais://@remote-cluster/datasets --inv-name snapshot-1
$ ais ls ais://@remote-cluster/datasets --inventory --inv-name snapshot-1
```

### Where inventories are stored

```console
$ ais ls ais://.sys-inventory
NAME                                              SIZE
aws/@#/training-data-v5/inv-Tp4nR7kWx             1.28GiB

$ ais ls ais://.sys-inventory --props chunked,size
NAME                                              CHUNKED         SIZE
aws/@#/training-data-v5/inv-Tp4nR7kWx             yes             1.28GiB
```
