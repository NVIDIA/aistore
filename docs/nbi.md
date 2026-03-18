# Native Bucket Inventory (NBI)

Native bucket inventory (NBI) lets AIS create and serve a **local snapshot** of a remote bucket's namespace.

It is designed for **very large, read-mostly buckets** - for instance, large training and inference datasets - where repeatedly walking the remote backend to list the same objects is expensive, slow, and operationally noisy.

In v4.3, NBI status is **experimental**. The code is new, has little production mileage, and inventories are currently created **manually**. There is no periodic refresh or automatic resynchronization yet.

## Motivation

NBI is intended to replace the older S3-inventory integration.

Compared to the legacy S3-specific path, NBI is:

- **AIS-native**: created and consumed via AIS API/CLI and xactions
- **backend-agnostic**: works with remote buckets generally, not just S3
- **format-independent**: does not depend on provider-generated inventory formats such as CSV or Parquet
- **operationally simpler**: no external scripting or AWS CLI required

This document references Go types and constants from the AIS codebase. For SDK usage, see the [Go API](https://github.com/NVIDIA/aistore/tree/main/api) and [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk) documentation.

Rest of it is structured as follows:

**Table of Contents**

- [Supported buckets](#supported-buckets)
- [Overview](#overview)
- [Current status in v4.3](#current-status-in-v43)
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
- [Summary](#summary)

## Supported buckets

NBI applies to **remote buckets**:

- cloud buckets
- remote AIS buckets

NBI is not meant for regular in-cluster AIS buckets.

## Overview

At a high level, NBI works as follows:

1. A user explicitly creates an inventory for a remote bucket.
2. AIS runs a distributed `create-inventory` job across all storage nodes.
3. Each storage node lists the backend bucket, keeps only the names that belong to it, and stores them locally in inventory chunks.
4. Later, list-objects requests with the `LsNBI` flag - whether via CLI (`ais ls --inventory`), Go SDK, or Python SDK - are served from the stored snapshot instead of walking the backend again.

The create step uses nearly the same control structure as a normal bucket listing. In particular, inventory creation embeds `apc.LsoMsg`, with a number of validations and restrictions described below.

## Current status in v4.3

NBI is **experimental** in v4.3.

Current characteristics:

- manual creation only
- no periodic refresh
- no automatic resync
- no progress reporting yet
- one inventory per bucket is currently supported

## Creating an inventory

Use the `ais nbi create` command to create a native bucket inventory for a remote bucket.

Examples:

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

Inventory creation is implemented as a distributed xaction, so it shows up in regular job monitoring.

Example:

```console
$ ais show job create-inventory --all

create-inventory[inv-n9yTGoY8D] (ctl: ais://@G-pURnyMti/nnn, props:name,cached, flags:no-dirs)
NODE             ID              KIND                    BUCKET                  START      END        STATE
QNDt8082         inv-n9yTGoY8D   create-inventory        ais://@G-pURnyMti/nnn   16:45:39   16:55:29   Finished
RFht8081         inv-n9yTGoY8D   create-inventory        ais://@G-pURnyMti/nnn   16:45:39   16:55:34   Finished
VgAt8084         inv-n9yTGoY8D   create-inventory        ais://@G-pURnyMti/nnn   16:45:39   16:55:01   Finished
fgSt8085         inv-n9yTGoY8D   create-inventory        ais://@G-pURnyMti/nnn   16:45:39   16:55:31   Finished
lFkt8086         inv-n9yTGoY8D   create-inventory        ais://@G-pURnyMti/nnn   16:45:39   16:55:15   Finished
mUkt8083         inv-n9yTGoY8D   create-inventory        ais://@G-pURnyMti/nnn   16:45:39   16:55:39   Finished
```

In other words, NBI creation is "just another xaction" in AIS terms.

## Showing inventories

Use `ais show nbi` to inspect inventories stored for a bucket.

Examples:

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
$ ais show nbi ais://@remais/nnn --v
BUCKET                   NAME            OBJECT                               SIZE      OBJECTS  CHUNKS  TARGETS  SMAP  STARTED               FINISHED              PREFIX
ais://@G59v2CUztI/nnn    inv-d8p8-QavW   ais/@G59v2CUztI#/nnn/inv-d8p8-QavW   5.37MiB   33062    2       6        v45   2026-03-18T16:43:22Z  2026-03-18T16:43:34Z  -
```

## Listing objects using inventory

Once an inventory has been created, use it by adding `--inventory` to a regular `ais ls` command.

Examples:

```console
ais ls gs://abc --inventory
ais ls oci://abc --inventory --paged --prefix=subdir
ais ls s3://abc --inventory --inv-name my-first-inventory
```

This is still a normal list-objects request. The difference is that the request sets the `LsNBI` flag internally and switches to the inventory-backed path.

### Pagination

When listing via NBI, `page-size` is **best-effort and approximate**.

Because NBI listing is distributed across targets, each target returns an approximate share of the requested page size, subject to:

* local chunking
* minimum bounds
* slight overfetch

This behavior is intentional.

## How inventory-backed listing works

When the `LsNBI` flag is set on a list-objects request:

* the request uses a previously stored inventory snapshot
* AIS reads inventory chunks from local storage
* targets return their local portions
* the proxy merges results into a single listing stream

The result is that repeated listing avoids re-walking the remote backend.

The flag is defined as:

```go
LsNBI // list native bucket inventory (NBI) snapshot instead of walking the remote backend
```

## Inventory metadata

AIS stores metadata alongside each inventory. Current metadata includes:

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

This is expected.

In that case, a target stores a valid local inventory object with:

* zero size
* zero chunks
* valid NBI metadata

This allows AIS to distinguish:

* **inventory exists but is empty on this target**
* from
* **inventory does not exist**

This distinction matters for correct EOF behavior during distributed listing.

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

### Operational limitations

| Limitation | Description |
|---|---|
| Manual creation only | Inventories must be created explicitly by the user or API caller. |
| No periodic refresh | AIS does not yet rebuild inventories automatically. |
| No automatic resync | Inventories are snapshots, not self-updating views of the remote bucket. |
| No progress reporting yet | Creation runs as an xaction and can be monitored as a job, but there is no dedicated progress API yet. |
| One inventory per bucket | Current implementation supports a single inventory per bucket at a time. `--force` can be used to remove and recreate. |
| Experimental status | NBI is new in v4.3 and should be treated accordingly. |

## Legacy `--s3-inventory`

AIS currently supports both:

* `--inventory` for native bucket inventory (NBI)
* `--s3-inventory` for the older S3-specific inventory path

The legacy S3 path is deprecated and scheduled for removal.

NBI is the intended replacement.

Unlike the legacy S3 inventory support, NBI:

* is not limited to S3
* is not tied to provider-generated inventory formats
* does not require external tooling such as AWS CLI scripts
* is created and consumed entirely through AIS

## Examples

### Create and inspect an inventory

```console
ais nbi create gs://imagenet --inv-name trainset-v1
ais show nbi gs://imagenet --inv-name trainset-v1
ais show nbi gs://imagenet --inv-name trainset-v1 --verbose
```

### List via inventory

```console
ais ls gs://imagenet --inventory --inv-name trainset-v1
ais ls gs://imagenet/train/ --inventory --paged --page-size 1000
ais ls gs://imagenet --inventory --prefix train/dogs/
```

### Create inventory for a remote AIS bucket

```console
ais nbi create ais://@remote-cluster/datasets --inv-name snapshot-1
ais ls ais://@remote-cluster/datasets --inventory --inv-name snapshot-1
```

## Summary

Native bucket inventory is an AIS-native way to accelerate repeated listing of very large remote buckets.

It is especially useful when:

* the bucket namespace is very large
* the dataset is read-mostly
* repeated listing would otherwise keep hitting the backend
* the same bucket is listed many times over time

In v4.3, NBI is experimental, manual, and intentionally narrow in scope. Even so, it already provides a backend-agnostic replacement for the older S3-specific inventory integration and lays the foundation for future refresh, resync, and broader list-objects support.
