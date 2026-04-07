# Native Bucket Inventory (NBI)

Native bucket inventory (NBI) lets AIS create and serve a **local snapshot** of a remote bucket namespace.

It is designed for **very large, read-mostly buckets** - for example, training and inference datasets - where repeatedly listing the remote backend is expensive, slow, and operationally noisy.

Native bucket inventory (NBI) was introduced in [v4.3](https://github.com/NVIDIA/aistore/releases/tag/v1.4.3) and is stable as of **4.4**.

Inventories are currently created manually: AIS does not yet provide built-in periodic refresh or automatic resynchronization.

This document occasionally references Go types and constants from the AIS codebase. For SDK usage, see the [Go API](https://github.com/NVIDIA/aistore/tree/main/api) and [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk) documentation.

## Table of Contents

- [Motivation](#motivation)
- [Supported buckets](#supported-buckets)
- [Overview](#overview)
- [System bucket](#system-bucket)
- [Creating an inventory](#creating-an-inventory)
  - [Defaults and stored properties](#defaults-and-stored-properties)
- [Monitoring inventory creation](#monitoring-inventory-creation)
- [Showing inventories](#showing-inventories)
- [Listing via inventory](#listing-via-inventory)
  - [Pagination](#pagination)
  - [How inventory-backed listing works](#how-inventory-backed-listing-works)
  - [Non-recursive listing](#non-recursive-listing)
- [Inventory metadata](#inventory-metadata)
- [Small buckets and empty local inventories](#small-buckets-and-empty-local-inventories)
- [Limitations and validation](#limitations-and-validation)
  - [Why creation is narrower than regular listing](#why-creation-is-narrower-than-regular-listing)
  - [Restrictions during inventory creation](#restrictions-during-inventory-creation)
  - [Restrictions during inventory-backed listing](#restrictions-during-inventory-backed-listing)
- [Examples](#examples)
- [Performance](#performance)

## Motivation

NBI replaces the older S3-specific inventory path.

Compared to that legacy approach, NBI is:

- **AIS-native**: created and consumed via AIS API, CLI, and xactions
- **backend-agnostic**: works with remote buckets generally, not only S3
- **format-independent**: does not depend on provider-generated inventory formats such as CSV, ORC, or Parquet
- **operationally simpler**: does not require provider-specific scripts or external tooling

In practice, NBI turns repeated remote listing into a two-step workflow:

1. create a reusable snapshot once
2. serve subsequent listings from that stored snapshot

## Supported buckets

NBI applies to **remote buckets** only: cloud buckets (S3, GCS, Azure, OCI) and remote AIS buckets.

It is not currently supported for in-cluster `ais://` buckets. The main reason is simple: NBI exists to avoid the latency and repeated backend traffic of **remote** listing. For local AIS buckets, namespace metadata is already local and served at native AIS speed, so an additional inventory layer would bring little practical benefit.

> A point-in-time snapshot of an in-cluster bucket may still be a valid future use case, but that is not part of the current implementation.

## Overview

At a high level, NBI has two phases:

1. **create** a reusable inventory snapshot for a remote bucket
2. **list** from that snapshot instead of re-walking the remote backend

The key design point is that inventories are stored as a **flat, lexicographically ordered snapshot** of object names plus selected properties. Listing behavior is derived from that stored snapshot.

### Create path

When a user creates an inventory:

1. The proxy initiates a distributed two-phase operation across all targets.
2. During the Begin phase, each target validates the request, checks capacity, ensures that no other `create-inventory` job is running for the same bucket, and enforces the current one-inventory-per-bucket limit.
3. If `--force` is specified, any existing inventory for the bucket is removed first.
4. During Commit, each target runs a `create-inventory` xaction: it lists the remote bucket, keeps only the object names that belong to it under the current cluster map, and writes them locally as inventory chunks.

> In the current implementation, all targets independently list the remote backend. A future optimization may designate a single target to list once and distribute the results.

### Listing path

When a user lists with `--inventory`, AIS serves the request from NBI.

The flow is:

1. The request arrives at the proxy as a normal list-objects operation.
2. AIS sets the `LsNBI` flag internally and switches to the inventory-backed path.
3. The proxy broadcasts the request to all targets.
4. Each target reads its local inventory chunks and returns its portion.
5. The proxy merges the per-target responses into a single sorted result.

The result is a normal AIS list response, but without walking the remote backend on every call.

> S3-compatible clients may also request NBI-backed listing by sending the `Ais-Bucket-Inventory: true` header; `Ais-Inv-Name` may be used to select a specific inventory.

### Example

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

The same `ais ls` without `--inventory` produces the same namespace view but re-walks the remote backend each time.

## System bucket

Inventories are stored as chunked objects in a designated **system bucket** - a special AIS bucket whose name starts with a dot (`.`).

> System buckets are AIS infrastructure and are not intended for direct user access. The dot-prefix naming convention (`.sys-*`) is reserved.
> For details on naming rules, visibility, and future plans, see [System Buckets](/docs/bucket.md#system-buckets).

The first and currently only system bucket is:

```text
ais://.sys-inventory
```

It is created automatically on the first inventory creation request. There is no need to create it manually.

Creating an inventory requires admin permissions. When AIStore is deployed with the [Authentication Server (AuthN)](/docs/authn.md), the operation requires `admin` [permissions](/docs/authn.md#permissions). Clusters without AuthN do not enforce this check.

```console
$ ais ls ais://.sys-inventory
NAME                                              SIZE
aws/@#/training-data-v5/inv-Tp4nR7kWx             1.28GiB

$ ais ls ais://.sys-inventory --props chunked,size
NAME                                              CHUNKED         SIZE
aws/@#/training-data-v5/inv-Tp4nR7kWx             yes             1.28GiB
```

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
* Inventory names are optional, but if specified they must be unique for the given bucket.
* When omitted, AIS generates a unique inventory name automatically (for example, `inv-Tp4nR7kWx`).
* `--force` removes any existing inventory for the bucket first and then proceeds with creation.
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

### Defaults and stored properties

Inventory creation validates and normalizes the embedded `LsoMsg`.

Current behavior includes:

* `continuation_token` must be empty
* `start_after` is not supported
* several list flags are rejected because they do not make sense for inventory creation
* `LsNoDirs` is always set internally

The default stored properties are:

* `name`
* `size`
* `cached`

If properties are explicitly requested, AIS still adds `cached`.

Advanced tunables:

* default `names_per_chunk`: `2 * MaxPageSizeAIS` (20K)
* minimum `names_per_chunk`: `2`
* maximum `names_per_chunk`: `64 * MaxPageSizeAIS` (640K)

## Monitoring inventory creation

Inventory creation is a distributed xaction, so it appears in regular job monitoring:

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

Each target stores only the subset of names that belongs to it under the current cluster map. The total is the size of the full bucket namespace captured by the inventory.

## Showing inventories

Use `ais show nbi` to inspect existing inventories.

> As with other `ais show` subcommands, `ais show nbi` is an alias for `ais nbi show`.

```console
$ ais show nbi --help
NAME:
   ais show nbi - Show bucket inventory or all matching inventories,
   e.g.:
     * ais show nbi                                         - show all inventories in the cluster;
     * ais show nbi s3:                                     - show inventories for all s3:// buckets;
     * ais show nbi s3://abc                                - show inventory details for the bucket;
     * ais show nbi s3://abc --inv-name my-first-inventory  - show specific named inventory.

USAGE:
   ais show nbi [BUCKET] [command options]

OPTIONS:
   inv-name   Bucket inventory name (optional; omit to match any inventory)
   verbose,v  Verbose output
   help, h    Show help
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

## Listing via inventory

Once an inventory has been created, use it by adding `--inventory` to a regular `ais ls` command:

```console
ais ls gs://abc --inventory
ais ls oci://abc --inventory --paged --prefix subdir
ais ls s3://abc --inventory --inv-name my-first-inventory
ais ls s3://abc --inventory --nr --prefix images/
```

`--inventory` now means NBI-backed listing.

This is still a normal list-objects request. The difference is that AIS sets the `LsNBI` flag internally and serves the request from pre-stored inventory chunks rather than walking the remote backend.

In the Python SDK, the corresponding flag is [`ListObjectFlag.NBI`](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/list_object_flag.py).

### Pagination

When listing via NBI, page size is **best-effort and approximate**.

Because NBI listing is distributed across targets, each target returns an approximate share of the requested page size, subject to local chunking and minimum bounds. This behavior is intentional: it reduces roundtrips and improves list latency.

### How inventory-backed listing works

When inventory-backed listing is requested, AIS serves the request from pre-stored inventory chunks on each target rather than walking the remote backend.

The flow is:

1. A list-objects-page request with `--inventory` arrives at the proxy.
2. The proxy broadcasts the request to all targets.
3. Each target seeks to the first inventory name greater than the provided continuation token within its local, lexicographically ordered chunks.
4. Each target returns its next locally sorted batch, together with its own continuation token if more names remain.
5. The proxy merges the returned names into a single sorted stream and applies min-token logic:

   * the global continuation token is the minimum non-empty per-target continuation token
   * the merged result is truncated to names not greater than that minimum token

Because continuation tokens are literal object names rather than opaque cursors, the process is deterministic, stateless on the proxy, and relatively easy to reason about.

Repeated listing therefore avoids re-walking the remote backend.

### Non-recursive listing

Inventory creation always stores a **flat snapshot**. Non-recursive behavior, when requested during listing, is derived later as a **view** over that flat snapshot rather than stored as a separate hierarchical inventory format.

That distinction is important:

* **creation** remains flat and recursive by design
* **listing** may still present immediate files and subdirectories only

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

When an inventory contains relatively few object names compared with the number of targets, some targets may end up with no local inventory entries.

That is expected. In such cases, a target still stores a valid local inventory object with zero size, zero chunks, and valid NBI metadata. This allows AIS to distinguish between:

* **inventory exists but is empty on this target**
* **inventory does not exist**

That distinction matters for correct EOF behavior during distributed listing.

## Limitations and validation

NBI is intentionally narrower than general remote listing.

A useful mental model is:

* **inventory creation** is a snapshot-building job
* **inventory listing** is a fast read path over that stored snapshot

Those two phases do not accept the same controls.

### Why creation is narrower than regular listing

Inventory creation is not an ad hoc `ls` request. It builds a reusable artifact that must be:

* complete within the requested scope
* deterministic for a given bucket, prefix, property set, and cluster map
* easy to store, merge, and paginate later
* represented as a flat, lexicographically ordered namespace snapshot

That is why several options that make sense for a live list request do **not** make sense during inventory generation.

### Restrictions during inventory creation

| Option / behavior                   | Description                                                                                              |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------- |
| non-empty `continuation_token`      | Inventory creation always starts from the beginning of the selected namespace.                           |
| `start_after`                       | Not supported during inventory generation.                                                               |
| `--cached` (`LsCached`)             | Not applicable. Inventory creation walks the remote bucket namespace, not only currently cached content. |
| `--not-cached` (`LsNotCached`)      | Not applicable for the same reason.                                                                      |
| `LsMissing`                         | Not supported during creation.                                                                           |
| `LsDeleted`                         | Not supported during creation.                                                                           |
| `--archive` / `LsArchDir`           | Inventory creation does not expand archive contents.                                                     |
| `LsBckPresent`                      | Not applicable. Inventory creation operates on a concrete remote bucket request.                         |
| `LsDontHeadRemote`                  | Inventory creation requires normal remote-bucket validation.                                             |
| `LsDontAddRemote`                   | The bucket must participate in normal AIS metadata flow.                                                 |
| `lsWantOnlyRemoteProps`             | NBI creation stores selected inventory properties rather than exposing backend metadata only.            |
| `--non-recursive` (`LsNoRecursion`) | Inventory creation always stores a flat snapshot. Non-recursive behavior is a listing-time view.         |
| `--diff` (`LsDiff`)                 | Inventory creation is not a diff job.                                                                    |
| `LsIsS3`                            | Not applicable.                                                                                          |
| `LsNoDirs`                          | Always set internally; directories are never stored in inventory snapshots.                              |

### Restrictions during inventory-backed listing

| Option / behavior              | Description                                                                                                           |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `start_after`                  | Not supported for inventory-backed listing. Use prefix and continuation-based pagination instead.                     |
| `--not-cached` (`LsNotCached`) | Not applicable. NBI serves a stored snapshot rather than computing "not present in cluster" against the live backend. |
| `LsMissing`                    | Not supported. Inventory listing is not a missing-copy inspection mode.                                               |
| `LsDeleted`                    | Not supported. Inventory snapshots do not currently track objects marked for deletion.                                |
| `--archive` / `LsArchDir`      | NBI lists stored object entries, not archive contents.                                                                |
| `lsWantOnlyRemoteProps`        | NBI is not a pass-through remote-property listing path.                                                               |
| `--diff` (`LsDiff`)            | NBI does not perform remote-versus-cluster diff during listing.                                                       |

In short, NBI is optimized for fast, repeated listing of a previously captured bucket snapshot.

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
$ ais ls gs://imagenet --inventory --nr --prefix train/
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

## Performance

A latency-vs-scale benchmark comparing NBI listing, regular AIS remote listing, and direct S3 access (`boto3`) from 1K to 80K objects is available at [`python/tests/perf/nbi/`](../python/tests/perf/nbi/README.md).
