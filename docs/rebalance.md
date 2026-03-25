## Global Rebalance

Global rebalance is the cluster-wide process of moving objects to their **proper** storage targets after a topology change.

In AIStore, object placement is not arbitrary and is not tracked via a central directory. Instead, every object has a uniquely defined destination computed from the current cluster state. When that state changes, some objects acquire a new correct location. Global rebalance is the decentralized background process that brings the cluster into agreement with the new placement.

Global rebalance is one of the core AIS mechanisms that makes it possible to grow or shrink a cluster, return nodes to service, and perform maintenance without taking the cluster offline.

**Table of Contents**

- [Placement: HRW and the three inputs](#placement-hrw-and-the-three-inputs)
- [What triggers global rebalance](#what-triggers-global-rebalance)
- [How global rebalance works](#how-global-rebalance-works)
- [Serving reads during migration](#serving-reads-during-migration)
- [Control and monitoring](#control-and-monitoring)
- [CLI: usage examples](#cli-usage-examples)
- [Starting rebalance administratively](#starting-rebalance-administratively)
- [Rebalance vs. resilver](#rebalance-vs-resilver)
- [Performance considerations](#performance-considerations)

## Placement: HRW and the three inputs

AIStore uses a variant of **highest random weight (HRW)**, also known as rendezvous hashing, to determine object placement.

At the cluster level, the destination target for an object is uniquely determined by the following three inputs:

- the current **cluster map**
- the fully qualified **bucket**, including provider and namespace
- the **object name**

This is the key to understanding rebalance.

When the cluster map changes, the HRW result may change as well. For some subset of objects, a different target becomes the correct owner. Those are the objects that must be moved.

AIS uses the same general idea internally as well. Within a storage target, object placement across local disks is also HRW-based, except that instead of the cluster map AIS uses the set of local [`mountpaths`](/docs/terminology.md#mountpath).

## What triggers global rebalance

Global rebalance is triggered by changes that affect cluster-wide target placement.

Typical examples include:

- a storage target joining the cluster
- a storage target leaving the cluster
- putting a target into maintenance
- decommissioning a target
- bringing a target back into active service

A useful rule of thumb is:

> if a topology change can alter the HRW destination for stored objects, it can trigger global rebalance.

When a single target is added to or removed from a cluster of `N` targets, the fraction of objects that move is typically on the order of `1/N`, though the exact amount always depends on the topology and the current object distribution.

For additional background on maintenance, shutdown, and decommission workflows, see [Node lifecycle: maintenance, shutdown, decommission](/docs/lifecycle_node.md).

## How global rebalance works

Global rebalance is fully decentralized.

At a high level:

1. The current primary proxy creates the next version of the cluster map.
2. The new map is distributed cluster-wide so that all nodes converge on the same version.
3. Each storage target starts traversing its locally stored objects.
4. For every object, the target recomputes the correct destination under the new map.
5. If the local target is no longer the correct owner, the object is sent directly to the proper target.
6. The process completes when all participating targets finish migrating the objects that no longer belong locally.

A few points are worth emphasizing:

- there is no central data-movement coordinator that decides ownership object by object
- each target independently evaluates the objects it currently stores
- object migration is performed via AIS intra-cluster transfers
- when provisioned, rebalance traffic can use separate intra-cluster networking

This design keeps rebalancing scalable and avoids turning the primary into a data path bottleneck.

## Serving reads during migration

Global rebalance does **not** require cluster downtime.

During migration, incoming reads may target objects that have not yet moved, or are in the middle of being moved. AIS handles this transparently.

In particular, the target that must own an object according to the **new** cluster map can internally locate a neighbor that still has the object and retrieve it on demand. This internal flow is often described as **get-from-neighbor**.

As a result:

- applications do not need to stop I/O while rebalance is running
- object movement remains transparent to clients
- the cluster can continue converging toward the new placement while still serving reads

## Control and monitoring

Global rebalance is controlled and monitored via:

- native HTTP-based APIs
- the [Go API](https://github.com/NVIDIA/aistore/tree/main/api)
- the [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk)
- the [Command Line Interface (CLI)](/docs/cli.md)

Operationally, administrators typically care about three things:

- whether automated rebalance is enabled
- whether a rebalance is currently running
- how much data each target has sent and received for the current rebalance

Like other long-running AIS activities, rebalance is tracked as a cluster job and can be inspected while in progress.

## CLI: usage examples

### Show current rebalance configuration

```console
$ ais config cluster rebalance --json
{
    "rebalance": {
        "burst_buffer": 1024,
        "dest_retry_time": "2m",
        "enabled": true,
        "compression": "never",
        "bundle_multiplier": 2
    }
}
```

> **Note**: As elsewhere in the documentation, `http://localhost:8080` in the examples below denotes a [local playground](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#local-playground) endpoint and should be understood as a placeholder for an arbitrary [AIS endpoint (`AIS_ENDPOINT`)](/docs/terminology.md#endpoint).

> Likewise, target names such as `t[kOHt8086]` are example node identifiers from a test cluster; production deployments will use their own node names, addresses, and ports.

### Trigger rebalance by putting a target into maintenance

First, populate a bucket:

```console
$ MODE=debug make aisloader
Building aisloader... done.

$ aisloader -bucket=ais://nnn -pctput=100 -duration=3m -numworkers=4 -maxsize 1k -minsize 1k -cleanup=false -quiet -epochs 7
Runtime configuration:
{
   "proxy": "http://localhost:8080",
   "bucket": "ais://nnn",
   "duration": "3m0s",
   "# workers": 4,
   "stats interval": "10s",
   "% PUT": 100,
   "minimum object size (bytes)": 1024,
   "maximum object size (bytes)": 1024,
   "name-getter": "random non-unique",
   "reader-type": "sg",
   "cleanup": false
}

Time      OP    Count                   Size (Total)            Latency (min, avg, max)                 Throughput (Avg)        Errors (Total)
10:13:34  PUT   105,552 (105,552 4 0)   103.1MiB (103.1MiB)     260.466µs  356.374µs  7.007ms           10.31MiB/s (10.31MiB/s) -
10:13:44  PUT   99,826 (205,378 4 0)    97.5MiB (200.6MiB)      269.811µs  369.751µs  185.490ms         9.75MiB/s (10.03MiB/s)  -
10:13:54  PUT   100,580 (305,958 4 0)   98.2MiB (298.8MiB)      268.668µs  366.325µs  136.984ms         9.82MiB/s (9.96MiB/s)   -
10:14:04  PUT   97,292 (403,250 4 0)    95.0MiB (393.8MiB)      269.781µs  377.38µs   203.156ms         9.50MiB/s (9.84MiB/s)   -
10:14:14  PUT   93,186 (496,436 4 0)    91.0MiB (484.8MiB)      267.808µs  394.835µs  155.001ms         9.10MiB/s (9.70MiB/s)   -
^C
```

Verify the object count before rebalance:

```console
$ ais ls ais://nnn --count-only
Listed 504,797 names in 1.69s
```

Put one target into maintenance. This changes the active target set and starts global rebalance:

```console
$ ais cluster add-remove-nodes start-maintenance t[kOHt8086] --yes
Started rebalance "g1" (to monitor, run 'ais show rebalance').
t[kOHt8086] is now in maintenance mode
```

Monitor rebalance progress:

```console
$ ais show rebalance --refresh 30
REB ID   NODE       OBJECTS RECV   SIZE RECV   OBJECTS SENT   SIZE SENT   START      END   STATE
g1       lJjt8082   33404          32.62MiB    0              0B          10:14:56   -     Running
g1       nZDt8085   33108          32.33MiB    0              0B          10:14:56   -     Running
g1       pAXt8084   34088          33.29MiB    0              0B          10:14:56   -     Running
g1       saLt8081   33434          32.65MiB    0              0B          10:14:56   -     Running
g1       yCut8083   33726          32.94MiB    0              0B          10:14:56   -     Running
```

In this example, the target placed into maintenance is the one draining data, while the remaining active targets receive it.

Verify the object count after rebalance:

```console
$ ais ls ais://nnn --count-only
Listed 504,797 names in 1.7s
```

The identical counts before and after rebalance show that the cluster converged to the new placement without losing objects.

## Starting rebalance administratively

Global rebalance normally starts automatically - triggered by a topology change that alters object placement. It can also be started administratively, on demand.

Like a few other AIS xactions, rebalance is directly startable from the CLI:

```console
$ ais start <TAB>  ## select one of the startable jobs - a strict subset of all supported xactions

prefetch           dsort              rebalance          mirror             warm-up-metadata
blob-download      lru                resilver           ec-encode          copy-bck
download           etl                cleanup            rechunk            move-bck
```

The `rebalance` command also supports (advanced-usage) bucket-scoped operation and related synchronization/versioning options:

> Bucket scope is advanced usage and generally should be avoided unless you know exactly why a partial, bucket-level rebalance is appropriate. Generally, the cluster should be allowed to rebalance globally.

```console
$ ais start rebalance --help
NAME:
   ais start rebalance - Rebalance ais cluster

USAGE:
   ais start rebalance [BUCKET[/PREFIX]] [command options]

OPTIONS:
   latest     Check in-cluster metadata and, possibly, GET, download, prefetch, or otherwise copy the latest object version
              from the associated remote bucket;
              the option provides operation-level control over object versioning (and version synchronization)
              without the need to change the corresponding bucket configuration: 'versioning.validate_warm_get';
              see also:
                - 'ais show bucket BUCKET versioning'
                - 'ais bucket props set BUCKET versioning'
                - 'ais ls --check-versions'
              supported commands include:
                - 'ais cp', 'ais prefetch', 'ais get', 'ais start rebalance'
   prefix     Select virtual directories or objects with names starting with the specified prefix, e.g.:
              '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
              '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   sync       Fully synchronize in-cluster content of a given remote bucket with its (Cloud or remote AIS) source;
              the option is, effectively, a stronger variant of the '--latest' (option):
              in addition to bringing existing in-cluster objects in-sync with their respective out-of-band updates (if any)
              it also entails removing in-cluster objects that are no longer present remotely;
              like '--latest', this option provides operation-level control over synchronization
              without requiring to change the corresponding bucket configuration: 'versioning.synchronize';
              see also:
                - 'ais show bucket BUCKET versioning'
                - 'ais bucket props set BUCKET versioning'
                - 'ais start rebalance'
                - 'ais ls --check-versions'
   help, h    Show help
```

## Rebalance vs. resilver

Both rebalance and resilver restore HRW-based placement, but they do so at different scopes.

Global rebalance restores placement across the **cluster**. When the active target set changes, some objects acquire a new HRW destination under the current cluster map, and AIS moves those objects **from one storage node to another**.

Resilver restores placement within a **single target**. When the local set of [`mountpaths`](/docs/terminology.md#mountpath) changes, some objects acquire a new local destination on that node, and AIS moves those objects **across mountpaths on the same target**.

In short:

> rebalance is cluster-wide and inter-target; resilver is local and intra-target.

For more on resilver, see [Resilver](/docs/resilver.md).

## Performance considerations

Rebalancing moves data in the background and competes for I/O, CPU, memory, and network resources.

As a result, during rebalance:

* response latency may increase
* aggregate throughput may decrease
* the impact may be more visible on heavily utilized clusters

The exact overhead depends on multiple factors, including:

* the amount of data that must move
* the size and shape of the topology change
* object sizes
* local disk performance
* available network bandwidth
* whether separate intra-cluster networking is provisioned

For this reason, administrators often tune rebalance behavior and may temporarily disable automated rebalance while performing planned maintenance or staged upgrades.
