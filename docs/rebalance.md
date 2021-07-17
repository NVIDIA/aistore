## Table of Contents

- [Global Rebalance](#global-rebalance)
- [CLI: usage examples](#cli-usage-examples)
- [Automated Resilvering](#automated-resilvering)

## Global Rebalance

To maintain [consistent distribution of user data at all times](https://en.wikipedia.org/wiki/Consistent_hashing#Examples_of_use), AIStore rebalances itself based on *new* versions of its [cluster map](/cluster/map.go).

More exactly:

* When storage targets join or leave the cluster, the current *primary* (leader) proxy transactionally creates the *next* updated version of the cluster map;
* [Synchronizes](/ais/metasync.go) the new map across the entire cluster so that each and every node gets the version;
* Which further results in each AIS target starting to traverse its locally stored content, recomputing object locations,
* And sending at least some of the objects to their respective *new* locations
* Whereby object migration is carried out via intra-cluster optimized [communication mechanism](/transport/README.md) and over a separate [physical or logical network](/cmn/network.go), if provisioned.

Thus, cluster-wide rebalancing is totally and completely decentralized. When a single server joins (or goes down in a) cluster of N servers, approximately 1/Nth of the entire namespace will get rebalanced via direct target-to-target transfers.

Further, cluster-wide rebalancing does not require any downtime.
Incoming GET requests for the objects that haven't yet migrated (or are being moved) are handled internally via the mechanism that we call "get-from-neighbor".
The (rebalancing) target that must (according to the new cluster map) have the object but doesn't, will locate its "neighbor", get the object, and satisfy the original GET request transparently from the user.

Similar to all other AIS modules and sub-systems, global rebalance is controlled and monitored via the documented [RESTful API](http_api.md).
It might be easier and faster, though, to use [AIS CLI](/docs/cli.md) - see next section.

## CLI: usage examples

1. Disable automated global rebalance (for instance, to perform maintenance or upgrade operations) and show resulting config in JSON on a randomly selected target:

```console
$ ais config cluster rebalance.enabled=false
config successfully updated

$ ais show config 361179t8088 --json | grep -A 6  rebalance

    "rebalance": {
        "dest_retry_time": "2m",
        "quiescent": "20s",
        "compression": "never",
        "multiplier": 4,
        "enabled": false
    },

```

2. Re-enable automated global rebalance and show resulting config section as a simple `name/value` list:

```console
$ ais config cluster rebalance.enabled=true
config successfully updated

$ ais show config <TAB-TAB>
125210p8082   181883t8089   249630t8087   361179t8088   477343p8081   675515t8084   70681p8080    782227p8083   840083t8086   911875t8085

$ ais show config 840083t8086 rebalance
PROPERTY                         VALUE   DEFAULT
rebalance.compression            never   -
rebalance.dest_retry_time        2m      -
rebalance.enabled                true    -
rebalance.multiplier             2       -
rebalance.quiescent              10s     -
```

3. Monitoring: notice per-target statistics and the `EndTime` column

```console
$ ais show rebalance
DaemonID     RebID   ObjRcv  SizeRcv  ObjSent  SizeSent  StartTime       EndTime          Aborted
======       ======  ======  ======   ======   ======    ======          ======           ======
181883t8089  1       0       0B       1058     1.27MiB   04-28 16:05:35  <not completed>  false
249630t8087  1       0       0B       988      1.18MiB   04-28 16:05:35  <not completed>  false
361179t8088  1       5029    6.02MiB  0        0B        04-28 16:05:35  <not completed>  false
675515t8084  1       0       0B       989      1.18MiB   04-28 16:05:35  <not completed>  false
840083t8086  1       0       0B       974      1.17MiB   04-28 16:05:35  <not completed>  false
911875t8085  1       0       0B       1020     1.22MiB   04-28 16:05:35  <not completed>  false

$ ais show rebalance
DaemonID     RebID   ObjRcv  SizeRcv  ObjSent  SizeSent  StartTime       EndTime         Aborted
======       ======  ======  ======   ======   ======    ======          ======          ======
181883t8089  1       0       0B       1058     1.27MiB   04-28 16:05:35  04-28 16:05:53  false
249630t8087  1       0       0B       988      1.18MiB   04-28 16:05:35  04-28 16:05:53  false
361179t8088  1       5029    6.02MiB  0        0B        04-28 16:05:35  04-28 16:05:53  false
675515t8084  1       0       0B       989      1.18MiB   04-28 16:05:35  04-28 16:05:53  false
840083t8086  1       0       0B       974      1.17MiB   04-28 16:05:35  04-28 16:05:53  false
911875t8085  1       0       0B       1020     1.22MiB   04-28 16:05:35  04-28 16:05:53  false
```

4. Since global rebalance is an [extended action (xaction)](/xaction/README.md), it can be also monitored via generic `show xaction` API:

```console
$ ais show job xaction rebalance
NODE             ID      KIND            BUCKET  OBJECTS         BYTES           START           END     ABORTED
181883t8089      g2      rebalance       -       1058            1.27MiB         04-28 16:10:14  -       false
...
```

5. Finally, you can always start and stop global rebalance administratively, for instance:


```console
$ ais job start rebalance
```

## Automated Resilvering

While rebalance (previous section) takes care of the cluster *grow* and *shrink* events, resilver, as the name implies, is responsible for the [mountpath](overview.md#terminology) *added* and [mountpath](overview.md#terminology) *removed* events handled locally within (and by) each storage target.

In other words, global rebalance handles scaling (up and down) of the entire AIS cluster while automated *resilvering* takes care of disk attachments and disk faults within a given storage node.

* A [mountpath](overview.md#terminology) is a single disk **or** a volume (a RAID) formatted with a local filesystem of choice, **and** a local directory that AIS utilizes to store user data and AIS metadata. A mountpath can be disabled and (re)enabled, automatically or administratively, at any point during runtime. In a given cluster, a total number of mountpaths would normally compute as a direct product of `(number of storage targets) x (number of disks in each target)`.

As stated, mountpath removal can be done administratively (via API) or be triggered by a disk fault (see [filesystem health checking](/health/fshc.md).
Irrespectively of the original cause, mountpath-level events activate resilver that in many ways performs the same set of steps as the rebalance.
The one salient difference is that all object migrations are local (and, therefore, relatively fast(er)).

### CLI Usage

Resilvering can be run on a specific target node or the entire cluster (when all targets execute resilvering in parallel).

Similar to global rebalancing, resilvering is a managed *eXtended operation* or [xaction](ic.md).
All xactions execute asyncrhonously and support a common set of documented APIs to start, terminate the xaction, inquire its progress, etc. The progress of resilvering can be monitored via `ais show job xaction` CLI.

Examples:

```console
$ ais advanced resilver # all targets will be resilvered
Started resilver "NGxmOthtE", use 'ais show job xaction NGxmOthtE' to monitor progress

$ ais advanced resilver BUQOt8086  # resilver a single node
Started resilver "NGxmOthtE", use 'ais show job xaction NGxmOthtE' to monitor progress
```

Automated resilvering can also be disabled. Just like with `rebalance`, the resulting config can be viewed through the CLI:
NOTE: When automated resilvering is disabled, removing a mountpath may result in data loss.

```console
$ ais config cluster resilver.enabled=false
config successfully updated

$ ais show config 361179t8088 resilver --json | grep -A 2 resilver
    "resilver": {
        "enabled": false
    },

$ ais config cluster resilver.enabled=true
config successfully updated

$ ais show config <TAB-TAB>
125210p8082   181883t8089   249630t8087   361179t8088   477343p8081   675515t8084   70681p8080    782227p8083   840083t8086   911875t8085

$ ais show config 361179t8088 resilver
PROPERTY                 VALUE
resilver.enabled         true
```

## IO Performance

During rebalancing, response latency and overall cluster throughput may substantially degrade.
