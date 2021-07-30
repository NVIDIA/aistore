---
layout: post
title: TROUBLESHOOTING
permalink: docs/troubleshooting
redirect_from:
 - docs/troubleshooting.md/
---

## Introduction

This text is intended to help troubleshooting AIStore operation. Easy-to-use TAB-completion based [CLI](/cmd/cli/README.md) is one of the first tools to consider, and of the first commands would be the one that shows the state of the cluster:

```console
$ ais show cluster <TAB>-<TAB>
...  proxy         smap          target
```

Meaning, you can run `ais show cluster` (the short version), and you can also run it with any of the additional completions listed above.

For example:

```console
$ ais show cluster
PROXY            MEM USED %    MEM AVAIL       UPTIME
202446p8082      0.06%         31.28GiB        19m
279128p8080      0.07%         31.28GiB        19m
928059p8081[P]   0.08%         31.28GiB        19m

TARGET           MEM USED %    MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %    REBALANCE      UPTIME
147665t8084      0.07%         31.28GiB        14%             2.511TiB        0.00%         -              19m
165274t8087      0.07%         31.28GiB        14%             2.511TiB        0.00%         -              19m
198815t8088      0.08%         31.28GiB        14%             2.511TiB        0.00%         -              19m
247389t8085      0.07%         31.28GiB        14%             2.511TiB        0.00%         -              19m
426988t8086      0.07%         31.28GiB        14%             2.511TiB        0.00%         -              19m
968103t8083      0.07%         31.28GiB        14%             2.511TiB        0.00%         -              19m
```

Since at any given time there's only one primary gateway, you may also find it useful to be able to designate a different one administratively. This is easy - example:

```console
$ ais cluster set-primary <TAB>-<TAB>
202446p8082  279128p8080  928059p8081
$ ais cluster set-primary 279128p8080
279128p8080 has been set as a new primary proxy
$ ais show cluster
PROXY            MEM USED %    MEM AVAIL       UPTIME
202446p8082      0.08%         31.28GiB        46m
279128p8080[P]   0.08%         31.28GiB        46m
928059p8081      0.08%         31.28GiB        46m10s
...
```

## Cluster Integrity Errors

The one category of errors that deserves special consideration is "cluster integrity". This category includes several numbered errors that may look as follows:

```
cluster integrity error `cie#50`:
Smaps have different origins: Smap v9[...111, t=9, p=3] vs p[232268p8080]: Smap v13[...281, t=4, p=4]
```

Above is an example of an actual error - `cie#50` in this case. Generally though, a cluster integrity violation is detected when a node that previously was (and possibly currently remains) a member of a cluster `A` tries to join a different cluster `B`. "Mixing up" nodes (in particular, storage targets) between different AIStore clusters triggers automated rebalancing with further complications that may be very difficult to sort out.

In many cases, the entirety of a troubleshooting step boils down to cleaning up the node's (obsolete) metadata - in particular, a copy of locally stored cluster map (aka Smap) and/or a copy of BMD. However, any type of metadata cleanup must be done with great caution after a careful review. To this end, the table below enumerates `cie` errors and provides some relevant context.

| Error | When | Description |
|--- | --- | --- |
| `cie#10` | When a primary proxy (gateway) starts up, it will use its own local (copy of) Smap to query other nodes for cluster-wide metadata. | The error indicates that either one of the nodes, or the primary itself, belongs (or did belong) to a different cluster. |
| `cie#30` | Same as above. | There are at least 2 targets in the cluster that "disagree" between themselves wrt their respective UUIDs. In other words, these two targets cannot be members of a single cluster. |
| `cie#40` | At node startup, or (secondly) when bucket metadata (BMD) changes at runtime. | In both cases, the node's local instance of bucket metadata conflicts with the cluster's version. |
| `cie#50` | Non-primary proxy or storage target: when receiving an updated cluster map that conflicts with the local copy. Primary proxy: when a joining node's Smap does not pass the validation. | In both cases, the node is not permitted to join (or is removed from) the cluster. |
| `cie#60` | When a primary proxy (gateway) is starting up, it uses its own local Smap to query other nodes for cluster-wide metadata. | The error is specific to bucket metadata and is triggered when there are two or more versions that are mutually incompatible. |
| `cie#70` | Same as above. | Same as above, except that there's a simple majority of nodes that have one of the BMD versions. |
| `cie#80` | Joining existing cluster | When node tries to join a cluster we do compare the node's local copy of the map with the existing one. The error, effectively, indicates that according to the node's own cluster map it must be a member of a different cluster. |

## Storage Integrity Error

Another category of errors is the "Storage Integrity Error (sie)," associated with mountpaths attached to the storage targets. A typical `sie` error may look as follows:

```
storage integrity error: sie#30 ... :
VMD is different ("/tmp/ais/7/3/.ais.vmd"): &{...} vs &{...}
```

Each AIStore target maintains a list of configured mountpaths ([more here](overview.md)) and their properties. This metadata is maintained across mountpath-changing events (such as disk faults and new attachments). It is also persisted and replicated across available mountpaths.

In addition, AIS target also stores its unique Node ID (aka `DaemonID`). This ID gets generated when the server joins an AIS cluster the first time and never changes during the server’s lifetime. The Node ID is recorded into each mountpath.

A troubleshooting step to deal with `sie` is cleaning up persisted metadata (i.e. removing the `.ais.vmd` file), recorded Node ID on mountpaths (erasing `user.ais.daemon_id` xattr), and/or updating the node deployment config. The table below enumerates `sie` errors and provides some relevant context.

**WARNING:** Caution while cleaning up the metadata on mountpaths. It could lead to loss or corruption of data.

| Error | When | Description |
|--- | --- | --- |
| `sie#10` | When mountpaths in deployment config have different Node ID recorded on them | The error indicates that either one of the mountpaths belongs (or did belong) to a different target |
| `sie#20` |  The target Node ID doesn't match persisted metadata | The Node ID assigned to the target node doesn't match Node ID recorded on a mountpath. Implies the mountpath has stale metadata and is/was part of a different target |
| `sie#30` | The persisted metadata on the mountpaths doesn't match | There are at least two mountpaths disagree on the metadata |
| `sie#40` | A mountpath has a corrupted metadata | At least one of the targets has a corrupted metadata file |
| `sie#50` | A mountpath is present in the config file but missing in persisted metadata | The target has valid metadata persisted on mountpaths, but the mountpaths present in the metadata don't match those provided in the config |
