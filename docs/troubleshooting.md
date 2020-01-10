## Introduction

This text is intended to help troubleshooting AIStore operation.

## Cluster Integrity Errors

The one category of errors that deserves special consideration is "cluster integrity". This category includes several numbered errors that may look as follows:

> cluster integrity error `cie#50`:
> Smaps have different origins: Smap v9[...111, t=9, p=3] vs p[232268p8080]: Smap v13[...281, t=4, p=4]

Above is an example of an actual error - `cie#50` in this case. Generally though, a cluster integrity violation is detected when a node that previously was (and possibly currently remains) a member of a cluster `A` tries to join a different cluster `B`. "Mixing up" nodes (in particular, storage targets) between different AIStore clusters triggers automated rebalancing with further complications that may be very difficult to sort out.

In many cases, the entirety of a troubleshooting step boils down to cleaning up the node's (obsolete) metadata - in particular, a copy of locally stored cluster map (aka Smap) and/or a copy of BMD. However, any type of metadata cleanup must be done with great caution after a careful review. To this end, the table below enumerates `cie` errors and provides some relevant context.

| Error | When | Description |
|--- | --- | --- |
| `cie#10` | When a primary proxy (gateway) starts up, it will use its own local (copy of) Smap to query other nodes for cluster-wide metadata. | The error indicates that either one of the nodes, or the primary itself, belongs (or did belong) to a different cluster. |
| `cie#30` | Same as above. | There are at least 2 targets in the cluster that "disagree" between themselves wrt their respective origin. In other words, these two targets cannot be members of a single cluster. |
| `cie#40` | At node startup, or (secondly) when bucket metadata (BMD) changes at runtime. | In both cases, the node's local instance of bucket metadata conflicts with the cluster's version. |
| `cie#50` | Non-primary proxy or storage target: when receiving an updated cluster map that conflicts with the local copy. Primary proxy: when a joining node's Smap does not pass the validation. | In both cases, the node is not permitted to join (or is removed from) the cluster. |
| `cie#60` | When a primary proxy (gateway) is starting up, it uses its own local Smap to query other nodes for cluster-wide metadata. | The error is specific to bucket metadata and is triggered when there are two or more versions that are mutually incompatible. |
| `cie#70` | Same as above. | Same as above, except that there's a simple majority of nodes that have one of the BMD versions. |
