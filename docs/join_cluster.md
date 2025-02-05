---
layout: post
title: JOIN CLUSTER
permalink: /docs/join-cluster
redirect_from:
 - /join_cluster.md/
 - /docs/join_cluster.md/
---

**Note**: for the most recent updates on the topic of cluster membership and node lifecycle, please also check:

* [Node lifecycle: maintenance mode, rebalance/rebuild, shutdown, decommission](/docs/lifecycle_node.md)
* [CLI: `ais cluster` command](/docs/cli/cluster.md)

In particular, CLI `ais cluster add-remove-nodes` suppots adding, removing, and maintainiing nodes within the cluster. It allows administrators to dynamically adjust the cluster's composition, handle maintenance operations, and ensure availability and correctness during transitions when nodes are added or removed.

Also, see related:

* [Leaving aistore cluster](leave_cluster.md)
* [Global rebalance](rebalance.md)
* [CLI: `ais cluster` command](/docs/cli/cluster.md)
* [Scripted integration tests](https://github.com/NVIDIA/aistore/tree/main/ais/test/scripts)

## Joining a Cluster: _discovery_ URL

First, some basic facts. AIStore clusters can be deployed with an arbitrary number of AIStore proxies (a.k.a. gateways).

Each proxy/gateway implements RESTful APIs (both native and S3 compatible) and provides full access to user data stored in the cluster.

Each proxy collaborates with other proxies in the cluster to perform majority-voted HA failovers (section [Highly Available Control Plane](ha.md).

All _electable_ proxies are functionally equivalent. The one that is elected as _primary_ is, among other things, responsible to _join_ nodes to the running cluster.

To facilitate node-joining in presence of disruptive events, such as:

* network failures, and/or
* partial or complete loss of local copies of aistore metadata (e.g., cluster maps)

- to still be able to reconnect and restore operation, we also provide so called *original* and *discovery* URLs in the cluster configuration.

The latter is versioned, replicated, protected and distributed - solely by the elected primary.

> **March 2024 UPDATE**: starting v3.23, the *original* URL does _not_ track the "original" primary. Instead, the current (or currently elected) primary takes full responsibility for updating both URLs with the single and singular purpose: optimizing time to join or rejoin cluster.

For instance:

When and if an HA event triggers automated failover, the role of the primary will be automatically assumed by a different proxy/gateway, with the corresponding cluster map (Smap) update getting synchronized across all running nodes.

A new node, however, could potentially experience a problem when trying to join an already deployed and running cluster - simply because its configuration may still be referring to the old primary. The *original* and *discovery* URLs (see [AIStore configuration](/deploy/dev/local/aisnode_config.sh)) are precisely intended to address this scenario.
