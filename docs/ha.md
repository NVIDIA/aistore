---
layout: post
title: HA
permalink: /docs/ha
redirect_from:
 - /ha.md/
 - /docs/ha.md/
---

## Table of Contents
- [Highly Available Control Plane](#highly-available-control-plane)
    - [Bootstrap](#bootstrap)
    - [Election](#election)
    - [Non-electable gateways](#non-electable-gateways)
    - [Metasync](#metasync)

## Highly Available Control Plane

AIStore cluster will survive a loss of any storage target and any gateway including the primary gateway (leader). New gateways and targets can join at any time – including the time of electing a new leader. Each new node joining a running cluster will get updated with the most current cluster-level metadata.
Failover – that is, the election of a new leader – is carried out automatically on failure of the current/previous leader. Failback on the hand – that is, administrative selection of the leading (likely, an originally designated) gateway – is done manually via AIStore [REST API](http_api.md).

It is, therefore, recommended that AIStore cluster is deployed with multiple proxies aka gateways (the terms that are interchangeably used throughout the source code and this README).

When there are multiple proxies, only one of them acts as the primary while all the rest are, respectively, non-primaries. The primary proxy's (primary) responsibility is serializing updates of the cluster-level metadata (which is also versioned and immutable).

Further:

- Each proxy/gateway stores a local copy of the cluster map (Smap)
- Each Smap instance is immutable and versioned; the versioning is monotonic (increasing)
- Only the current primary (leader) proxy distributes Smap updates to all other clustered nodes

### Bootstrap

The proxy's bootstrap sequence initiates by executing the following three main steps:

- step 1: load a local copy of the cluster map (Smap) and try to use it for the discovery of the current one;
- step 2: use the local configuration and the local Smap to perform the discovery of the cluster-level metadata;
- step 3: use all of the above and, optionally, [`AIS_PRIMARY_EP`](environment-vars.md) to figure out whether this proxy must keep starting up as a _primary_;
  - otherwise, join as a non-primary (a.k.a. _secondary_).

The rules to determine whether a given starting-up proxy is the primary one in the cluster - are simple. In fact, it's a single switch statement in the namesake function:

* [`determineRole`](https://github.com/NVIDIA/aistore/blob/main/ais/earlystart.go).

Further, the (potentially) primary proxy executes more steps:

- (i)    initialize empty Smap;
- (ii)   wait a configured time for other nodes to join;
- (iii)  merge the Smap containing newly joined nodes with the Smap that was previously discovered;
- (iiii) and use the latter to rediscover cluster-wide metadata and resolve remaining conflicts, if any.

If during any of these steps the proxy finds out that it must be joining as a non-primary then it simply does so.

### Election

The primary proxy election process is as follows:

- A candidate to replace the current (failed) primary is selected;
- The candidate is notified that an election is commencing;
- After the candidate (proxy) confirms that the current primary proxy is down, it broadcasts vote requests to all other nodes;
- Each recipient node confirms whether the current primary is down and whether the candidate proxy has the HRW (Highest Random Weight) according to the local Smap;
- If confirmed, the node responds with Yes, otherwise it's a No;
- If and when the candidate receives a majority of affirmative responses it performs the commit phase of this two-phase process by distributing an updated cluster map to all nodes.

### Non-electable gateways

AIStore cluster can be *stretched* to collocate its redundant gateways with the compute nodes. Those non-electable local gateways ([AIStore configuration](/deploy/dev/local/aisnode_config.sh)) will only serve as access points but will never take on the responsibility of leading the cluster.

### Metasync

By design, AIStore does not have a centralized (SPOF) shared cluster-level metadata. The metadata consists of versioned objects: cluster map, buckets (names and properties), authentication tokens. In AIStore, these objects are consistently replicated across the entire cluster – the component responsible for this is called [metasync](/ais/metasync.go). AIStore metasync makes sure to keep cluster-level metadata in-sync at all times.
