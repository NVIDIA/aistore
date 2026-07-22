## Table of Contents

- [Highly Available Control Plane](#highly-available-control-plane)
    - [Bootstrap](#bootstrap)
    - [Target Smap recovery](#target-smap-recovery)
    - [Election](#election)
    - [Non-electable gateways](#non-electable-gateways)
    - [Metasync](#metasync)
- [Data Plane Availability](#data-plane-availability)
- [References](#references)

## Highly Available Control Plane

An AIStore cluster deployed with multiple gateways survives the loss of any storage target and any single gateway, including the primary gateway (leader).

New gateways and targets can join at any time, including while a new leader is being elected. Each node joining a running cluster receives the current cluster-level metadata.

Failover - the election of a new leader - is automatic when the current primary fails.

Failback - "administrative selection of a preferred gateway, often the originally designated primary" is performed manually through the AIStore [API](/docs/overview.md#aistore-apis).

An AIStore cluster should therefore be deployed with multiple proxies, also called gateways. The terms are used interchangeably throughout the source code and documentation.

Only one gateway acts as the primary; all others are non-primaries. The primary serializes updates to immutable, versioned cluster-level metadata.

Further:

- Each clustered node maintains a local copy of the cluster map (Smap)
- Each Smap instance is immutable and versioned, with a monotonically increasing version
- Only the current primary distributes Smap updates to the other clustered nodes
- Gateways persist cluster-level metadata under `ConfigDir`; storage targets additionally have mountpaths that can retain selected recovery metadata

### Bootstrap

A starting gateway performs three initial steps:

1. Load its local Smap, if one exists, and use it to discover the current cluster.
2. Use the local configuration and Smap to discover cluster-level metadata.
3. Use the discovered state and, optionally, [`AIS_PRIMARY_EP`](/docs/environment-vars.md) to determine whether to continue as the primary; otherwise, join the existing primary as a non-primary (secondary).

The primary-role decision is implemented by [`determineRole`](https://github.com/NVIDIA/aistore/blob/main/ais/earlystart.go).

A gateway that provisionally assumes the primary role then:

1. Initializes a working Smap containing itself.
2. Waits a configured amount of time for gateways and targets to self-join.
3. Reconciles the highest-version cluster metadata reported by those registrations with its locally loaded metadata.
4. Rediscovers cluster-wide metadata and resolves any remaining conflicts.

If the gateway determines during this sequence that another gateway is the primary, it joins that gateway as a non-primary.

### Target Smap recovery

Gateways have no data mountpaths, so their persistent cluster-level metadata resides under `ConfigDir`. Storage targets have both `ConfigDir` and data mountpaths.

A target normally loads and persists its Smap under `ConfigDir`. In addition, it keeps a best-effort backup copy on a single HRW-selected mountpath. The `ConfigDir` copy is always preferred: when it loads successfully, the mountpath backup is neither compared nor merged.

If the `ConfigDir` copy is missing or invalid at startup, the target scans all available mountpaths for Smap backups.

When multiple valid copies are found, the copy with the highest version is selected; the presence of multiple copies is reported (logged) but is not fatal.

The recovered Smap serves as a bootstrap seed, not as a competing source of truth. When the target self-joins, it reports the recovered cluster UUID and last-known member endpoints. A starting primary can thereby preserve cluster identity while rebuilding current membership from node registrations.

This intentionally differs from bucket metadata (BMD). BMD contains critical bucket identity and properties and is stored redundantly, with consistency checks across target mountpaths. Smap is largely reconstructed from node self-joins, so a single best-effort recovery copy is sufficient.

### Election

The primary gateway election process is as follows:

- A candidate to replace the failed primary is selected.
- The candidate is notified that an election is commencing.
- After confirming that the current primary is down, the candidate broadcasts vote requests to all other nodes.
- Each recipient confirms whether the current primary is down and whether the candidate has the HRW (Highest Random Weight) according to its local Smap.
- The recipient responds Yes when both conditions hold and No otherwise.
- Once the candidate receives a majority of affirmative responses, it performs the commit phase of the two-phase election by distributing an updated Smap to all nodes.

### Non-electable gateways

An AIStore cluster can be *stretched* to colocate redundant gateways with compute nodes. These non-electable local gateways serve as access points but never assume responsibility for leading the cluster.

### Metasync

AIStore has no centralized shared metadata service or corresponding single point of failure. Cluster-level metadata consists of immutable, versioned objects, including the cluster map, bucket metadata, global configuration, rebalance state, ETL metadata, and authentication tokens.

The primary distributes metadata updates through [metasync](https://github.com/NVIDIA/aistore/blob/main/ais/metasync.go). Recipients validate metadata identity and version, persist applicable updates, and converge on the primary's current versions.

## Data Plane Availability

While the control plane handles node membership and consistency of cluster-level metadata, the data plane has its own resilience mechanisms:

- [Filesystem Health Checker](/docs/fshc.md) - detects and isolates faulty storage
- [Data Protection](/docs/overview.md#data-protection) - protects data across nodes
- [Global Rebalancing](/docs/rebalance.md) - automatically redistributes data upon [node lifecycle](/docs/lifecycle_node.md) events

## References

- [Node lifecycle: maintenance mode, rebalance/rebuild, shutdown, decommission](/docs/lifecycle_node.md)
- [Blog: Split-brain is Inevitable](https://aistore.nvidia.com/blog/2025/02/16/split-brain-blog)
- [Filesystem Health Checker](/docs/fshc.md)
