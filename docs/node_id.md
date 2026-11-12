# AIStore Node IDs

Every AIS node - proxy (gateway) or target - has a node ID. The ID is unique within the cluster and shows up in the cluster map (Smap), logs, and CLI, e.g. `t[GBLgBUup]` and `p[FfIXNSzh]`.

For targets, the ID is more than a name: object placement is computed from target IDs. A target that restarts under a different ID is a different target, and a target that restarts under the same ID is expected to have the same data. Hence, two rules:

* node ID must not change across restarts;
* target ID stays with the target's [mountpaths](/docs/terminology.md#mountpath) that are based on respective data disks (or Kubernetes PVs).

This document describes how nodes get their IDs, where IDs are stored, and - for Kubernetes - how to derive IDs from pod names, and who is responsible for what.

## Table of Contents

- [How a node gets its ID](#how-a-node-gets-its-id)
- [Where the ID is stored](#where-the-id-is-stored)
- [Pod-derived IDs](#pod-derived-ids)
- [Responsibilities: Kubernetes operator vs AIS](#responsibilities-kubernetes-operator-vs-ais)
- [Scenarios](#scenarios)
- [References](#references)

## How a node gets its ID

At startup, each node goes through the following sources, in this order, and takes the first one that produces an ID:

| # | Proxy | Target |
| --- | --- | --- |
| 1 | pod-derived ID, when requested (see [below](#pod-derived-ids)) | pod-derived ID, when requested |
| 2 | `-daemon_id` command line, then `AIS_DAEMON_ID` environment | `-daemon_id` command line, then `AIS_DAEMON_ID` environment |
| 3 | Kubernetes only: hash of the Kubernetes node name (`MY_NODE`) | ID stored on the target's mountpaths |
| 4 | ID stored in the `.ais.proxy_id` file | new random ID |
| 5 | new random ID (then stored in `.ais.proxy_id`) | |

Notes:

* `-daemon_id` and `AIS_DAEMON_ID` are for advanced usage and testing.
* A target that finds no ID on its mountpaths but finds volume metadata (VMD) recovers the ID from the VMD instead of keeping the newly generated one.
* A random ID is 8 letters.

## Where the ID is stored

| Location | Node | Comment |
| --- | --- | --- |
| extended attribute `user.ais.daemon_id` at the root of each mountpath | target | written when the mountpath is added to the target |
| `.ais.vmd` (volume metadata) | target | replicated across the target's mountpaths |
| `.ais.proxy_id` under the configuration directory | proxy | random IDs only |
| `.ais.smap` (cluster map) | proxy and target | IDs of all nodes in the cluster |

A target validates the IDs stored on its mountpaths at startup. Any disagreement is a [storage integrity error](/docs/troubleshooting.md#storage-integrity-errors-sie), and the target exits.

A node also checks the cluster map it has stored locally: if that map does not list the node's own ID, the node exits. Remove stale copies of `.ais.smap` before restarting a node under a different ID.

## Pod-derived IDs

In Kubernetes, a node can compute its ID from the identity of its pod instead of generating a random one.

To enable, set the environment variable (or the equivalent `-daemon_id_source` command line) for both proxies and targets:

```console
AIS_DAEMON_ID_SOURCE=k8s-pod:<scope>
```

The ID is then a function of four inputs:

| Input | Source | Example |
| --- | --- | --- |
| node role | `-role` | `target` |
| namespace | `K8S_NS` | `ais` |
| pod name | `MY_POD` | `ais-target-1` |
| scope | the part after `k8s-pod:` | `6f1c2b7e-3d4a-4c5b-9e8f-0a1b2c3d4e5f` |

With the inputs from the table, the target ID is `GBLgBUup`:

```console
$ ais show cluster target
TARGET        MEM USED(%)  MEM AVAIL   CAP USED(%)  CAP AVAIL  LOAD AVERAGE   REBALANCE  UPTIME   K8s POD       STATUS
t[GBLgBUup]   3.08%        367.66GiB   51%          8.414TiB   [0.9 1.1 1.3]  -          12h10m   ais-target-1  online
```

Properties:

* Same inputs, same ID: on any Kubernetes node, after any number of restarts, and in every AIS release.
* A pod-derived ID has the same length and alphabet as a random ID (8 letters).
* The scope is opaque to AIS. Its purpose is to make IDs differ between AIS clusters that use the same namespace and pod names, e.g. in different Kubernetes clusters. `k8s-pod` with no scope is accepted and provides no such separation.

Rules:

* `MY_POD` and `K8S_NS` are required.
* Pod-derived IDs cannot be combined with `-daemon_id` or `AIS_DAEMON_ID`.
* Pod-derived IDs are for new clusters only. Do not enable them, and do not change the scope, in a cluster that already has data.
* Invalid or incomplete settings are fatal at startup.

### Targets: mountpaths must belong to the same target

When the target ID is pod-derived, the target checks every mountpath it is about to use. The ID stored on the mountpath (extended attribute, VMD) must either be absent or equal the pod-derived ID. Otherwise, the target exits with `sie#70` before writing anything to any of its mountpaths:

```text
storage integrity error sie#70: pod "ais-target-1" expects target ID "GBLgBUup",
mountpath "/ais/nvme2n1" belongs to "xTKdSUkl"
```

In other words, a target does not adopt disks of a different target, and does not adopt the identity found on the disks.

Disabled mountpaths are not checked at startup - same as with any other ID source. They are checked when enabled.

### Proxies

Proxies do not store pod-derived IDs: `.ais.proxy_id` is neither read nor written.

## Responsibilities: Kubernetes operator vs AIS

AIS does not talk to Kubernetes to establish node identity. It computes the ID from what it is given, and enforces the result against what it finds on disk. Everything else belongs to the deployment - in production, to the [AIS K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator).

### Kubernetes operator

* **Enable at cluster creation, never later.** Set `AIS_DAEMON_ID_SOURCE` (or `-daemon_id_source`) on all proxy and target containers of a new cluster. Enabling it in an existing cluster changes every node ID: targets exit with `sie#70`, proxies rejoin as new nodes.
* **Provide a scope and keep it constant.** The scope must be unique to the AIS cluster and must not change for as long as the cluster's volumes exist. The operator uses the UID of the AIStore custom resource. Note that deleting and recreating the custom resource produces a new UID: volumes retained from the previous incarnation are then rejected (`sie#70`).
* **Provide `MY_POD` and `K8S_NS`.**
* **Keep pod names stable.** The pod name is the node's identity. StatefulSet ordinals (`ais-target-0`, `ais-target-1`, ...) satisfy that.
* **Keep each target pod with its own volumes.** A given pod name must always get the same persistent volumes, wherever the pod runs. Per-pod volume claims of a StatefulSet satisfy that; host-path mounts do not when the pod moves to a different Kubernetes node.
* **Do not set `-daemon_id` or `AIS_DAEMON_ID`.**
* **Own the consequences of ephemeral storage.** See [blank volume](#scenarios) below.

### AIS

* Derive the ID deterministically from role, namespace, pod name, and scope; keep the derivation unchanged across releases.
* Exit at startup when the setting is invalid, when `MY_POD` or `K8S_NS` is missing, or when the setting is combined with `-daemon_id` or `AIS_DAEMON_ID`.
* On targets: reject mountpaths that carry a different ID (`sie#70`) before modifying any mountpath; write the ID to mountpaths that have none.
* Reject a mountpath that carries a different ID when it is attached or enabled at runtime (`sie#10`).
* Keep a node in the cluster map when it re-registers under the same ID while the primary is still retrying its previous incarnation.

### Not covered

* AIS does not validate the scope and does not query the Kubernetes API for it.
* AIS does not migrate existing clusters from random to pod-derived IDs.
* AIS does not detect that a target has returned with blank volumes under an ID that the cluster already knows.

## Scenarios

Pod-derived IDs enabled; `ais-target-1` is the pod in question.

| Event | Result |
| --- | --- |
| pod restarts, same volumes | same ID; the target loads its VMD and rejoins |
| pod moves to a different Kubernetes node, volumes follow | same as above |
| pod returns with blank volumes (ephemeral storage, replaced or wiped disks) | same ID; the target creates a new VMD and rejoins; the cluster map keeps a single entry for it |
| pod returns with volumes of a different target | `sie#70`; the target exits; no mountpath is modified |
| pod returns with some volumes blank and some of a different target | `sie#70`; the target exits; the blank volumes are not written to |
| scope changes, volumes retained | `sie#70` |
| a disabled mountpath carries a different ID | not checked at startup; rejected when enabled |

**Blank volume.** A target that returns with blank volumes under its previous ID is, for the rest of the cluster, the same target. Objects that it stored are gone, and the cluster is not notified. In buckets with a remote backend those objects are downloaded again upon the next (cold) GET. In `ais://` buckets with no redundancy they are lost. Deployments that keep target data on ephemeral storage accept this by design.

## References

* [Command-line arguments](/docs/command_line.md)
* [Environment variables](/docs/environment-vars.md)
* [System files](/docs/sysfiles.md)
* [Troubleshooting: storage integrity errors](/docs/troubleshooting.md#storage-integrity-errors-sie)
* [Node lifecycle](/docs/lifecycle_node.md)
* [AIS K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator)
