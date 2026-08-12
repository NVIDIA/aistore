There's a set of system-management topics that often appear under alternative subtitles:
"graceful termination and cleanup", "shutting down and restarting", "adding and removing members", "joining and leaving cluster", and similar.

All of these topics involve state transitions, so let's start by naming the states and the transitions between them.

![Node lifecycle: states and transitions](images/lifecycle-graceful-term.png)

To put things in perspective, this picture is about a node (not shown) in an AIStore cluster (also not shown).

Tracking it from top to bottom, first notice the state called **maintenance mode**. This is the gentlest way to remove a node from an operating cluster. When in maintenance, the node stops keepalive heartbeats but remains in the cluster map and remains connected, unless you disconnect or shut it down manually, which is perfectly valid and often expected.

Next comes **shutdown**. Graceful shutdown can also be achieved in a single shot, as indicated by one of the arrows on the left:

`online => shutdown`

When in `shutdown`, the node can later return and rejoin the cluster. That takes two steps, not one: restart the node first, then take it out of maintenance. In the diagram, `RESTART` must be understood as a deployment-specific action such as `kubectl run`, restarting a systemd unit, or powering the machine back on.

Both `maintenance` and `shutdown` involve a certain intra-cluster operation called **global rebalance**.

The third and final special state is **decommission**. Loosely synonymous with cleanup - very thorough cleanup - decommission entails:

* migrating all user data currently stored on the node to other online nodes;
* partial or complete cleanup of the node itself; and
* removing AIS metadata, configuration files, and, optionally, user data in its entirety.

Needless to say, there's no simple way back out of `decommission` - the proverbial point of no return. To rejoin the cluster after a completed decommission, the node must be rejoined or redeployed, depending on how far the cleanup progressed and whether local AIS metadata and data were removed.

## Table of Contents

- [Joining a Cluster: Discovery URL](#joining-a-cluster-discovery-url)
- [Cluster](#cluster)
- [Privileges](#privileges)
- [Rebalance](#rebalance)
  - [Proper Location](#proper-location)
  - [Quick Example](#quick-example)
- [Putting a Node in Maintenance](#putting-a-node-in-maintenance)
  - [Batch Operations](#batch-operations)
  - [Unconfirmed Maintenance State](#unconfirmed-maintenance-state)
  - [Skipping Rebalance](#skipping-rebalance)
- [One Membership Change at a Time](#one-membership-change-at-a-time)
- [Clearing Maintenance State](#clearing-maintenance-state)
- [Removing a Node from a Cluster](#removing-a-node-from-a-cluster)
- [Checking Removal Status](#checking-removal-status)
- [Summary](#summary)
- [References](#references)

## Joining a Cluster: Discovery URL

AIStore clusters can be deployed with an arbitrary number of AIStore proxies (a.k.a. gateways). Each proxy implements RESTful APIs, both native and S3-compatible, and provides full access to user data stored in the cluster.

Each proxy collaborates with the others to perform majority-voted HA failovers; see [Highly Available Control Plane](/docs/ha.md). All _electable_ proxies are functionally equivalent. The one elected as the current _primary_ is, among other things, responsible for joining nodes to the running cluster.

To facilitate node joins in the presence of disruptive events such as:

* network failures; and/or
* partial or complete loss of local AIS metadata such as cluster maps,

AIStore uses the so-called *original* and *discovery* URLs in the cluster configuration. The latter is versioned, replicated, protected, and distributed solely by the elected primary.

> **March 2024 update:** starting with v3.23, the *original* URL does _not_ track the original primary. Instead, the current primary takes full responsibility for updating both URLs with a single purpose: optimizing time to join or rejoin the cluster.

When an HA event triggers automated failover, the role of primary is assumed by a different proxy, with the corresponding cluster map (Smap) update synchronized across all running nodes.

A new node, however, may still have configuration that refers to the old primary. The *original* and *discovery* URLs exist precisely to address that scenario:

```console
$ ais config cluster proxy --json
{
    "proxy": {
        "primary_url": "https://ais-proxy-15.ais-proxy.ais.svc.cluster.local:51082",
        "original_url": "https://ais-proxy-15.ais-proxy.ais.svc.cluster.local:51082",
        "discovery_url": "https://ais-proxy.ais.svc.cluster.local:51082",
        "non_electable": false
    }
}
```

## Cluster

There is one cluster-level lifecycle command that deserves to be called out separately:

```console
$ ais cluster decommission --rm-user-data --yes
```

The above command destroys an existing cluster - completely and utterly, no questions asked. It is useful in testing, benchmarking, and other non-production environments. See `--help` for details.

## Privileges

All lifecycle management commands and their associated APIs require administrative privileges.

Broadly, there are three ways to satisfy that requirement:

* deploy the cluster with authentication disabled:

```console
$ ais config cluster auth --json
{
    "auth": {
        "signature": {
            "key": "**********",
            "method": "hmac"
        },
        "required_claims": {
            "aud": null
        },
        "oidc": {
            "issuer_ca_bundle": "-",
            "allowed_iss": null
        },
        "intra_cluster": {
            "enabled": false,
            "ttl": "0s",
            "nonce_window": "1m",
            "rotation_grace": "1m"
        },
        "enabled": false ### <<<<< authentication disabled
    }
}
```

* use the integrated `AuthN` server, which provides OAuth 2.0-compliant JWTs and a set of [CLI auth commands](/docs/cli/auth.md) to manage users, roles, and permissions; or
* outsource authorization to a separate centralized system, often LDAP-integrated, that manages existing users, groups, and mappings.

## Rebalance

Conceptually, AIStore rebalance is somewhat similar to what is often called a RAID rebuild. The underlying mechanics are different, but the high-level idea is similar: user data migrates from some nodes in a cluster to other nodes to restore the correct placement.

In AIStore, rebalancing is the system response to a lifecycle event that has already happened or is about to happen. Its singular purpose is to satisfy one governing rule:

**user data must be *properly* located**

### Proper Location

For any object in a cluster, its proper location is defined by the current cluster map and, locally on each target, by the configured target [mountpaths](/docs/terminology.md#mountpath).

In that sense, the `maintenance` state, for instance, has its beginning when the cluster starts rebalancing, and its post-rebalancing end when the corresponding sub-state is recorded in the next Smap version and safely distributed across all nodes.

### Quick Example

Given a 3-node single-gateway cluster, suppose we shut down one of the nodes:

```console
$ ais cluster add-remove-nodes shutdown <TAB-TAB>
p[BkTqWmFd]   t[QrmZvKdN]   t[XvnGkRdM]   t[ZdpKcVnT]

$ ais cluster add-remove-nodes shutdown t[QrmZvKdN] -y

Started rebalance "g47" (to monitor, run 'ais show rebalance').
t[QrmZvKdN] is shutting down, please wait for cluster rebalancing to finish

Note: the node t[QrmZvKdN] is _not_ decommissioned - it remains in the cluster map and can be manually
restarted at any later time (and subsequently activated via 'stop-maintenance' operation).
```

Once the command is executed, notice the following:

```console
$ ais show cluster
...
t[QrmZvKdN][x]   -   -   -   -   maintenance
```

At first, `maintenance` will show up in red, indicating a simple fact: data is expeditiously migrating from the node that is about to leave the cluster.

> A visual cue that effectively says: please don't disconnect it yet, and do not power it off.

Eventually, if you run:

```console
$ ais show cluster --refresh 3
```

or simply check a few times manually, the output will report that rebalance (`g47` in this example) has finished and the node `t[QrmZvKdN]` has gracefully left service. Simultaneously, `maintenance` in the `show` output becomes non-red:

| when rebalancing             | after                         |
| ---------------------------- | ----------------------------- |
| $${\color{red}maintenance}$$ | $${\color{cyan}maintenance}$$ |

The takeaway is simple: [global rebalance](/docs/rebalance.md) runs its full course *before* the node is permitted to leave cleanly. If interrupted for any reason - power cycle, network disconnect, another node joining, cluster shutdown, and so on - rebalance resumes and continues until the [governing condition](#proper-location) is globally satisfied.

## Putting a Node in Maintenance

To temporarily take a node out of the cluster, put it in maintenance mode. Nodes in maintenance remain in the cluster map but stop participating in normal request processing.

```console
$ ais cluster add-remove-nodes start-maintenance t[QrmZvKdN]
Started rebalance "g1" (to monitor, run 'ais show rebalance').
t[QrmZvKdN] is now in maintenance mode
```

Alternatively, you can shut the node down as part of the same workflow:

```console
$ ais cluster add-remove-nodes shutdown t[QrmZvKdN]
Started rebalance "g1" (to monitor, run 'ais show rebalance').
t[QrmZvKdN] is shutting down, please wait for cluster rebalancing to finish

Note: the node t[QrmZvKdN] is _not_ decommissioned - it remains in the cluster map and can be manually
restarted at any later time (and subsequently activated via 'stop-maintenance' operation).
```

If the node is a target, the cluster will rebalance after a short preparation phase. When the rebalance finishes, it is safe to power the node off.

### Batch Operations

`start-maintenance`, `stop-maintenance`, `shutdown`, and `decommission` all accept multiple nodes:
`NODE_ID [NODE_ID...]`, comma- or space-separated. TAB completion suggests the nodes not yet selected.

The batch executes as one coordinated operation. Each lifecycle phase updates the cluster map once for
the entire batch. When rebalance is required, the cluster performs one RMD increment and starts
one global rebalance, regardless of how many nodes you specify:

```console
$ ais cluster add-remove-nodes start-maintenance <TAB-TAB>
p[BkTqWmFd]   t[XvnGkRdM]   t[QrmZvKdN]   t[ZdpKcVnT]   t[HbjTwLpS]   t[NwLjRbGq]

$ ais cluster add-remove-nodes start-maintenance t[QrmZvKdN] t[HbjTwLpS] --yes
Started rebalance "g1" (to monitor, run 'ais show rebalance').
t[QrmZvKdN] is now in maintenance mode
t[HbjTwLpS] is now in maintenance mode
```

The three remaining targets receive the migrating data - a single `g1` for the pair, not one rebalance per node:

```console
$ ais show rebalance
REB ID   NODE       TX OBJECTS   TX BYTES   RX OBJECTS   RX BYTES   START      END   STATE
g1       ZdpKcVnT   0            0B         42695        41.69MiB   12:11:28   -     Running
g1       NwLjRbGq   0            0B         42555        41.56MiB   12:11:28   -     Running
g1       XvnGkRdM   0            0B         42961        41.95MiB   12:11:28   -     Running

$ ais wait rebalance g1
Waiting for rebalance[g1] ...
Done.
```

Admission is all-or-nothing: if any specified node is unknown or is the current primary, the entire request
is rejected and no node is touched. Conversely, once the transaction is underway, a node that
disappears - keepalive-removed, for instance - does not abort it; the remaining nodes complete
normally.

Operation-specific state checks also apply. `start-maintenance` skips a node that has already completed
the same transition, and `stop-maintenance` skips a node that is already active. If every specified node is
skipped, the command reports "nothing to do" and leaves the cluster map untouched. A node in
maintenance can be advanced to `shutdown` or `decommission`; `stop-maintenance` refuses a node that
is being decommissioned.

### Unconfirmed Maintenance State

A target is marked (and stays in) `maintenance` before its post-rebalance transition is confirmed. This is
the normal final state of `start-maintenance --no-rebalance`, but it can also mean that the associated
global rebalance transaction was interrupted, renewed by a concurrent self-join, or aborted because
another target left the cluster (e.g., via K8s delete-pod => SIGTERM => `rmSelf`).

These cases are indistinguishable from the primary's perspective.

Given unconfirmed maintenance state, the operator can proceed in one of several ways:

* repeat `start-maintenance`. This is accepted rather than rejected, so a retry - or a rolling-upgrade
  script that reissues one - does not fail. It keeps the target out of service and reapplies maintenance
  on the node when reachable. If no active target is specified alongside, that is all it does;
* run `stop-maintenance` to clear maintenance and return the target to service, with rebalance as
  required;
* advance the target to `shutdown` or `decommission`; or
* leave it in maintenance. An explicit `ais start rebalance` can restore global data placement, but
  does not itself change the target's unconfirmed maintenance flag.

> An unconfirmed target specified together with an active one follows the normal batch path. With
> automatic rebalance enabled and without `--no-rebalance`, that batch rebalances, and its
> post-rebalance step confirms both.
>
> Specifying it together with a target whose maintenance is already confirmed changes nothing: the
> confirmed target is left as it is (e.g., for `{unconfirmed A, confirmed B}` - B is skipped), and the
> command behaves as if only the unconfirmed one had been specified.

### Skipping Rebalance

> **Advanced usage only:** `--no-rebalance` is not recommended for routine cluster operations.
> In normal operation, let AIS run rebalance automatically.
>
> The primary recommended use case is a controlled rolling-maintenance or rolling-upgrade workflow, where nodes are taken out of service and returned in a coordinated sequence. In Kubernetes deployments, this sequencing is typically handled automatically by the [AIS Kubernetes operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator).

If you use `--no-rebalance`, the node enters maintenance immediately without waiting for data migration:

```console
$ ais cluster add-remove-nodes start-maintenance t[QrmZvKdN] --no-rebalance --yes
t[QrmZvKdN] is now in maintenance mode
```

Keeping automatic rebalance enabled is strongly recommended, but there are cases where skipping it is safe:

* all buckets are empty;
* maintenance was started with `--no-rebalance` and no objects were added or updated during maintenance;
* all objects can be refetched from remote backends such as remote AIS, HTTP, or cloud buckets, understanding that this may incur extra cloud traffic charges; or
* multiple nodes are being returned from maintenance, in which case name them all in a single `stop-maintenance` command - see [Batch Operations](#batch-operations) - rather than sequencing them with `--no-rebalance`.

The `--no-rebalance` flag is available for `start-maintenance`, `shutdown`, `stop-maintenance`, and `decommission`.

## One Membership Change at a Time

The primary admits one administrative membership change at a time. A second request issued while the
first is still executing is refused. If the first request starts a global rebalance, the exclusion
continues until that rebalance reaches a terminal state:

```console
$ ais cluster add-remove-nodes start-maintenance t[HbjTwLpS] --yes
Started rebalance "g1" (to monitor, run 'ais show rebalance').
t[HbjTwLpS] is now in maintenance mode

$ ais cluster add-remove-nodes start-maintenance t[QrmZvKdN] --yes
Error: ErrBusy: cluster membership "start-maintenance" is currently busy (rebalance[g1] is running), please try again
```

The rule covers `start-maintenance`, `stop-maintenance`, `shutdown`, `decommission`, the advanced
unsafe removal command, explicit `join`, and an operator-initiated `ais start rebalance` with or
without `--cleanup`.

There are two deliberate qualifications:

* **Self-join is not serialized.** A node starting or restarting and registering on its own - including
  normal Kubernetes restart and scale-up paths - is not subject to the administrative admission guard.
  It may join while a rebalance is running and cause that rebalance to be renewed.
* **An exact inverse is not exempt.** Taking the same nodes back out of maintenance while their causal
  rebalance is running is refused like any other membership change. Wait for the rebalance to finish
  (`ais show rebalance`), then reactivate the nodes.

Do not abort a lifecycle-triggered rebalance merely to issue its inverse. Lifecycle operations are not
rollback transactions: aborting rebalance does not restore the preceding Smap, can leave maintenance
or shutdown post-rebalance state unconfirmed, and does not necessarily prevent decommission
finalization.

To transition several nodes together, specify them in one command - see
[Batch Operations](#batch-operations) - rather than issuing requests one after another.

## Clearing Maintenance State

Once a node is in maintenance mode, the cluster keeps it there until you explicitly clear that state.

If the node was shut down, restart or power it on first and wait for it to register with the primary proxy. Then run:

```console
$ ais cluster add-remove-nodes stop-maintenance t[QrmZvKdN]
Started rebalance "g3" (to monitor, run 'ais show rebalance').
t[QrmZvKdN] is now active
```

To skip automatic rebalance, provide `--no-rebalance` (advanced usage only; see [Skipping Rebalance](#skipping-rebalance).

> In general, automatic rebalance should remain enabled. The same considerations listed under [Skipping Rebalance](#skipping-rebalance) apply here as well.

The node starts accepting requests again after it rejoins and the cluster clears its maintenance state. You do not have to wait for the rebalance that `stop-maintenance` itself starts.

Note, however, that `stop-maintenance` is rejected while a global rebalance is already running - including the rebalance triggered by the `start-maintenance`, `shutdown`, or `decommission` that put the node there. Wait for it to finish (`ais show rebalance`) and then reactivate.

## Removing a Node from a Cluster

To permanently remove a node from the cluster, decommission it:

```console
$ ais cluster add-remove-nodes decommission t[QrmZvKdN]
Started rebalance "g5" (to monitor, run 'ais show rebalance').
t[QrmZvKdN] is being decommissioned, please wait for cluster rebalancing to finish...
```

When the rebalance finishes, the primary proxy removes the node automatically from the cluster map. On unregistering, the node erases its AIS metadata.

Skipping rebalance performs only the minimal preparation and removes the node immediately:

```console
$ ais cluster add-remove-nodes decommission --no-rebalance t[QrmZvKdN]
t[QrmZvKdN] has been decommissioned (permanently removed from the cluster)
```

Note that `decommission` cleans up AIS metadata and stops the node. By contrast, `shutdown` only stops AIS services.

If the node is a target, shutdown takes full effect after the rebalance completes. If the node is a proxy, shutdown is immediate.

## Checking Removal Status

Putting a node in maintenance does **not** automatically power it off.

AIS runs a rebalance when a node enters maintenance mode. You should verify cluster state via `ais show cluster target` before deciding that it is safe to power the node off.

In the example below, the `REBALANCE` column shows `finished` and the node is labeled `maintenance` - it is safe to power it off:

```console
$ ais show cluster target
TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE    UPTIME  STATUS
QrmZvKdN         0.13%           31.28GiB        16%             2.435TiB        0.00%           finished     31m     maintenance
XvnGkRdM         0.13%           31.28GiB        16%             2.435TiB        0.12%           finished     31m     online
```

For decommissioning nodes, the status looks like this while rebalance is still running:

```console
$ ais show cluster target
TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE    UPTIME  STATUS
QrmZvKdN         0.13%           31.28GiB        16%             2.435TiB        0.00%           running      31m     decommission
XvnGkRdM         0.13%           31.28GiB        16%             2.435TiB        0.12%           running      31m     online
```

When rebalance finishes, the primary proxy removes the decommissioned node automatically:

```console
$ ais show cluster target
TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE    UPTIME
XvnGkRdM         0.13%           31.28GiB        16%             2.435TiB        0.12%           finished     31m
```

## Summary

| lifecycle operation          | CLI                             | brief description                                                                                                                                                          |
| ---------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| maintenance mode             | `start-maintenance`             | The lightest way to remove a node from service. Stop keepalive heartbeats, do not insist on metadata updates, and ignore transient failures while the cluster transitions. |
| shutdown                     | `shutdown`                      | Same as above, plus node shutdown (`aisnode` exit).                                                                                                                        |
| decommission                 | `decommission`                  | Same as above, plus partial or complete cleanup. A decommissioned node is eventually removed from the cluster map.                                                         |
| remove node from cluster map | `ais advanced remove-from-smap` | Strictly intended for testing and special use-at-your-own-risk scenarios. Immediately remove the node from the cluster and distribute an updated Smap with no rebalancing. |
| take node out of maintenance | `stop-maintenance`              | Re-enable keepalive, update the node with current cluster metadata, run global rebalance, and return the node to `online`.                                                 |
| join new node                | `join`                          | Update the node, synchronize current cluster metadata, and run global rebalance as needed.                                                                                 |

All of the above are administrative membership changes. The cluster admits one at a time and refuses
another while global rebalance is running; see
[One Membership Change at a Time](#one-membership-change-at-a-time). A node's own self-join is not
subject to this rule.

### Assorted Notes

Normally, a starting AIS node (`aisnode`) uses its local [configuration](/docs/configuration.md) to contact the cluster and perform a self-join. That does not require an explicit `join` command or any separate administrative action.

Still, the `join` command is useful when the node is misconfigured. Separately, it can also be used to join a standby node - that is, a node started in standby mode; see [`aisnode` command line](/docs/command_line.md).

The explicit `join` command is an administrative membership change and is serialized with the other
operations. A node's own self-join is not; see
[One Membership Change at a Time](#one-membership-change-at-a-time).

During rebalance, the cluster remains fully operational: users can read and write data, list, create, and destroy buckets, run jobs, and so on. In other words, none of the lifecycle operations described here requires downtime.

## References

* [CLI: cluster management commands](/docs/cli/cluster.md)
* [Global Rebalance](/docs/rebalance.md)
* [AuthN](/docs/authn.md)
* [AIS on Kubernetes deployment: playbooks](https://github.com/NVIDIA/ais-k8s/tree/main/playbooks)
