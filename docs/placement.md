# Placement weights

Status (v5.2): object placement honors target weights and bucket overrides - in request
routing, rebalance, storage cleanup, EC, and all jobs that write or locate objects.
Weight changes that alter relative shares among active targets trigger global rebalance;
proportional changes and equal-vs-none transitions do not.

To inspect actual in-cluster object counts and sizes on each target, use
`ais storage summary BUCKET --per-target` (or `ais ls BUCKET --summary --per-target`).
These measured shares are separate from the configured shares shown by
`ais cluster placement`; see [per-target content distribution](/docs/cli/bucket.md#per-target-content-distribution).

**Table of Contents**

- [Target weights](#target-weights)
- [Setting weights](#setting-weights)
- [CLI](#cli)
- [Target admission and restart](#target-admission-and-restart)
- [Managing node weights across the lifecycle](#managing-node-weights-across-the-lifecycle)
  - [Set weights](#set-weights)
  - [Restart the cluster](#restart-the-cluster)
  - [Add a target](#add-a-target)
  - [Change an assigned weight](#change-an-assigned-weight)
  - [Clear weights](#clear-weights)
  - [Cold restart: how the primary decides](#cold-restart-how-the-primary-decides)
  - [Startup readiness](#startup-readiness)
  - [Troubleshooting](#troubleshooting)
- [Upgrade](#upgrade)
- [Python SDK direct reads](#python-sdk-direct-reads)
- [Buckets](#buckets)
- [Erasure coding](#erasure-coding)
- [Verifying placement](#verifying-placement)
- [Weighted HRW implementation](#weighted-hrw-implementation)

## Target weights

A weight is a relative share of objects an AIS target is expected to own. Weights have no
units and no fixed total: `1, 3, 6` and `100GB, 300GB, 600GB` (in bytes) define the
same placement.

Practical applications may include accounting for unequal usable capacity across
heterogeneous cluster hardware—such as mixed disk sizes, uneven hardware upgrades, or
hyperconverged deployments where storage targets share resources with other local services.

Other practical examples may include:

* Generational hardware mixing: When expanding an older cluster with newer, much denser nodes,
weights will allow the cluster to use more of the newer nodes’ capacity
without hitting capacity limits on the legacy nodes.
* Storage nodes with different disk sizes and/or media (SSDs vs HDDs, etc.)

However, totally abstract weights detached from any physical resource metrics are fully supported as well.

- 64-bit integers, quoted in JSON (`placement.weight`); negative values are reserved
- all targets are weighted, or none; setting all weights to zero clears them
- requires (minimum) two targets: a single-target cluster rejects weights (HTTP 400)
- max/min ratio: 10x or greater warns, 100x or greater is rejected
- a target that joins a weighted cluster *by default* gets the mean weight of active targets
- re-registration and cluster restart keep assigned weights

To exclude a target from the cluster, use [maintenance mode](/docs/lifecycle_node.md) - not zero weight.

> **Note on workloads:** Object placement maps directly to access patterns, meaning both read
and write workloads will be proportional to these weights. Assigning a disproportionately
large weight can turn that target into a performance bottleneck.

## Setting weights

Get the cluster map, assign every target's weight, and submit:

```go
smap, _ := api.GetClusterMap(bp)
for _, tsi := range smap.Tmap {
	tsi.SetPlacementWeight(w)
}
xid, err := api.SetPlacementWeights(bp, smap)
```

- same weights: no-op
- proportional weights (and equal weights vs. none): new cluster map, no rebalance
- otherwise: new cluster map and global rebalance (requires rebalance enabled);
  returns the rebalance ID
- a stale cluster map returns HTTP 409
- fewer than two active targets: accepted without rebalance; recorded weights are
  retained when targets in maintenance rejoin

## CLI

```console
ais cluster placement                                             # show weights and shares ('show' is optional)
ais cluster placement set-weights 't[abc]=1' 't[def]=3' 't[ghi]=6'
ais cluster placement set-weights 't[abc]=300' rest=100           # one target, and all others
ais cluster placement set-weights 't[abc]=none'                   # reset one weight
ais cluster placement set-weights none                            # clear all weights
ais cluster placement clear-weights                               # same
```

- `rest=WEIGHT` applies to all targets not listed in the same command; in a cluster without weights, every target must be listed or covered by `rest`
- `TARGET_ID=none` assigns the weight a joining target would get (see above)
- `none` alone, `rest=none` alone, or `none` for every target clears all weights
- equal weights place objects the same way as no weights; `show` reports them as uniform
- quote node IDs: `[...]` is a glob pattern in most shells

## Target admission and restart

A target may specify a preferred weight in its local `ais_local.json`:

```json
{
    "placement": {"weight": "300"}
}
```

This is an admission preference, separate from the primary-assigned weight in Smap.
When joining a weighted cluster, a new target uses its positive local preference, or
the mean of existing active weights if the preference is absent or zero. Negative
preferences are invalid. The primary checks the resulting cluster's weight ratio
before admission. Once the cluster is running, a local preference does not enable weighting
in an unweighted cluster (see primary startup below).

A target that remains in Smap keeps its assigned weight after shutdown, crash, or
restart, even if its local preference differs. Change an assigned weight with
`ais cluster placement set-weights`. A target removed from Smap is a new admission.

For a manual join, `ais cluster add-remove-nodes join IP:PORT --role=target --weight=300` supplies an
explicit preference; otherwise the target gets the cluster mean. An existing
Smap assignment takes precedence in either case.

Primary startup assigns each joining target its saved Smap weight or, if there is none,
its local preference. Once the cluster map is assembled, the primary decides:

- a strict super-majority of targets is weighted: the remaining targets get the mean weight
  (see [cold restart](#cold-restart-how-the-primary-decides));
- otherwise, or if the resulting weights are invalid: the cluster starts unweighted
  (logged as an error; the next rebalance moves data accordingly).

If the assembled cluster map is weighted, pre-5.2 members are removed (with an upgrade
message) before the super-majority count. When saved or requested weights are involved,
a starting node declares readiness only after the primary reconciles weights and the node
holds the reconciled (or newer) cluster map. This also applies when reconciliation clears
weights. Startups without weights are unaffected.

Local preferences persist across restarts: to clear weights for good, remove `placement.weight`
from targets' local configs as well.

## Managing node weights across the lifecycle

The examples below use a cluster of up to five targets; node IDs are illustrative.

### Set weights

```console
$ ais cluster placement
Placement: uniform (target weights are not set)

$ ais cluster placement set-weights 't[ROytQbMs]=100' 't[UpftZhCx]=200' 't[flztWnGd]=300'
Started rebalance "g1" (to monitor, run 'ais show rebalance').

$ ais cluster placement
TARGET       WEIGHT  SHARE
t[ROytQbMs]  100     16.7%
t[UpftZhCx]  200     33.3%
t[flztWnGd]  300     50.0%
```

### Restart the cluster

Restart all nodes: the cluster comes back with the same weights.

While the primary is still starting up, `ais cluster placement` may show `uniform` -
the cluster map is not yet assembled. Check again once the cluster has started.

### Add a target

The following rules apply to a running weighted cluster. In a running unweighted cluster,
join preferences do not enable weights; use `set-weights` instead.

| you do | you get |
|---|---|
| start a new target (no local `placement.weight`) | the rounded mean weight of active weighted targets |
| start a new target with local `placement.weight` | the requested weight (if the resulting ratio is valid) |
| `ais cluster add-remove-nodes join HOST:PORT --role=target --weight=W` | `W`; without `--weight`, the mean (admin join does not use the target's local config) |
| restart a target that is still in the cluster map | its assigned weight (local preference ignored) |

The mean is rounded to the nearest multiple of the weights' greatest common divisor (a.k.a. GCD).
If no weighted target is active, all weighted targets are used.

For example, a new target joins the weighted cluster above:

```console
$ ais cluster placement
TARGET       WEIGHT  SHARE
t[ROytQbMs]  100     12.5%
t[UpftZhCx]  200     25.0%
t[flztWnGd]  300     37.5%
t[tVbtJrPk]  200     25.0%
```

And another one, with `"placement": {"weight": "500"}` in its local config:

```console
$ ais cluster placement
TARGET       WEIGHT  SHARE
t[LDLtHsVy]  500     38.5%
t[ROytQbMs]  100     7.7%
t[UpftZhCx]  200     15.4%
t[flztWnGd]  300     23.1%
t[tVbtJrPk]  200     15.4%
```

For these five weights, the default mean is 300: the arithmetic mean of 260 rounds to
the nearest multiple of their greatest common divisor, 100.

When joining a running weighted cluster, a requested weight that would make the max/min
ratio 100x or greater is rejected outright - the target does not retry. Fix (or remove)
its local `placement.weight` and restart it. During primary startup, invalid weights can
instead cause a fallback to unweighted placement (see cold restart below).

### Change an assigned weight

Specify the target's new weight; unlisted targets keep their assignments:

```console
$ ais cluster placement set-weights 't[UpftZhCx]=400'
```

Editing that target's local `placement.weight` and restarting it keeps its existing
assignment; use `set-weights` to change it.

### Clear weights

If clearing is blocked by a bucket placement override (HTTP 409), reset the override
first (see [Buckets](#buckets)).

```console
$ ais cluster placement clear-weights
Started rebalance "g403" (to monitor, run 'ais show rebalance').

$ ais cluster placement
Placement: uniform (target weights are not set)
```

To clear weights for good, also remove `placement.weight` from targets' local configs -
see cold restart below.

### Cold restart: how the primary decides

Once the cluster map is assembled, the primary counts weighted targets (assigned in the
cluster map, or requested by a joining target). If the assembled map is weighted,
detected pre-5.2 members are removed before counting.
For `N` targets, a strict super-majority is `floor(2*N/3)+1`: three of four, four of five, or `667` of `1,000`.

| weighted targets | result |
|---|---|
| none | the cluster starts unweighted |
| all, with valid weights | weights unchanged |
| strict super-majority, with valid resulting weights | the rest get the mean weight |
| below the threshold, or the resulting weights are invalid (max/min ratio) | all weights cleared; the cluster starts unweighted; error logged |

For example, three weighted targets with weights `100/200/300` and one unassigned target
meet the threshold of three out of four. The unassigned target gets 200, producing
`100/200/300/200`.

For instance, a cluster with weights cleared, where one of five targets still has a local
`placement.weight`, logs at startup:

```
p[uUVpKfNz] starting unweighted: 1 out of 5 targets are weighted (no super-majority)
```

and comes up unweighted. To get weights back, use `set-weights`; to stop the message, remove
the stray `placement.weight`.

### Startup readiness

| startup | starting nodes declare readiness |
|---|---|
| no saved or requested weights involved | as before (unchanged) |
| saved or requested weights involved, including fallback to unweighted | after the primary reconciles weights, holding the reconciled (or newer) cluster map |

Nodes already ready before weights appear keep their readiness.

### Troubleshooting

| you see | meaning | what to do |
|---|---|---|
| `(waiting for cluster to start ...)` | the CLI is retrying an HTTP 503 response, often during startup | let the retries finish; inspect the final error if the command still fails |
| `ErrBusy: ... "set-placement-weights" is currently busy (rebalance[gNNN] is running)` | a global rebalance is running; it may have resumed after a restart | wait for it (`ais show rebalance`), retry |
| `starting unweighted: N out of M targets are weighted (no super-majority)` | see cold restart above | `set-weights`, or remove stray local `placement.weight` |
| `cannot admit t[...] with placement weight W: ...` | the requested weight would make the max/min ratio 100x or greater | fix the target's local `placement.weight` (or `--weight`) |
| `cannot clear placement weights: bucket ...` | a bucket has a placement override | reset that override first (see [Buckets](#buckets)) |
| `ErrBusy: ... "set-placement-weights" is currently busy (node versions not yet known: ...)` | the primary has not yet heard from these members (e.g., right after primary change) | retry in a few seconds |
| `node versions differ` | some member runs a different version | complete the upgrade |

## Upgrade

Older nodes cannot decode a cluster map with weights, bucket metadata with placement
overrides, or the rebalance metadata used for bucket redistribution.

- setting weights returns HTTP 409 while node versions differ
- a pre-5.2 node cannot join a weighted cluster, or a cluster with any bucket placement override
- the primary learns node versions on join and from keepalives; right after a primary change,
  setting weights may return HTTP 409 until all members outside maintenance/decommission
  have reported the primary's version; those in maintenance/decommission are checked on rejoin
- introducing a bucket placement override or changing effective bucket placement
  also requires every active member to run the primary's version

## Python SDK direct reads

Python SDK 2.1 computes uniform HRW placement for `get_reader(direct=True)` and `Smap.get_target_for_object()`. It does not apply target weights or bucket placement overrides and can therefore select the wrong target for a weighted bucket.

For weighted buckets, keep the default `direct=False`. The request goes through a proxy, which selects the target using the bucket's effective placement policy. The subsequent data transfer still follows the normal proxy-to-target redirect path.

## Buckets

By default, every bucket follows target weights. To make a bucket ignore them:

```console
ais bucket props set ais://abc placement.weighting=uniform   # uniform placement, as if unweighted
ais bucket props set ais://abc placement.weighting=""        # reset (follow target weights)
```

- changing effective placement starts a bucket-scoped rebalance and returns its job ID;
  this applies to existing objects, including when resetting the override or bucket properties
- unchanged policies, or changes between default and uniform placement with equal or
  unset target weights, update properties without redistribution
- a single properties update cannot both redistribute objects and re-mirror, re-EC,
  or rechunk the bucket; submit these layout changes separately
- ownership-changing updates require rebalance enabled and obey the same membership
  serialization and EC spare-target checks as target-weight updates
- any change of `placement.weighting`, even in an unweighted cluster, returns `ErrBusy`
  (HTTP 409) while a rebalance or membership change is in progress; retry when it completes
- clearing target weights returns HTTP 409 while any bucket has an override
- a pre-5.2 node cannot join a cluster with any bucket override

## Erasure coding

Weights apply to EC as well: the object's full replica goes to its owner, and slices
(or replicas) go to the next targets in weighted order.

- a target holds at most one part of any given object; as `data_slices + parity_slices + 1`
  approaches the number of active targets, almost every target holds a part of every
  object, and weights have little effect on EC buckets
- moving a sliced object's owner (e.g., after a weight change) needs a spare active target
  beyond the full replica and slices: `data_slices + parity_slices + 2` active targets;
  target-weight and bucket-policy updates that require migration are rejected without
  that spare capacity; uniform buckets are unaffected by target-weight updates

## Verifying placement

Configured shares (`ais cluster placement`) are expected values; measured shares are:

```console
ais storage summary ais://abc --per-target     # per-target object counts and sizes
ais storage validate ais://abc                 # misplaced objects, if any
```

With `N` objects and a target's configured share `p`, its count deviates from `N*p`
by about `sqrt(N*p*(1-p))` - small buckets show visibly uneven shares. Both commands also
count misplaced copies that a recent rebalance has yet to clean up.

## Weighted HRW implementation

Traditional HRW generates a uniform random score for each target using a hash function,
and picks the maximum score. Weighted HRW transforms the uniform random variables
`U` in (0, 1] into exponentially distributed `-ln(U)/weight`, and picks the minimum:

- each active target owns a `weight/sum(weights)` share of objects
- equal weights select exactly the same target as no weights
- proportional weights select exactly the same targets
- changing one target's weight moves objects only to or from that target
- an object's ranked targets (EC) are the owner followed by the remaining targets in
  weighted order

The implementation (`core/meta/hrw.go`) uses only integer arithmetic - fixed-point
logarithm and 128-bit products - with identical results on every architecture, for any
64-bit weights. Placement of objects in a bucket with `placement.weighting=uniform`
remains traditional HRW. Targets in maintenance or being decommissioned own no objects.
Selecting a target for a job, or a proxy, remains uniform.
