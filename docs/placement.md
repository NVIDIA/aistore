# Placement weights

Status (v5.2): the control plane records target weights and bucket overrides; object
placement remains uniform until weighted HRW is integrated. Weight changes that alter
relative shares among active targets trigger global rebalance;
proportional changes and equal-vs-none transitions do not.

## Target weights

A weight is a relative share of objects a target is expected to own. Weights have no
units and no fixed total: `1, 3, 6` and `100GB, 300GB, 600GB` (in bytes) define the
same placement.

- 64-bit integers, quoted in JSON (`placement.weight`); negative values are reserved
- all targets are weighted, or none; setting all weights to zero clears them
- requires two or more targets: a single-target cluster rejects weights (HTTP 400)
- max/min ratio: 10x or greater warns, 100x or greater is rejected
- a target that joins a weighted cluster gets the mean weight of active targets
- re-registration and cluster restart keep assigned weights

To exclude a target, use maintenance - not zero weight.

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

- a super-majority (2/3 + 1) of targets is weighted: the remaining targets get the mean weight;
- otherwise, or if the resulting weights are invalid: the cluster starts unweighted
  (logged as an error; the next rebalance moves data accordingly).

If the assembled cluster map is weighted, pre-5.2 members are removed (with an upgrade
message) before the super-majority count. In a weighted startup, a starting node declares
readiness only after the primary reconciles weights and the node holds the reconciled
(or newer) cluster map. Unweighted startups are unaffected.

Local preferences persist across restarts: to clear weights for good, remove `placement.weight`
from targets' local configs as well.

## Upgrade

Older nodes cannot decode a cluster map with weights, or bucket metadata with placement overrides.

- setting weights returns HTTP 409 while node versions differ
- a pre-5.2 node cannot join a weighted cluster, or a cluster with any bucket placement override
- the primary learns node versions on join; a newly elected primary may not have them
- setting a bucket placement override is not yet rejected while node versions differ

## Buckets

`placement.weighting="uniform"` makes a bucket ignore target weights; an empty value
clears the override. Clearing target weights returns HTTP 409 while any bucket has an
override. Redistribution on override change is pending.
