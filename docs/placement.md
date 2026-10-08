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

## Upgrade

Older nodes cannot decode a cluster map with weights.

- setting weights returns HTTP 409 while node versions differ
- a pre-5.2 node cannot join a weighted cluster
- the primary learns node versions on join; a newly elected primary may not have them

## Buckets

`placement.weighting="uniform"` makes a bucket ignore target weights; an empty value
clears the override. Redistribution on override change is pending.
