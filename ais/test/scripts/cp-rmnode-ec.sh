#!/bin/bash
ais bucket rm ais://src-ec ais://dst -y 2>/dev/null

## create erasure-coded bucket
## NOTE: must have enough (i.e., 5 in this case) nodes in the cluster
ais create ais://src-ec --props "ec.enabled=true ec.data_slices=3 ec.parity_slices=1 ec.objsize_limit=0" || \
exit 1

ais advanced gen-shards 'ais://src-ec/shard-{001..999}.tar'
num=$(ais ls ais://src-ec --no-headers | wc -l)
[[ $num == 999 ]] || { echo "FAIL: $num != 999"; exit 1; }

cleanup() {
  ais cluster add-remove-nodes stop-maintenance $node
}

trap cleanup EXIT INT TERM

while true
do
  ## 1. start copying all
  ais cp ais://src-ec ais://dst --template ""

  ## randomize event timing
  sleep $((RANDOM % 5))

  ## 2. remove a random node immediately (no rebalance!)
  node=$(ais advanced random-node)
  ais cluster add-remove-nodes start-maintenance $node --no-rebalance -y

  ## 3. wait for the copying job to finish
  ais wait copy-objects

  ## 4. activate and join back
  ais cluster add-remove-nodes stop-maintenance $node
  ais wait rebalance

  ## 5. check the the numbers
  res=$(ais ls ais://src-ec --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: source $num != $res"; exit 1; }
  res=$(ais ls ais://dst --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: post-rebalance destination $num != $res"; exit 1; }

  ## 6. cleanup and repeat
  ais bucket rm ais://dst -y
done
