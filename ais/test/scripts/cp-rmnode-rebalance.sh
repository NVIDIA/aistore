#!/bin/bash
ais bucket rm ais://src ais://dst -y 2>/dev/null
ais create ais://src
ais advanced gen-shards 'ais://src/shard-{001..999}.tgz'
num=$(ais ls ais://src --no-headers | wc -l)
[[ $num == 999 ]] || { echo "FAIL: $num != 999"; exit 1; }

cleanup() {
  ais cluster add-remove-nodes stop-maintenance $node
}

trap cleanup EXIT INT TERM

while true
do
  ## 1. start copying with an empty template (any prefix, any range)
  ais cp ais://src ais://dst --template ""
  sleep $((2 + RANDOM % 2))

  ## 2. gracefully remove a random node
  node=$(ais advanced random-node)
  ais cluster add-remove-nodes start-maintenance $node -y ## triggers global rebalance
  sleep $((2 + RANDOM % 2))

  ## 3. wait for the copy
  ais wait copy-objects

  ## 4. check the numbers
  res=$(ais ls ais://dst --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: destination $num != $res"; exit 1; }
  res=$(ais ls ais://src --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: source $num != $res"; exit 1; }

  ## 5. activate the (previously lost) node and join it back
  ## (rebalance may be still running at this point - will be aborted and restarted)
  ais cluster add-remove-nodes stop-maintenance $node

  ## 6. cleanup and repeat
  ais wait rebalance

  ## and check again
  res=$(ais ls ais://dst --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: destination $num != $res"; exit 1; }
  res=$(ais ls ais://src --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: source $num != $res"; exit 1; }

  ais bucket rm ais://dst -y
done
