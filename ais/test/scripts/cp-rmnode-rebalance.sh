#!/bin/bash

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi

## start from scratch and generate 999 tgz shards
ais bucket rm ais://src ais://dst -y 2>/dev/null
ais create ais://src
ais archive gen-shards 'ais://src/shard-{001..999}.tgz'

## list-objects to confirm
num=$(ais ls ais://src --no-headers | wc -l)
[[ $num == 999 ]] || { echo "FAIL: $num != 999"; exit 1; }

cleanup() {
  ais cluster add-remove-nodes stop-maintenance $node
}

trap cleanup EXIT INT TERM

## run forever or Ctrl-C
while true
do
  ## 1. start copying with an empty template (any prefix, any range)
  ais cp ais://src ais://dst --template ""

  ## randomize event timing: traffic vs cluster losing a node
  sleep $((RANDOM % 5))

  ## 2. gracefully remove a random node
  node=$(ais advanced random-node)
  ais cluster add-remove-nodes start-maintenance $node -y ## triggers global rebalance
  sleep $((2 + RANDOM % 2))

  ## 3. wait for the copy
  ais wait copy-objects

  ## 4. check the numbers
  res=$(ais ls ais://dst --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: destination $num != $res"; exit 1; }
  ## the same using '--summary' option:
  res=$(ais ls ais://src --summary --no-headers | awk '{print $3}')
  [[ $num == $res ]] || { echo "FAIL: source $num != $res"; exit 1; }

  ## 5. activate the (previously lost) node and join it back
  ## (rebalance from step 2 may still be running at this point - it'll be aborted and restarted)
  ais cluster add-remove-nodes stop-maintenance $node

  ## 6. cleanup and repeat
  ais wait rebalance

  ## and double-check again
  res=$(ais ls ais://dst --summary --no-headers | awk '{print $3}')
  [[ $num == $res ]] || { echo "FAIL: destination $num != $res"; exit 1; }
  res=$(ais ls ais://src --summary --no-headers | awk '{print $3}')
  [[ $num == $res ]] || { echo "FAIL: source $num != $res"; exit 1; }

  ais bucket rm ais://dst -y
done
