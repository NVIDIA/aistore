#!/bin/bash

## start from scratch and generate 999 tgz shards
ais bucket rm ais://src-mirror ais://dst -y 2>/dev/null

## NOTE: 3-way mirror implies that each target has at least 3 disks
ais create ais://src-mirror --props="mirror.enabled=true mirror.copies=3" || \
exit 1

ais advanced gen-shards 'ais://src-mirror/shard-{001..999}.tgz'

## list-objects to confirm
num=$(ais ls ais://src-mirror --no-headers | wc -l)
[[ $num == 999 ]] || { echo "FAIL: $num != 999"; exit 1; }

cleanup() {
  ais storage mountpath attach $node $mountpath
}

trap cleanup EXIT INT TERM

## run forever or Ctrl-C
while true
do
  ## 1. start copying with an empty template (any prefix, any range)
  ais cp ais://src-mirror ais://dst --template ""

  ## randomize event timing: traffic vs node losing a disk
  sleep $((RANDOM % 5))

  ## 2. remove a random node's (random) mountpath (aka disk)
  node=$(ais advanced random-node)
  mountpath=$(ais advanced random-mountpath $node)
  ais storage mountpath detach $node $mountpath

  ## 3. wait for the copying job
  ais wait copy-objects

  ## 4. check the numbers
  res=$(ais ls ais://dst --no-headers | wc -l)
  [[ $num == $res ]] || { echo "FAIL: destination $num != $res"; exit 1; }
  ## the same using '--summary' option:
  res=$(ais ls ais://src-mirror --summary --no-headers | awk '{print $3}')
  [[ $num == $res ]] || { echo "FAIL: source $num != $res"; exit 1; }

  ## 5. attach the (previously lost) disk
  ais storage mountpath attach $node $mountpath

  ## 6. wait for the node to resilver itself
  ais wait resilver

  ## and double-check again
  res=$(ais ls ais://dst --summary --no-headers | awk '{print $3}')
  [[ $num == $res ]] || { echo "FAIL: destination $num != $res"; exit 1; }
  res=$(ais ls ais://src-mirror --summary --no-headers | awk '{print $3}')
  [[ $num == $res ]] || { echo "FAIL: source $num != $res"; exit 1; }

  ais bucket rm ais://dst -y
done
