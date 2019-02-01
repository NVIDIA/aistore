#!/bin/bash
set -xo

echo Creating disks total of $#

chmod 744 createaisvolumes.sh
./createaisvolumes.sh $#

echo Create FS dirs
for disk in "$@"; do
    parallel-ssh -h inventory/targets.txt -P 'sudo mkdir -p /ais/'$disk
    if [[ -s inventory/new_targets.txt ]]; then parallel-ssh -h inventory/new_targets.txt -P 'sudo mkdir -p /ais/'$disk; fi
done

echo Wait 30 sec before creating FS
sleep 30

echo Create FS
for disk in "$@"; do
    echo Create XFS on $disk
    parallel-ssh -h inventory/targets.txt -P 'sudo mkfs -t xfs /dev/'$disk
    if [[ -s inventory/new_targets.txt ]]; then parallel-ssh -h inventory/new_targets.txt -P 'sudo mkfs -t xfs /dev/'$disk; fi
done

