#!/bin/bash
set -xo

echo Creating disks total of $#

chmod 744 createdfcvolumes.sh
./createdfcvolumes.sh $#

echo Create FS dirs
for disk in "$@"; do
    parallel-ssh -h inventory/targets.txt -P 'sudo mkdir -p /dfc/'$disk
    parallel-ssh -h inventory/new_targets.txt -P 'sudo mkdir -p /dfc/'$disk
done

echo Wait 30 sec before creating FS
sleep 30

echo Create FS
for disk in "$@"; do
    echo Create XFS on $disk
    parallel-ssh -h inventory/targets.txt -P 'sudo mkfs -t xfs /dev/'$disk
    parallel-ssh -h inventory/new_targets.txt -P 'sudo mkfs -t xfs /dev/'$disk
done

