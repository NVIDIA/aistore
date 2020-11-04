#!/bin/bash
set -e

echo "Unmounting all AIS file systems"
for i in $(mount | grep ais | cut -d' ' -f3); do sudo umount $i; done
echo "Mounting file systems"
for disk in "$@"; do
    sudo mount -t xfs /dev/$disk -o noatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k /ais/$disk
done

