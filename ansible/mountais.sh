#!/bin/bash
set -e
#mount file systems
echo umount ais file system if any mounted
for i in `mount | grep ais | cut -d' ' -f3`; do sudo umount $i; done
echo mounting file systems
for disk in "$@"; do
    sudo mount -t xfs /dev/$disk -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /ais/$disk
done

