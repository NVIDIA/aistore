#!/bin/bash
set -e
#mount file systems
echo umount dfc file system if any mounted
mount
for i in `mount | grep dfc | cut -d' ' -f3`; do sudo umount $i; done
mount
echo mounting file systems
sudo mount -t xfs /dev/md0 -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/1
sudo mount -t xfs /dev/md1 -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/2
sudo mount -t xfs /dev/md2 -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/3
sudo mount -t xfs /dev/md3 -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/4
sudo mount -t xfs /dev/md4 -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/5
sudo mount -t xfs /dev/md5 -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/6

