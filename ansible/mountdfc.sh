#!/bin/bash
set -e
#mount file systems
echo umount dfc file system if any mounted
mount
for i in `mount | grep dfc | cut -d' ' -f3`; do sudo umount $i; done
mount
echo mounting file systems
sudo mount -t xfs /dev/xvdl -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdl
sudo mount -t xfs /dev/xvdm -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdm
sudo mount -t xfs /dev/xvdn -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdn
sudo mount -t xfs /dev/xvdo -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdo
sudo mount -t xfs /dev/xvdp -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdp
sudo mount -t xfs /dev/xvdq -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdq
sudo mount -t xfs /dev/xvdr -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdr
sudo mount -t xfs /dev/xvds -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvds
sudo mount -t xfs /dev/xvdt -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdt
sudo mount -t xfs /dev/xvdu -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdu
sudo mount -t xfs /dev/xvdv -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdv
sudo mount -t xfs /dev/xvdw -onoatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k,nobarrier /dfc/xvdw
