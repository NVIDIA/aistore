#!/bin/bash
set -e
#create md volume
echo creating md volume
sudo mdadm --create  /dev/md0 --level=raid0 -c 1024K --raid-devices=2 /dev/xvdl /dev/xvdm
sudo mdadm --create  /dev/md1 --level=raid0 -c 1024K --raid-devices=2 /dev/xvdn /dev/xvdo
sudo mdadm --create  /dev/md2 --level=raid0 -c 1024K --raid-devices=2 /dev/xvdp /dev/xvdq
sudo mdadm --create  /dev/md3 --level=raid0 -c 1024K --raid-devices=2 /dev/xvdr /dev/xvds
sudo mdadm --create  /dev/md4 --level=raid0 -c 1024K --raid-devices=2 /dev/xvdt /dev/xvdu
sudo mdadm --create  /dev/md5 --level=raid0 -c 1024K --raid-devices=2 /dev/xvdv /dev/xvdw

#create file systems
echo creating file systems
sudo mkfs  -t xfs -f /dev/md0
sudo mkfs  -t xfs -f /dev/md1
sudo mkfs  -t xfs -f /dev/md2
sudo mkfs  -t xfs -f /dev/md3
sudo mkfs  -t xfs -f /dev/md4
sudo mkfs  -t xfs -f /dev/md5

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

