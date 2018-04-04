#!/bin/bash
sudo umount -f /dfc/1
sudo umount -f /dfc/2
sudo umount -f /dfc/3
sudo umount -f /dfc/4
sudo umount -f /dfc/5
sudo umount -f /dfc/6
sudo umount -f /dfc/7
sudo umount -f /dfc/8
sudo mkfs -t ext4 /dev/xvdb
sudo mkfs -t ext4 /dev/xvdc
sudo mkfs -t ext4 /dev/xvdd
sudo mkfs -t ext4 /dev/xvde
sudo mkfs -t ext4 /dev/xvdf
sudo mkfs -t ext4 /dev/xvdg
sudo mkfs -t ext4 /dev/xvdh
sudo mkfs -t ext4 /dev/xvdi
sudo mkdir /dfc
sudo mkdir /dfc/1 /dfc/2 /dfc/3 /dfc/4 /dfc/5 /dfc/6 /dfc/7 /dfc/8
sudo mount /dev/xvdb /dfc/1
sudo mount /dev/xvdc /dfc/2
sudo mount /dev/xvdd /dfc/3
sudo mount /dev/xvde /dfc/4
sudo mount /dev/xvdf /dfc/5
sudo mount /dev/xvdg /dfc/6
sudo mount /dev/xvdh /dfc/7
sudo mount /dev/xvdi /dfc/8

