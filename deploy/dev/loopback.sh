#!/bin/bash

if ! [ -x "$(command -v losetup)" ]; then
  echo "Error: losetup not installed (apt-get mount or klibc-utils)" >&2
  exit 1
fi

## required:
while (( "$#" )); do
  case "${1}" in
    --mountpath) mountpath=$2; shift; shift;;
    --size) size=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

[[ ! -z $mountpath ]] || { echo "Error: mountpath not defined"; exit 1; }
[[ ! -z $size ]] || { echo "Error: size not defined"; exit 1; }

sz=`numfmt --from=iec ${size}` || exit_error $?
let mbcount=$sz/1048576 # IEC mebibytes
if [ $mbcount -lt 100 ] ; then
  echo "the minimum loopback size is 100M (have ${size})"
  exit 1
fi

mkdir -p $mountpath
dd if=/dev/zero of="${mountpath}.img" bs=1M count=1024
losetup -fP "${mountpath}.img"
mkfs.ext4 "${mountpath}.img" > /dev/null
device=$(losetup -l | grep "${mountpath}.img" | awk '{print $1}')
mount -o loop "${device}" $mountpath
