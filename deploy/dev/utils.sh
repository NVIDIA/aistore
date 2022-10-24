#!/bin/bash

exit_error() {
  echo "Error: $1."
  exit 1
}

is_number() {
  if ! [[ "$1" =~ ^[0-9]+$ ]] ; then
    exit_error "'$1' is not a number"
  fi
}

is_boolean() {
  if ! [[ "$1" =~ ^[yn]+$ ]] ; then
    exit_error "input is not a 'y' or 'n'"
  fi
}

is_command_available() {
  if [[ -z $(command -v "$1") ]]; then
    exit_error "command '$1' not available"
  fi
}

# uncomment to see the commands
run_cmd() {
#  set -x
  $@ &
#  { set +x; } 2>/dev/null
}

# NOTE: `AIS_BACKEND_PROVIDERS` and all other system environment variables
# are listed in the `env` package:
# https://github.com/NVIDIA/aistore/blob/master/api/env/README.md

parse_backend_providers() {
  AIS_BACKEND_PROVIDERS=""
  echo "Select backend providers:"
  echo "Amazon S3: (y/n) ?"
  read -r cld_aws
  is_boolean "${cld_aws}"
  echo "Google Cloud Storage: (y/n) ?"
  read -r cld_gcp
  is_boolean "${cld_gcp}"
  echo "Azure: (y/n) ?"
  read -r cld_azure
  is_boolean "${cld_azure}"
  echo "HDFS: (y/n) ?"
  read -r cld_hdfs
  is_boolean "${cld_hdfs}"

  if  [[ "${cld_aws}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} aws"
  fi
  if  [[ "${cld_gcp}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} gcp"
  fi
  if  [[ "${cld_azure}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} azure"
  fi
  if  [[ "${cld_hdfs}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} hdfs"
  fi
}

create_loopbacks() {
  echo "Loopback device size, e.g. 10G, 100M (creating loopbacks first time may take a while, press Enter to skip): "
  read -r loopback_size
  if [[ "$loopback_size" == "" || "$loopback_size" == "0" ]] ; then
    return
  fi
  if ! command -v numfmt &> /dev/null; then
    exit_error "numfmt not found (check GNU coreutils)"
  fi
  if ! command -v losetup &> /dev/null; then
	  exit_error "losetup not found (install mount or klibc-utils)"
  fi
  size=`numfmt --from=iec ${loopback_size}` || exit_error $?
  let mbcount=$size/1048576 # IEC mebibytes
  if [ $mbcount -lt 100 ] ; then
    exit_error "the minimum loopback size is 100M (got ${loopback_size})"
  fi
  for ((i=1; i<=TEST_FSPATH_COUNT; i++)); do
    dir=${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}mp${i}
    mkdir -p "${dir}"

    if mount | grep "${dir}" > /dev/null; then
      continue
    fi

    sudo dd if=/dev/zero of="${dir}.img" bs=1M count=$mbcount
    sudo losetup -fP "${dir}.img"
    sudo mkfs.ext4 "${dir}.img" > /dev/null

    device=$(sudo losetup -l | grep "${dir}.img" | awk '{print $1}')
    sudo mount -o loop "${device}" "${dir}"
    sudo chown -R ${USER}: "${dir}"
  done
  TEST_LOOPBACK_COUNT=$TEST_FSPATH_COUNT
}

rm_loopbacks() {
  for mpath in $(df -h | grep "/tmp/ais${NEXT_TIER}/mp" | awk '{print $1}'); do
    sudo umount "${mpath}"
  done

  if [[ -x "$(command -v losetup)" ]]; then
    for mpath in $(losetup -l | grep "/tmp/ais${NEXT_TIER}/mp" | awk '{print $1}'); do
      sudo losetup -d "${mpath}"
    done
  fi
}
