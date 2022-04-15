#!/bin/bash

print_error() {
  echo "Error: $1."
  exit 1
}

is_number() {
  if ! [[ "$1" =~ ^[0-9]+$ ]] ; then
    print_error "'$1' is not a number"
  fi
}

is_boolean() {
  if ! [[ "$1" =~ ^[yn]+$ ]] ; then
    print_error "input is not a 'y' or 'n'"
  fi
}

is_command_available() {
  if [[ -z $(command -v "$1") ]]; then
    print_error "command '$1' not available"
  fi
}

run_cmd() {
  set -x
  $@ &
  { set +x; } 2>/dev/null
}

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

create_loopback_paths() {
  echo "Create loopback devices (note that it may take some time): (y/n) ?"
  read -r create_mount_points
  if [[ "$create_mount_points" == "y" ]] ; then
    for ((i=1; i<=TEST_FSPATH_COUNT; i++)); do
      dir=${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}mp${i}
      mkdir -p "${dir}"

      if mount | grep "${dir}" > /dev/null; then
        continue
      fi

      sudo dd if=/dev/zero of="${dir}.img" bs=100M count=50
      sudo losetup -fP "${dir}.img"
      sudo mkfs.ext4 "${dir}.img"

      device=$(sudo losetup -l | grep "${dir}.img" | awk '{print $1}')
      sudo mount -o loop "${device}" "${dir}"
      sudo chown -R ${USER}: "${dir}"
    done
  fi
}

clean_loopback_paths() {
  for mpath in $(df -h | grep /tmp/ais/mp | awk '{print $1}'); do
    sudo umount "${mpath}"
  done

  if [[ -x "$(command -v losetup)" ]]; then
    for mpath in $(losetup -l | grep /tmp/ais/mp | awk '{print $1}'); do
      sudo losetup -d "${mpath}"
    done
  fi
}
