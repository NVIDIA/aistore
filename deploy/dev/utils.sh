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

parse_cld_providers() {
  AIS_CLD_PROVIDERS=""
  echo "Select cloud providers:"
  echo "Amazon S3: (y/n) ?"
  read -r CLD_AWS
  is_boolean $CLD_AWS
  echo "Google Cloud Storage: (y/n) ?"
  read -r CLD_GCP
  is_boolean $CLD_GCP
  echo "Azure: (y/n) ?"
  read -r CLD_AZURE
  is_boolean $CLD_AZURE
  if  [[ "$CLD_AWS" == "y" ]] ; then
    AIS_CLD_PROVIDERS="${AIS_CLD_PROVIDERS} aws"
  fi
  if  [[ "$CLD_GCP" == "y" ]] ; then
    AIS_CLD_PROVIDERS="${AIS_CLD_PROVIDERS} gcp"
  fi
  if  [[ "$CLD_AZURE" == "y" ]] ; then
    AIS_CLD_PROVIDERS="${AIS_CLD_PROVIDERS} azure"
  fi
}

create_loopback_paths() {
    echo "Would you like to create loopback mount points"
    read -r create_mount_points
    if [[ "$create_mount_points" == "y" ]] ; then
       for (( i=1; i<=${TEST_FSPATH_COUNT}; i++ ))
       do
        dir=${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}mp${i}
        mkdir -p ${dir}

        if mount | grep ${dir} > /dev/null; then
            continue
        fi

        sudo dd if=/dev/zero of="${dir}.img" bs=100M count=50
        sudo losetup -fP "${dir}.img"
        sudo mkfs.ext4 "${dir}.img"

        device=$(sudo losetup -l | grep "${dir}.img" | awk '{print $1}')
        sudo mount -o loop ${device} ${dir}
        sudo chown -R ${USER}: ${dir}
       done
    fi
}

clean_loopback_paths() {
  for i in $(df -h | grep /tmp/ais/mp | awk '{print $1}'); do
    sudo umount ${i}
  done

  for i in $(losetup -l | grep /tmp/ais/mp | awk '{print $1}'); do
    sudo losetup -d ${i}
  done
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


