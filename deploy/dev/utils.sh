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

# NOTE 1:
# AIS_BACKEND_PROVIDERS and all other system environment variables are listed in the `env` package:
# https://github.com/NVIDIA/aistore/blob/main/api/env/README.md

# NOTE 2:
# defined AIS_BACKEND_PROVIDERS (empty or non-empty) always takes precedence over STDIN

set_env_backends() {
  if [[ ! -z $TAGS ]]; then
    ## env var TAGS may contain all build tags, including backends
    arr=( aws gcp azure )
    for b in "${arr[@]}"; do
      re="\\b$b\\b"
      if [[ $TAGS =~ $re && ! $AIS_BACKEND_PROVIDERS =~ $re ]]; then
        AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} $b"
      fi
    done
  fi

  if [[  -v AIS_BACKEND_PROVIDERS ]]; then
    ## env takes precedence over STDIN
    local orig=$AIS_BACKEND_PROVIDERS
    _set_env_backends
    AIS_BACKEND_PROVIDERS=$orig
  else
    _set_env_backends
  fi
}

_set_env_backends() {
  AIS_BACKEND_PROVIDERS=""
  echo "Select backend providers (press Enter at any point to stop adding backends):"
  echo "Amazon S3: (y/n) ?"
  read -r cld_aws
  if [[ "$cld_aws" == "" ]] ; then
    return
  fi
  is_boolean "${cld_aws}"
  if  [[ "${cld_aws}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} aws"
  fi
  echo "Google Cloud Storage: (y/n) ?"
  read -r cld_gcp
  if [[ "$cld_gcp" == "" ]] ; then
    return
  fi
  is_boolean "${cld_gcp}"
  if  [[ "${cld_gcp}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} gcp"
  fi
  echo "Azure: (y/n) ?"
  read -r cld_azure
  if [[ "$cld_azure" == "" ]] ; then
    return
  fi
  is_boolean "${cld_azure}"
  if  [[ "${cld_azure}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} azure"
  fi
}

make_backend_conf() {
  backend_conf=()
  for backend in ${AIS_BACKEND_PROVIDERS}; do
    case $backend in
      aws)   backend_conf+=('"aws":   {}') ;;
      azure) backend_conf+=('"azure": {}') ;;
      gcp)   backend_conf+=('"gcp":   {}') ;;
    esac
  done
  echo {$(IFS=$','; echo "${backend_conf[*]}")}
}

create_loopbacks_or_skip() {
  echo "Loopback device size, e.g. 10G, 100M (press Enter to skip): "
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
