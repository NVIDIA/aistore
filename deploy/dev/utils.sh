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

# NOTE:
# 1. AIS_BACKEND_PROVIDERS and all other system environment variables are listed in the `env` package:
#    https://github.com/NVIDIA/aistore/blob/main/api/env/README.md
# 2. environment AIS_BACKEND_PROVIDERS (empty or non-empty)
#    always takes precedence over STDIN
# 3. when adding/deleting backends, update the 3 (three) functions that follow below:

set_env_backends() {
  known_backends=( aws gcp azure oci ht )
  if [[ ! -z $TAGS ]]; then
    ## environment var TAGS may contain any/all build tags, including backends
    for b in "${known_backends[@]}"; do
      re="\\b$b\\b"
      if [[ $TAGS =~ $re ]]; then
        if [[ ! $AIS_BACKEND_PROVIDERS =~ $re ]]; then
          AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} $b"
	fi
	## dedup
        TAGS=${TAGS//$b/}
      fi
    done
  fi

  if [[ -n "${AIS_BACKEND_PROVIDERS+x}" ]]; then
    ## environment takes precedence over STDIN
    local orig=$AIS_BACKEND_PROVIDERS

    ## validate
    for b in ${AIS_BACKEND_PROVIDERS}; do
      case $b in
        aws)   ;;
        azure) ;;
        gcp)   ;;
        oci)   ;;
        ht)    ;;
        *)     echo "fatal: unknown backend '$b' in 'AIS_BACKEND_PROVIDERS=${AIS_BACKEND_PROVIDERS}'"; exit 1;;
      esac
    done
    ## consume (and discard) all reads
    _set_env_backends

    ## restore from env
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
  echo "OCI: (y/n) ?"
  read -r cld_oci
  if [[ "$cld_oci" == "" ]] ; then
    return
  fi
  is_boolean "${cld_oci}"
  if  [[ "${cld_oci}" == "y" ]] ; then
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} oci"
  fi
}

make_backend_conf() {
  backend_conf=()
  for backend in ${AIS_BACKEND_PROVIDERS}; do
    case $backend in
      aws)   backend_conf+=('"aws":   {}') ;;
      azure) backend_conf+=('"azure": {}') ;;
      gcp)   backend_conf+=('"gcp":   {}') ;;
      oci)   backend_conf+=('"oci":   {}') ;;
      ht)    backend_conf+=('"ht":    {}') ;;
    esac
  done
  echo {$(IFS=$','; echo "${backend_conf[*]}")}
}

read_fspath_count() {
  local test_fspath_cnt
  read test_fspath_cnt
  if [[ "$test_fspath_cnt" == "" ]] ; then
    return
  fi
  is_number ${test_fspath_cnt}
  echo ${test_fspath_cnt}
}

create_loopbacks_or_skip() {
  local loopback_size
  if [[  -n "${TEST_LOOPBACK_SIZE+x}" ]]; then
    ## environment takes precedence over STDIN
    local discard
    loopback_size=${TEST_LOOPBACK_SIZE}
    read -r discard
  else
    echo "Loopback device size, e.g. 10G, 100M (press Enter to skip): "
    read -r loopback_size
  fi

  ## check presence
  if [[ "$loopback_size" == "" || "$loopback_size" == "0" ]] ; then
    return
  fi

  ## check installed packages
  if ! command -v numfmt &> /dev/null; then
    exit_error "numfmt not found (tip: install GNU coreutils package)"
  fi
  if ! command -v losetup &> /dev/null; then
    exit_error "losetup not found (tip: install mount and/or klibc-utils package)"
  fi

  ## IEC units to bytes
  size=`numfmt --from=iec ${loopback_size^^}` || exit_error $?

  let mbcount=$size/1048576 # IEC mebibytes
  if [ $mbcount -lt 100 ] ; then
    exit_error "the minimum loopback size is 100M (got ${loopback_size})"
  fi

  ## create TEST_FSPATH_COUNT /dev/loop devices
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

set_env_tracing_or_skip() {
  echo "$AIS_TRACING_ENDPOINT"
  if [[ -n "${AIS_TRACING_ENDPOINT}" ]]; then
    TAGS="${TAGS} oteltracing"
    return
  fi

  echo "Enable distributed tracings (y/n)?"
  read -r enable_tracing

  ## check presence
  if [[ "$enable_tracing" == "" || "$enable_tracing" == "n" ]] ; then
    return
  fi

  is_boolean "${enable_tracing}"

  ## check presence
  if [[ "$enable_tracing" == "y" ]] ; then
    TAGS="${TAGS} oteltracing"
  else
    return
  fi

  echo "Exporter endpoint (default:'localhost:4317' jaeger)"
  read -r exporter_endpoint
  if [[ -z "${AIS_TRACING_ENDPOINT}" ]]; then
    AIS_TRACING_ENDPOINT=${exporter_endpoint:-"localhost:4317"}
  fi

  echo "Exporter auth-header (default:'')"
  read -r exporter_auth_header
  if [[ -z "${AIS_TRACING_AUTH_TOKEN_HEADER}" ]]; then
    AIS_TRACING_AUTH_TOKEN_HEADER=${exporter_auth_header}
  fi

  echo "Exporter auth-token-file (default:'')"
  read -r exporter_auth_token_file
  if [[ -z "${AIS_TRACING_AUTH_TOKEN_FILE}" ]]; then
    AIS_TRACING_AUTH_TOKEN_FILE={exporter_auth_token_file}
  fi
}


make_tracing_conf() {
  tracing_auth_conf=""
  if [[ -n "${AIS_TRACING_AUTH_TOKEN_HEADER}" && -n "${AIS_TRACING_AUTH_TOKEN_FILE}" ]]; then
    tracing_auth_conf=',
      "exporter_auth": {
        "token_header": "'"${AIS_TRACING_AUTH_TOKEN_HEADER}"'",
        "token_file": "'"${AIS_TRACING_AUTH_TOKEN_FILE}"'"
      }'
  fi

  tracing_conf=""
  if [[ -n "${AIS_TRACING_ENDPOINT}" ]]; then
    tracing_conf='
      "tracing": {
        "enabled": true,
        "exporter_endpoint": "'${AIS_TRACING_ENDPOINT}'",
        "skip_verify": true,
        "service_name_prefix": "'${AIS_TRACING_SERVICE_PREFIX:-aistore}'",
        "sampler_probability": "'${AIS_TRACING_SAMPLING_PROBABILITY:-1.0}'"'${tracing_auth_conf}'
      },
    '
  fi

  echo "${tracing_conf}"
}