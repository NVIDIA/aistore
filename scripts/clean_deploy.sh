#!/bin/bash
#
# clean_deploy.sh - locally deploy AIS clusters for development
#
set -e

function retry {
  local n=1
  local max=5
  local delay=5
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "Command failed. Attempt $n/$max:"
        sleep $delay
      else
        echo "The command has failed after $n attempts." >&2
        exit 1
      fi
    }
  done
}

AISTORE_PATH="$(cd "$(dirname "$0")/../"; pwd -P)" ## NOTE: assumes `clean_deploy.sh` is one level below repo root

source "${AISTORE_PATH}/deploy/dev/utils.sh"

# Environment
#
export AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS:-""}"
export AIS_USE_IPv6="${AIS_USE_IPv6:-false}"
export AIS_USE_HTTPS="${AIS_USE_HTTPS:-false}"

# Default values
#
loopback=0
target_cnt=5
proxy_cnt=5
remote_target_cnt=1
remote_proxy_cnt=1
mountpath_cnt=5
deployment="local"
remote_alias="remais"
cleanup="false"
tracing="n"

usage="NAME:
  $(basename "$0") - locally deploy AIS clusters for development

USAGE:
  ./clean_deploy.sh [options...]

OPTIONS:
  --target-cnt        Number of target nodes in the cluster (default: 5)
  --proxy-cnt         Number of proxies/gateways (default: 5)
  --mountpath-cnt     Number of mountpaths (default: 5)
  --cleanup           Cleanup data and metadata from the previous deployments
  --deployment        Choose which AIS cluster(s) to deploy, one of: 'local', 'remote', 'all' (default: 'local')
  --remote-alias      Alias to assign to the remote cluster (default: 'remais')
  --remote-target-cnt Number of remote cluster (remais) target nodes in the cluster (default: 1)
  --remote-proxy-cnt  Number of remote proxies/gateways (default: 1)
  --aws               Build with AWS S3 backend
  --gcp               Build with Google Cloud Storage backend
  --azure             Build with Azure Blob Storage backend
  --oci               Build with OCI Object Storage backend
  --ht                Build with ht:// backend (experimental)
  --loopback          Loopback device size, e.g. 10G, 100M (default: 0). Zero size means emulated mountpaths (with no loopback devices).
  --dir               The root directory of the aistore repository
  --https             Use HTTPS (note: X509 certificates may be required)
  --ipv6              Use IPv6 (note: IPv4 is default)
  --standby           When starting up, do not join cluster - wait instead for admin request (advanced usage, target-only)
  --transient         Do not store config changes, keep all the updates in memory
  --tracing           Enable distributed tracing
  -h, --help          Show this help text
"

# NOTE: `AIS_USE_HTTPS` and other system environment variables are listed in the `env` package:
# https://github.com/NVIDIA/aistore/blob/main/api/env/README.md

# NOTE: additional `aisnode` command-line (run `aisnode --help`)
export RUN_ARGS=""

function validate_arg {
  local arg_name=$1
  local arg_value=$2
  if [ -z "$arg_value" ] || [[ "$arg_value" == -* ]]; then
    echo "Error: ${arg_name} option requires a non-empty value."
    exit 1
  fi
}

loopback_addr() {
  $AIS_USE_IPv6 && echo "::1" || echo "127.0.0.1"
}

while (( "$#" )); do
  case "${1}" in
    -h|--help)
      echo -n "${usage}"
      exit
      ;;

    --aws)   AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} aws"; shift;;
    --azure) AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} azure"; shift;;
    --gcp)   AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} gcp"; shift;;
    --oci)   AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} oci"; shift;;
    --ht)    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} ht"; shift;;

    --tracing)
      tracing="y\n${AIS_TRACING_ENDPOINT}\n${AIS_TRACING_AUTH_TOKEN_HEADER}\n${AIS_TRACING_AUTH_TOKEN_FILE}"
      shift
      ;;

    --loopback)
      validate_arg $1 $2
      loopback=$2
      shift 2
      ;;

    --dir)
      validate_arg $1 $2
      AISTORE_PATH=$2
      shift 2
      ;;

    --deployment)
      validate_arg $1 $2
      deployment=$2
      if [[ "$deployment" != "local" && "$deployment" != "remote" && "$deployment" != "all" ]]; then
        echo "fatal: unknown --deployment argument value '${deployment}' (expected one of: 'local', 'remote', 'all')"
        exit 1
      fi
      shift 2
      ;;

    --remote-alias)
      validate_arg $1 $2
      remote_alias=$2
      shift 2
      ;;

    --target-cnt)
      validate_arg $1 $2
      target_cnt=$2
      shift 2
      ;;

    --proxy-cnt)
      validate_arg $1 $2
      proxy_cnt=$2
      shift 2
      ;;

    --mountpath-cnt)
      validate_arg $1 $2
      mountpath_cnt=$2
      shift 2
      ;;

    --remote-proxy-cnt)
      validate_arg $1 $2
      remote_proxy_cnt=$2
      shift 2
      ;;

    --remote-target-cnt)
      validate_arg $1 $2
      remote_target_cnt=$2
      shift 2
      ;;

    --debug)
      export MODE="debug"
      shift
      ;;

    --cleanup)
      cleanup="true"
      shift
      ;;

    --transient)
      RUN_ARGS="$RUN_ARGS -transient"
      shift
      ;;

    --standby)
      RUN_ARGS="$RUN_ARGS -standby"
      shift
      ;;

    --https)
      AIS_USE_HTTPS=true
      export AIS_USE_HTTPS

      export AIS_SKIP_VERIFY_CRT="true"
      export AIS_SERVER_CRT="${AIS_SERVER_CRT:-$HOME/localhost.crt}"
      export AIS_SERVER_KEY="${AIS_SERVER_KEY:-$HOME/localhost.key}"
      shift
      ;;

    --ipv6)
      AIS_USE_IPv6=true
      export AIS_USE_IPv6
      shift
      ;;

    -*)
      # catch-all assumes anything falling through is a binary option for aisnode
      RUN_ARGS="$RUN_ARGS ${1}"
      shift
      ;;

    *)
      echo "fatal: unknown argument '${1}'"
      exit 1
      ;;
  esac
done

# If --dir was provided, re-source utils.sh from that repo path for consistency.
# (In practice: make_url comes from there and we want the chosen tree.)
source "${AISTORE_PATH}/deploy/dev/utils.sh"

pushd "${AISTORE_PATH}" 1>/dev/null

make kill
if [[ ${cleanup} == "true" ]]; then
  make clean
fi

if [[ ${deployment} == "local" || ${deployment} == "all" ]]; then
  echo -e "${target_cnt}\n${proxy_cnt}\n${mountpath_cnt}\nn\nn\nn\nn\n${loopback}\n${tracing}\n" | \
    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS}" make deploy "RUN_ARGS=${RUN_ARGS}"
fi

# Build binaries in parallel
make -j8 authn aisloader cli 1>/dev/null

if [[ ${deployment} == "remote" || ${deployment} == "all" ]]; then
  if [[ ${deployment} == "all" ]]; then
    echo -e "\n*** Remote cluster ***"
  fi

  ## NOTE: must have the same build tags and, in particular, same backends -
  ## otherwise, `make deploy` below will rebuild and replace aisnode binary
  echo -e "${remote_target_cnt}\n${remote_proxy_cnt}\n3\n" | \
    DEPLOY_AS_NEXT_TIER="true" AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS}" AIS_AUTHN_ENABLED=false make deploy

  # Do not try attach remote cluster if the main cluster did not start.
  if [[ ${deployment} == "all" ]]; then
    tier_endpoint=$(make_url "$(loopback_addr)" 11080)
    sleep 7

    if [[ ${AIS_AUTHN_ENABLED} == "true" ]]; then
      tokenfile=$(mktemp -q /tmp/ais.auth.token.XXXXXX)
      ais auth login admin -p admin -f "${tokenfile}"
      export AIS_AUTHN_TOKEN_FILE="${tokenfile}"
    fi

    retry ais cluster remote-attach "${remote_alias}=${tier_endpoint}"
  fi
fi

popd 1>/dev/null
