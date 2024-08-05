#!/bin/bash

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
        sleep $delay;
      else
        echo "The command has failed after $n attempts." >&2
        exit 1
      fi
    }
  done
}

root_dir="$(cd "$(dirname "$0")/../"; pwd -P)" ## NOTE: this assumes `clean_deploy.sh` itself is one level below

# Default values
AIS_BACKEND_PROVIDERS=""

loopback=0
target_cnt=5
proxy_cnt=5
mountpath_cnt=5
deployment="local"
remote_alias="remais"
cleanup="false"

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
  --aws               Build with AWS S3 backend
  --gcp               Build with Google Cloud Storage backend
  --azure             Build with Azure Blob Storage backend
  --ht                Build with ht:// backend (experimental)
  --loopback          Loopback device size, e.g. 10G, 100M (default: 0). Zero size means emulated mountpaths (with no loopback devices).
  --dir               The root directory of the aistore repository
  --https             Use HTTPS (note: X509 certificates may be required)
  --standby           When starting up, do not join cluster - wait instead for admin request (advanced usage, target-only)
  --transient         Do not store config changes, keep all the updates in memory
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

while (( "$#" )); do
  case "${1}" in
    -h|--help) echo -n "${usage}"; exit;;

    --aws)   AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} aws"; shift;;
    --azure) AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} azure"; shift;;
    --gcp)   AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} gcp"; shift;;
    --ht)    AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS} ht"; shift;;

    --loopback) loopback=$2;

      # if loopback is empty stop and notify
      validate_arg $1 $2

      shift 2;;
    --dir) root_dir=$2;

      # if dir is empty stop and notify
      if [ -z "$root_dir" ] || [[ "$root_dir" == -* ]]; then
        echo "Error: --dir option requires a non-empty value."
        exit 1
      fi

      shift 2;;
    --deployment) deployment=$2;

      # if deployment is empty stop and notify
      validate_arg $1 $2

      # if deployment is invalid stop and notify
      if [[ "$deployment" != "local" && "$deployment" != "remote" && "$deployment" != "all" ]]; then
        echo "fatal: unknown --deployment argument value '${deployment}' (expected one of: 'local', 'remote', 'all')"
        exit 1
      fi

      shift 2;;
    --remote-alias) remote_alias=$2;

      # if remote-alias is empty stop and notify
      validate_arg $1 $2

      shift 2;;
    --target-cnt) target_cnt=$2;

      # if the target-cnt is empty stop and notify
      validate_arg $1 $2

      shift 2;;
    --proxy-cnt) proxy_cnt=$2;

      # if the proxy-cnt is empty stop and notify
      validate_arg $1 $2

      shift 2;;
    --mountpath-cnt) mountpath_cnt=$2;

      # if the mountpath-cnt is empty stop and notify
      validate_arg $1 $2

      shift 2;;
    --debug) export MODE="debug"; shift;;
    --cleanup) cleanup="true"; shift;;
    --transient) RUN_ARGS="$RUN_ARGS -transient"; shift;;
    --standby) RUN_ARGS="$RUN_ARGS -standby"; shift;;
    --https)
      export AIS_USE_HTTPS="true"
      export AIS_SKIP_VERIFY_CRT="true"
      export AIS_SERVER_CRT="${AIS_SERVER_CRT:$HOME/localhost.crt}"
      export AIS_SERVER_KEY="${AIS_SERVER_KEY:$HOME/localhost.key}"
      shift
      ;;
    -*) RUN_ARGS="$RUN_ARGS ${1}"; shift;; ## NOTE: catch-all here assumes that everything that falls through the switch is binary

    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

pushd "${root_dir}" 1>/dev/null

make kill
if [[ ${cleanup} == "true" ]]; then
  make clean
fi

if [[ ${deployment} == "local" || ${deployment} == "all" ]]; then
  echo -e "${target_cnt}\n${proxy_cnt}\n${mountpath_cnt}\nn\nn\nn\n${loopback}\n" |\
	  AIS_BACKEND_PROVIDERS="${AIS_BACKEND_PROVIDERS}" make deploy "RUN_ARGS=${RUN_ARGS}"
fi

make -j8 authn aisloader cli 1>/dev/null # Build binaries in parallel

if [[ ${deployment} == "remote" || ${deployment} == "all" ]]; then
  if [[ ${deployment} == "all" ]]; then
    echo -e "\n*** Remote cluster ***"
  fi
  echo -e "1\n1\n3\n" | DEPLOY_AS_NEXT_TIER="true" AIS_AUTHN_ENABLED=false make deploy

  # Do not try attach remote cluster if the main cluster did not start.
  if [[ ${deployment} == "all" ]]; then
    tier_endpoint="http://127.0.0.1:11080"
    if [[ -n ${AIS_USE_HTTPS} ]]; then
      tier_endpoint="https://127.0.0.1:11080"
    fi
    sleep 7
    if [[ ${AIS_AUTHN_ENABLED} == "true" ]]; then
       tokenfile=$(mktemp -q /tmp/ais.auth.token.XXXXXX)
       ais auth login admin -p admin -f ${tokenfile}
       export AIS_AUTHN_TOKEN_FILE=${tokenfile}
    fi
    retry ais cluster remote-attach "${remote_alias}=${tier_endpoint}"
  fi
fi

popd 1>/dev/null
