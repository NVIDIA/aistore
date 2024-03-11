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
aws_provider="n"
azure_provider="n"
gcp_provider="n"
hdfs_provider="n"
loopback=0
target_cnt=5
proxy_cnt=5
mountpath_cnt=5
deployment="local"
remote_alias="rmtais"
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
  --remote-alias      Alias to assign to the remote cluster (default: 'rmtais')
  --aws               Support AWS S3 backend (i.e., build \`aisnode\` executable with AWS S3 SDK)
  --gcp               Support Google Cloud Platform (i.e., build \`aisnode\` with libraries to access GCP)
  --azure             Support Azure Cloud (experimental)
  --hdfs              Support HDFS as a backend provider (experimental)
  --loopback          Loopback device size, e.g. 10G, 100M (default: 0). Zero size means: no loopbacks.
  --dir               The root directory of the aistore repository
  --https             Use HTTPS
  --override_backends Configure remote backends at deployment time (override previously stored backend configuration)
  --standby           When starting up, do not join cluster - wait instead for admin request (advanced usage, target-only)
  --transient         Do not store config changes, keep all the updates in memory
  -h, --help          Show this help text
"

# NOTE: `AIS_USE_HTTPS` and other system environment variables are listed in the `env` package:
# https://github.com/NVIDIA/aistore/blob/main/api/env/README.md

export MODE="debug"

# NOTE: additional `aisnode` command-line (run `aisnode --help`)
export RUN_ARGS=""

while (( "$#" )); do
  case "${1}" in
    -h|--help) echo -n "${usage}"; exit;;

    --aws)   aws_provider="y";   shift;;
    --azure) azure_provider="y"; shift;;
    --gcp)   gcp_provider="y";   shift;;
    --hdfs)  hdfs_provider="y";  shift;;
    --loopback) loopback=$2;  shift; shift;;
    --dir) root_dir=$2; shift; shift;;
    --deployment) deployment=$2; shift; shift;;
    --remote-alias) remote_alias=$2; shift; shift;;
    --target-cnt) target_cnt=$2; shift; shift;;
    --proxy-cnt) proxy_cnt=$2; shift; shift;;
    --mountpath-cnt) mountpath_cnt=$2; shift; shift;;
    --cleanup) cleanup="true"; shift;;
    --transient) RUN_ARGS="$RUN_ARGS -transient"; shift;;
    --standby) RUN_ARGS="$RUN_ARGS -standby"; shift;;
    --override_backends) RUN_ARGS="$RUN_ARGS -override_backends"; shift;;
    --override-backends) RUN_ARGS="$RUN_ARGS -override_backends"; shift;;
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

case "${deployment}" in
  local|remote|all)
    ;;
  *)
    echo "fatal: unknown --deployment argument value '${deployment}' (expected one of: 'local', 'remote', 'all')"
    exit 1
    ;;
esac

pushd "${root_dir}" 1>/dev/null

make kill
if [[ ${cleanup} == "true" ]]; then
  make clean
fi

if [[ ${deployment} == "local" || ${deployment} == "all" ]]; then
  echo -e "${target_cnt}\n${proxy_cnt}\n${mountpath_cnt}\n${aws_provider}\n${gcp_provider}\n${azure_provider}\n${hdfs_provider}\n${loopback}\n" |\
	  make deploy "RUN_ARGS=${RUN_ARGS}"
fi

make -j8 authn aisloader cli 1>/dev/null # Build binaries in parallel

if [[ ${deployment} == "remote" || ${deployment} == "all" ]]; then
  if [[ ${deployment} == "all" ]]; then
    echo -e "\n*** Remote cluster ***"
  fi
  echo -e "1\n1\n3\n${aws_provider}\n${gcp_provider}\n${azure_provider}\n${hdfs_provider}\n${loopback}\n" | DEPLOY_AS_NEXT_TIER="true" AIS_AUTHN_ENABLED=false make deploy

  # Do not try attach remote cluster if the main cluster did not start.
  if [[ ${deployment} == "all" ]]; then
    tier_endpoint="http://127.0.0.1:11080"
    if [[ -n ${AIS_USE_HTTPS} ]]; then
      tier_endpoint="https://127.0.0.1:11080"
    fi
    sleep 5
    if [[ ${AIS_AUTHN_ENABLED} == "true" ]]; then
       tokenfile=$(mktemp -q /tmp/ais.auth.token.XXXXXX)
       ais auth login admin -p admin -f ${tokenfile}
       export AIS_AUTHN_TOKEN_FILE=${tokenfile}
    fi
    retry ais cluster remote-attach "${remote_alias}=${tier_endpoint}"
  fi
fi

popd 1>/dev/null
