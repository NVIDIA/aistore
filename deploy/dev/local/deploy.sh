#!/bin/bash

############################################
#
# Usage: deploy.sh
#
# To deploy AIStore as a next tier cluster to the *already running*
# AIStore cluster set DEPLOY_AS_NEXT_TIER=1.
#
# NOTE: all environment variables are listed in the `env` package.
# Please see https://github.com/NVIDIA/aistore/blob/master/api/env
# for the most recently updated (AIS + AuthN) environment.
#
############################################

if ! command -v go &> /dev/null; then
  echo "Go (toolchain) is not installed"
  echo "Use https://go.dev/dl to install the required (as per go.mod) version of Go"
  echo "See https://aiatscale.org/docs/getting-started for step-by-step instruction"
  exit 1
fi

if [[ -z $GOPATH ]]; then
  echo "Warning: GOPATH variable is not defined, using home directory ${HOME}"
  echo "(Tip: see https://aiatscale.org/docs/getting-started for step-by-step instruction)"
  echo ""
  if [ ! -d "${HOME}/go/pkg" ]; then
    echo "${HOME}/go/pkg does not exist (deploying the very first time and from scratch?)"
    echo "(Tip: run 'make mod-tidy' to download required packages)"
    mkdir -p "${HOME}/go/pkg"
    echo ""
  fi
  if [ ! -w "${HOME}/go/pkg" ]; then
    echo "${HOME}/go/pkg is not writable - exiting"
    exit 1
  fi
else
  if [ ! -d "${GOPATH}/pkg" ]; then
    echo "${GOPATH}/pkg does not exist (deploying the very first time and from scratch?)"
    echo "(Tip: run 'make mod-tidy' to download required packages)"
    mkdir -p "${GOPATH}/pkg"
    echo ""
  fi
  if [ ! -w "${GOPATH}/pkg" ]; then
    echo "${GOPATH}/pkg is not writable - exiting"
    exit 1
  fi
fi

AISTORE_DIR=$(cd "$(dirname "$0")/../../../"; pwd -P) # absolute path to aistore directory
source $AISTORE_DIR/deploy/dev/utils.sh
AIS_USE_HTTPS=${AIS_USE_HTTPS:-false}
AIS_HTTP_CHUNKED_TRANSFER=true
HTTP_WRITE_BUFFER_SIZE=65536
HTTP_READ_BUFFER_SIZE=65536
if [[ -z $DEPLOY_AS_NEXT_TIER ]]; then
  PORT=${PORT:-8080}
  PORT_INTRA_CONTROL=${PORT_INTRA_CONTROL:-9080}
  PORT_INTRA_DATA=${PORT_INTRA_DATA:-10080}
  NEXT_TIER=
else
  PORT=${PORT:-11080}
  PORT_INTRA_CONTROL=${PORT_INTRA_CONTROL:-12080}
  PORT_INTRA_DATA=${PORT_INTRA_DATA:-13080}
  NEXT_TIER="_next"
fi
AIS_PRIMARY_URL="http://localhost:$PORT"
if $AIS_USE_HTTPS; then
  AIS_PRIMARY_URL="https://localhost:$PORT"
fi
LOG_ROOT="${LOG_ROOT:-/tmp/ais}${NEXT_TIER}"
#### Authentication setup #########
AIS_SECRET_KEY="${AIS_SECRET_KEY:-aBitLongSecretKey}"
AIS_AUTHN_ENABLED="${AIS_AUTHN_ENABLED:-false}"
AIS_AUTHN_SU_NAME="${AIS_AUTHN_SU_NAME:-admin}"
AIS_AUTHN_SU_PASS="${AIS_AUTHN_SU_PASS:-admin}"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
AIS_CONF_DIR="$HOME/.ais$NEXT_TIER"
TEST_FSPATH_COUNT=1

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null; then
  exit_error "TCP port $PORT is not open (check if AIStore is already running)"
fi
TMPF=$(mktemp /tmp/ais$NEXT_TIER.XXXXXXXXX)
touch $TMPF;
OS=$(uname -s)
case $OS in
  Linux) # Linux
    is_command_available "iostat" "-V"
    is_command_available "lsblk" "--version"
    is_command_available "df" "--version"
    setfattr -n user.comment -v comment $TMPF
    ;;
  Darwin) # macOS
    is_command_available "df" "--version"
    xattr -w user.comment comment $TMPF
    echo "WARNING: Darwin architecture is not yet fully supported. You may stumble upon bugs and issues when testing on Mac."
    ;;
  *)
    rm $TMPF 2>/dev/null
    exit_error "'${OS}' is not supported"
    ;;
esac
if [ $? -ne 0 ]; then
  rm $TMPF 2>/dev/null
  exit_error "bad kernel configuration: extended attributes are not enabled"
fi
rm $TMPF 2>/dev/null

# Read target count
echo "Enter number of storage targets:"
read -r TARGET_CNT
is_number ${TARGET_CNT}

# Read proxy count
echo "Enter number of proxies (gateways):"
read -r PROXY_CNT
is_number ${PROXY_CNT}
if  [[ ${PROXY_CNT} -lt 1 ]] ; then
  exit_error "${PROXY_CNT} must be at least 1"
fi
if [[ ${PROXY_CNT} -gt 1 ]] ; then
  AIS_DISCOVERY_PORT=$((PORT + 1))
  AIS_DISCOVERY_URL="http://localhost:$AIS_DISCOVERY_PORT"
  if $AIS_USE_HTTPS; then
    AIS_DISCOVERY_URL="https://localhost:$AIS_DISCOVERY_PORT"
  fi
fi

START=0
END=$((TARGET_CNT + PROXY_CNT - 1))

echo "Number of local mountpaths (enter 0 for preconfigured filesystems):"
read test_fspath_cnt
is_number ${test_fspath_cnt}
TEST_FSPATH_COUNT=${test_fspath_cnt}

TEST_LOOPBACK_COUNT=0

# If not specified, AIS_BACKEND_PROVIDERS will remain empty (or `0`) and
# aisnode build will include neither AWS ("aws") nor GCP ("gcp").

parse_backend_providers

create_loopbacks

if ! AIS_BACKEND_PROVIDERS=${AIS_BACKEND_PROVIDERS}  make --no-print-directory -C ${AISTORE_DIR} node; then
  exit_error "failed to compile 'aisnode' binary"
fi

mkdir -p $AIS_CONF_DIR

# Not really used for local testing but to keep aisnode_config.sh quiet
GRAPHITE_PORT=2003
GRAPHITE_SERVER="127.0.0.1"
COLLECTD_CONF_FILE=$AIS_CONF_DIR/collectd.conf
STATSD_CONF_FILE=$AIS_CONF_DIR/statsd.conf

#
# generate conf file(s) based on the settings/selections above
#
if [ "${!#}" != "--dont-generate-configs" ]; then
  for (( c=START; c<=END; c++ )); do
    AIS_CONF_DIR="$HOME/.ais$NEXT_TIER$c"
    INSTANCE=$c
    mkdir -p "$AIS_CONF_DIR"
    AIS_CONF_FILE="$AIS_CONF_DIR/ais.json"
    AIS_LOCAL_CONF_FILE="$AIS_CONF_DIR/ais_local.json"
    AIS_LOG_DIR="$LOG_ROOT/$c/log"
    source "${AISTORE_DIR}/deploy/dev/local/aisnode_config.sh"

    ((PORT++))
    ((PORT_INTRA_CONTROL++))
    ((PORT_INTRA_DATA++))
  done
fi

# run all daemons
CMD="${GOPATH}/bin/aisnode"
listening_on="Proxies are listening on ports: "
if [ $PROXY_CNT -eq 1 ]; then
  listening_on="Proxy is listening on port: "
fi
loopback=""
if [[ "$TEST_LOOPBACK_COUNT" != "0" ]] ; then
  loopback="-loopback"
fi
for (( c=START; c<=END; c++ )); do
  AIS_CONF_DIR="$HOME/.ais${NEXT_TIER}$c"
  AIS_CONF_FILE="$AIS_CONF_DIR/ais.json"
  AIS_LOCAL_CONF_FILE="$AIS_CONF_DIR/ais_local.json"

  PROXY_PARAM="${AIS_NODE_FLAGS} -config=${AIS_CONF_FILE} -local_config=${AIS_LOCAL_CONF_FILE} -role=proxy -ntargets=${TARGET_CNT} ${RUN_ARGS}"
  TARGET_PARAM="${AIS_NODE_FLAGS} -config=${AIS_CONF_FILE} -local_config=${AIS_LOCAL_CONF_FILE} -role=target ${RUN_ARGS} $loopback"

  pub_port=$(grep "\"port\":" ${AIS_LOCAL_CONF_FILE} | awk '{ print $2 }')
  pub_port=${pub_port:1:$((${#pub_port} - 3))}
  if [[ $c -eq 0 ]]; then
    export AIS_IS_PRIMARY="true"
    run_cmd "${CMD} ${PROXY_PARAM}"
    listening_on+=${pub_port}
    unset AIS_IS_PRIMARY

    # Wait for the proxy to start up
    sleep 2
  elif [[ $c -lt ${PROXY_CNT} ]]; then
    run_cmd "${CMD} ${PROXY_PARAM}"
    listening_on+=", ${pub_port}"
  else
    run_cmd "${CMD} ${TARGET_PARAM}"
  fi
done

if [[ $AIS_AUTH_ENABLED == "true" ]]; then
	exit_error "env var 'AIS_AUTH_ENABLED' is deprecated (use 'AIS_AUTHN_ENABLED')"
	exit 1
fi
if [[ $AIS_AUTHN_ENABLED == "true" ]]; then
  # conf file for authn
  AIS_AUTHN_CONF_DIR="$HOME/.ais/authn"
  mkdir -p "$AIS_AUTHN_CONF_DIR"
  AIS_AUTHN_CONF_FILE="$AIS_AUTHN_CONF_DIR/authn.json"
  AIS_AUTHN_LOG_DIR="$LOG_ROOT/authn/log"
  source "${AISTORE_DIR}/deploy/dev/local/authn_config.sh"

  if ! make --no-print-directory -C ${AISTORE_DIR} authn; then
    exit_error "failed to compile 'authn' binary"
  fi
  run_cmd "${GOPATH}/bin/authn -config=${AIS_AUTHN_CONF_FILE}"
fi

if [[ $MODE == "debug" ]]; then
   sleep 1.5
else
   sleep 0.1
fi

if command -v pgrep &> /dev/null; then
   run_count=$(pgrep -a aisnode | grep -c "${NEXT_TIER}")
   if [[ "${run_count}" -eq $((TARGET_CNT + PROXY_CNT)) ]]; then
      echo "${listening_on}"
   fi
else
   echo "Warning: pgrep not found"
   echo "${listening_on}"
fi
