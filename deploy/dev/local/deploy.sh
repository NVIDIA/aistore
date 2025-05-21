#!/bin/bash

############################################
#
# Usage: deploy.sh
#
# To deploy AIStore as a next tier cluster to the *already running*
# AIStore cluster set DEPLOY_AS_NEXT_TIER=1.
#
# NOTE: system environment variables are listed in the `env` package.
# See https://github.com/NVIDIA/aistore/blob/main/api/env/README.md
#
# NOTE: by default, configuration files are stored under $HOME/.config/ais/<app-name>
# E.g., $HOME/.config/ais/authn - AuthN config, $HOME/.config/ais/cli - CLI config
# and so on. This rule holds for all AIS "applications" except `aisnode` itself.
# See https://github.com/NVIDIA/aistore/tree/main/cmn/fname for the most updated locations.
#
# NOTE: Prometheus is the Local Playground's default; use TAGS to specify `statsd` and/or
# any other non-default build tag.
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

## NOTE: absolute path to aistore root
## (should we use `git rev-parse --show-toplevel` instead?)
AISTORE_PATH=$(cd "$(dirname "$0")/../../../"; pwd -P)
source $AISTORE_PATH/deploy/dev/utils.sh

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
PRIMARY_HOST=${AIS_PRIMARY_HOST:-localhost}
AIS_PRIMARY_URL="http://$PRIMARY_HOST:$PORT"
if $AIS_USE_HTTPS; then
  AIS_PRIMARY_URL="https://$PRIMARY_HOST:$PORT"
fi
LOG_ROOT="${LOG_ROOT:-/tmp/ais}${NEXT_TIER}"
#### Authentication setup #########
AIS_AUTHN_SECRET_KEY="${AIS_AUTHN_SECRET_KEY:-aBitLongSecretKey}"
AIS_AUTHN_ENABLED="${AIS_AUTHN_ENABLED:-false}"
AIS_AUTHN_SU_NAME="${AIS_AUTHN_SU_NAME:-admin}"
AIS_AUTHN_SU_PASS="${AIS_AUTHN_SU_PASS:-admin}"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################

# NOTE:
# for system-wide conventions on locations of these and other configs,
# see: `cmn/fname`
AIS_CONF_DIR="$HOME/.ais$NEXT_TIER"
APP_CONF_DIR="$HOME/.config/ais"
mkdir -p $AIS_CONF_DIR
mkdir -p $APP_CONF_DIR
COLLECTD_CONF_FILE="${APP_CONF_DIR}/collectd.conf"
STATSD_CONF_FILE="${APP_CONF_DIR}/statsd.conf"

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null; then
  exit_error "TCP port $PORT is not open (check if AIStore is already running)"
fi

TMPF=$(mktemp /tmp/ais$NEXT_TIER.XXXXXXXXX)
touch $TMPF

OS=$(uname -s)
case $OS in
  Linux) # Linux
    if ! [ -x "$(command -v setfattr)" ]; then
      echo "Warning: setfattr not installed" >&2
    elif ! setfattr -n user.comment -v comment $TMPF; then
      echo "Warning: bad kernel configuration: extended attributes are not enabled."
    fi
    ;;
  Darwin) # macOS
    if ! xattr -w user.comment comment $TMPF; then
      echo "Warning: bad macOS configuration: extended attributes are not enabled."
    fi
    echo "Warning: Darwin architecture is not yet fully supported. You may stumble upon bugs and issues when testing on Mac."
    ;;
  *)
    rm $TMPF 2>/dev/null
    echo "Error: '${OS}' is not supported."
    exit 1
    ;;
esac

rm $TMPF 2>/dev/null

### begin reading STDIN =================== 5 steps below ========================================

# 1. read target count
  echo "Enter number of storage targets:"
  read -r TARGET_CNT
  is_number ${TARGET_CNT}

# 2. read proxy count
  echo "Enter number of proxies (gateways):"
  read -r PROXY_CNT
  is_number ${PROXY_CNT}
  if  [[ ${PROXY_CNT} -lt 1 && -z "${AIS_PRIMARY_HOST}" ]] ; then
    exit_error "Number of proxies must be at least 1 if no external primary proxy is specified with AIS_PRIMARY_HOST. Received "${PROXY_CNT}""
  fi
  if [[ ${PROXY_CNT} -gt 1 ]] ; then
    AIS_DISCOVERY_PORT=$((PORT + 1))
    AIS_DISCOVERY_URL="http://$PRIMARY_HOST:$AIS_DISCOVERY_PORT"
    if $AIS_USE_HTTPS; then
      AIS_DISCOVERY_URL="https://$PRIMARY_HOST:$AIS_DISCOVERY_PORT"
    fi
  fi

  START=0
  END=$((TARGET_CNT + PROXY_CNT - 1))

# 3. read mountpath count (and notice the default)
  if [[ ! -n "${TEST_FSPATH_COUNT+x}" ]]; then
    echo "Number of local mountpaths (enter 0 for preconfigured filesystems):"
    TEST_FSPATH_COUNT=$(read_fspath_count)
  fi
  ## default = 4
  if [[ -z $TEST_FSPATH_COUNT ]]; then
    TEST_FSPATH_COUNT=4
  fi

# 4. conditionally linked backends
  set_env_backends

# 5. /dev/loop* devices, if any
# see also: TEST_LOOPBACK_SIZE
  TEST_LOOPBACK_COUNT=0
  create_loopbacks_or_skip

# 6. conditionally enable distributed tracing
  set_env_tracing_or_skip

### end reading STDIN ============================ 5 steps above =================================


## NOTE: to enable StatsD instead of Prometheus, use build tag `statsd` in the make command, as follows:
## TAGS=statsd make ...
## For more information, see docs/build_tags.md and/or docs/monitoring-overview.md.
##
if ! TAGS=${TAGS} AIS_BACKEND_PROVIDERS=${AIS_BACKEND_PROVIDERS} make --no-print-directory -C ${AISTORE_PATH} node; then
  exit_error "failed to compile 'aisnode' binary"
fi

# Not really used for local testing but to keep aisnode_config.sh quiet
GRAPHITE_PORT=2003
GRAPHITE_SERVER="127.0.0.1"

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
    source "${AISTORE_PATH}/deploy/dev/local/aisnode_config.sh"

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
  if [[ $c -eq 0 && $PROXY_CNT -gt 0 ]]; then
    run_cmd "${CMD} ${PROXY_PARAM}"
    listening_on+=${pub_port}

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
  AIS_AUTHN_CONF_DIR="${APP_CONF_DIR}/authn"
  mkdir -p "$AIS_AUTHN_CONF_DIR"
  AIS_AUTHN_LOG_DIR="$LOG_ROOT/authn/log"
  source "${AISTORE_PATH}/deploy/dev/local/authn_config.sh"

  if ! make --no-print-directory -C ${AISTORE_PATH} authn; then
    exit_error "failed to compile 'authn' binary"
  fi
  run_cmd "${GOPATH}/bin/authn -config=${AIS_AUTHN_CONF_DIR}"
fi

if [[ $MODE == "debug" ]]; then
   sleep 1.5
else
   sleep 0.1
fi

##
## TODO: could be in fact HTTPS, not HTTP. Difficult to find out without introducing delay and checking the logs.
##
if command -v pgrep &> /dev/null; then
   run_count=$(pgrep -a aisnode | grep -c "${NEXT_TIER}")
   if [[ "${run_count}" -eq $((TARGET_CNT + PROXY_CNT)) ]]; then
      echo "${listening_on}"
      if [[ -z $DEPLOY_AS_NEXT_TIER ]]; then
         echo "Primary endpoint: ${AIS_PRIMARY_URL}"
      fi
   fi
else
   echo "Warning: pgrep not found"
   echo "${listening_on}"
fi
