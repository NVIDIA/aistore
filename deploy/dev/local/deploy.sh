#!/bin/bash

############################################
#
# Usage: deploy.sh
#
# To deploy AIStore as a next tier cluster to the *already running*
# AIStore cluster set DEPLOY_AS_NEXT_TIER=1.
#
############################################

# https://golang.org/doc/go1.12#runtime - revert behavior (to go1.11) of
# releasing memory to the system.
export GODEBUG=madvdontneed=1

printError() {
  echo "Error: $1."
  exit 1
}

is_number() {
  if ! [[ "$1" =~ ^[0-9]+$ ]] ; then
    printError "'$1' is not a number"
  fi
}

isCommandAvailable() {
  if [[ -z $(command -v "$1") ]]; then
    printError "command '$1' not available"
  fi
}

runCmd() {
  set -x
  $@ &
  { set +x; } 2>/dev/null
}

AISTORE_DIR=$(cd "$(dirname "$0")/../../../"; pwd -P) # absolute path to aistore directory

# Used to determine ProjectID field for *your* Google Cloud project.
# Must be set iff AIS_CLD_PROVIDER (see below) is Google (aka GCP or "gcp").
if [ -z "$GOOGLE_CLOUD_PROJECT" ]
then
	export GOOGLE_CLOUD_PROJECT="random-word-123456"
fi

AIS_USE_HTTPS=${AIS_USE_HTTPS:-false}
CHUNKED_TRANSFER=true
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
AUTH_ENABLED="${AUTH_ENABLED:-false}"
AUTH_SU_NAME="${AUTH_SU_NAME:-admin}"
AUTH_SU_PASS="${AUTH_SU_PASS:-admin}"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
AIS_CONF_DIR="$HOME/.ais$NEXT_TIER"
TEST_FSPATH_COUNT=1

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null; then
  printError "TCP port $PORT is not open (check if AIStore is already running)"
fi
TMPF=$(mktemp /tmp/ais$NEXT_TIER.XXXXXXXXX)
touch $TMPF;
OS=$(uname -s)
case $OS in
  Linux) # Linux
    isCommandAvailable "iostat" "-V"
    isCommandAvailable "lsblk" "--version"
    isCommandAvailable "df" "--version"
    setfattr -n user.comment -v comment $TMPF
    ;;
  Darwin) # macOS
    isCommandAvailable "df" "--version"
    xattr -w user.comment comment $TMPF
    echo "WARNING: Darwin architecture is not yet fully supported. You may stumble upon bugs and issues when testing on Mac."
    ;;
  *)
    rm $TMPF 2>/dev/null
    printError "'${OS}' is not supported"
    ;;
esac
if [ $? -ne 0 ]; then
  rm $TMPF 2>/dev/null
  printError "bad kernel configuration: extended attributes are not enabled"
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
  printError "${PROXY_CNT} must be at least 1"
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

echo "Number of local cache directories (enter 0 for preconfigured filesystems):"
read test_fspath_cnt
is_number ${test_fspath_cnt}
TEST_FSPATH_COUNT=${test_fspath_cnt}

# If not specified, AIS_CLD_PROVIDER will remain empty (or `0`) and
# aisnode build will include neither AWS ("aws") nor GCP ("gcp").

AIS_CLD_PROVIDER=""
echo "Select:"
echo " 0: No 3rd party Cloud"
echo " 1: Amazon S3"
echo " 2: Google Cloud Storage"
echo " 3: Azure Cloud"
read -r cld_provider
is_number ${cld_provider}

if [[ ${cld_provider} -eq 0 ]]; then
  AIS_CLD_PROVIDER=""
elif [[ ${cld_provider} -eq 1 ]]; then
  AIS_CLD_PROVIDER="aws"
elif [[ ${cld_provider} -eq 2 ]]; then
  AIS_CLD_PROVIDER="gcp"
elif [[ ${cld_provider} -eq 3 ]]; then
  AIS_CLD_PROVIDER="azure"
else
  printError "${cld_provider} is not a valid entry - expecting 0, 1, 2, or 3"
fi

if ! AIS_CLD_PROVIDER=${AIS_CLD_PROVIDER} make --no-print-directory -C ${AISTORE_DIR} node; then
  printError "failed to compile 'aisnode' binary"
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
for (( c=START; c<=END; c++ )); do
  AIS_CONF_DIR="$HOME/.ais$NEXT_TIER$c"
  INSTANCE=$c
  mkdir -p "$AIS_CONF_DIR"
  AIS_CONF_FILE="$AIS_CONF_DIR/ais.json"
  AIS_LOG_DIR="$LOG_ROOT/$c/log"
  source "${AISTORE_DIR}/deploy/dev/local/aisnode_config.sh"

  ((PORT++))
  ((PORT_INTRA_CONTROL++))
  ((PORT_INTRA_DATA++))
done

# run proxy and storage targets
CMD="${GOPATH}/bin/aisnode"
for (( c=START; c<=END; c++ )); do
  AIS_CONF_DIR="$HOME/.ais${NEXT_TIER}$c"
  AIS_CONF_FILE="$AIS_CONF_DIR/ais.json"

  PROXY_PARAM="${AIS_NODE_FLAGS} -config=${AIS_CONF_FILE} -role=proxy -ntargets=${TARGET_CNT} $1 $2"
  TARGET_PARAM="${AIS_NODE_FLAGS} -config=${AIS_CONF_FILE} -role=target $1 $2"

  if [[ $c -eq 0 ]]; then
    export AIS_IS_PRIMARY="true"
    runCmd "${CMD} ${PROXY_PARAM}"
    unset AIS_IS_PRIMARY

    # Wait for the proxy to start up
    sleep 2
  elif [[ $c -lt ${PROXY_CNT} ]]; then
    runCmd "${CMD} ${PROXY_PARAM}"
  else
    runCmd "${CMD} ${TARGET_PARAM}"
  fi
done

if [[ $AUTH_ENABLED == "true" ]]; then
  # conf file for authn
  AUTHN_CONF_DIR="$HOME/.authn"
  mkdir -p "$AUTHN_CONF_DIR"
  AUTHN_CONF_FILE="$AUTHN_CONF_DIR/authn.json"
  AUTHN_LOG_DIR="$LOG_ROOT/authn/log"
  source "${AISTORE_DIR}/deploy/dev/local/authn_config.sh"

  if ! make --no-print-directory -C ${AISTORE_DIR} authn; then
    printError "failed to compile 'authn' binary"
  fi
  runCmd "${GOPATH}/bin/authn -config=${AUTHN_CONF_FILE}"
fi

sleep 0.1

echo "Done."
