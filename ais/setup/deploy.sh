#!/bin/bash

############################################
#
# Usage: deploy.sh [-loglevel=0|1|2|3] [-stats_time=<DURATION>] [-list_time=<DURATION>]
#
# To deploy AIStore as a next tier cluster to the *already running*
# AIStore cluster set DEPLOY_AS_NEXT_TIER=1.
#
############################################

# https://golang.org/doc/go1.12#runtime - revert behavior (to go1.11) of
# releasing memory to the system.
export GODEBUG=madvdontneed=1

printError () {
  echo "Error: $1."
  exit 1
}

isCommandAvailable () {
  if [[ -z $(command -v "$1") ]]; then
    printError "command '$1' not available"
  fi
}

runCmd () {
  set -x
  $@ &
  { set +x; } 2>/dev/null
}

isCommandAvailable "lsblk" "--version"
isCommandAvailable "df" "--version"

AISTORE_DIR=$(cd "$(dirname "$0")"; realpath ../../) # absolute path to aistore directory

export GOOGLE_CLOUD_PROJECT="involuted-forge-189016"
USE_HTTPS=false
CHUNKED_TRANSFER=true
HTTP_WRITE_BUFFER_SIZE=65536
HTTP_READ_BUFFER_SIZE=65536
if [ "$DEPLOY_AS_NEXT_TIER" == "" ]
then
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
PROXYURL="http://localhost:$PORT"
if $USE_HTTPS; then
  PROXYURL="https://localhost:$PORT"
fi
LOGROOT="/tmp/ais$NEXT_TIER"
#### Authentication setup #########
SECRETKEY="${SECRETKEY:-aBitLongSecretKey}"
AUTHENABLED="${AUTHENABLED:-false}"
AUTH_SU_NAME="${AUTH_SU_NAME:-admin}"
AUTH_SU_PASS="${AUTH_SU_PASS:-admin}"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
CONFDIR="$HOME/.ais$NEXT_TIER"
TEST_FSPATH_COUNT=1

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null; then
  echo "Error: TCP port $PORT is not open (check if AIStore is already running)"
  exit 1
fi
TMPF=$(mktemp /tmp/ais$NEXT_TIER.XXXXXXXXX)
touch $TMPF;
OS=$(uname -s)
case $OS in
  Linux) #Linux
    isCommandAvailable "iostat" "-V"
    setfattr -n user.comment -v comment $TMPF
    ;;
  Darwin) #macOS
    xattr -w user.comment comment $TMPF
    ;;
  *)
    echo "Error: '$OS' is not supported"
    rm $TMPF 2>/dev/null
    exit 1
esac
if [ $? -ne 0 ]; then
  echo "Error: bad kernel configuration: extended attributes are not enabled"
  rm $TMPF 2>/dev/null
  exit 1
fi
rm $TMPF 2>/dev/null

# Read target count
echo "Enter number of storage targets:"
read TARGET_CNT
if ! [[ ${TARGET_CNT} =~ ^[0-9]+$ ]] ; then
  printError "${TARGET_CNT} is not a number"
fi

# Read proxy count
echo "Enter number of proxies (gateways):"
read PROXY_CNT
if ! [[ ${PROXY_CNT} =~ ^[0-9]+$ ]] ; then
  printError "${PROXY_CNT} is not a number"
elif  [[ ${PROXY_CNT} -lt 1 ]] ; then
  printError "${PROXY_CNT} must be at least 1"
fi
if [[ ${PROXY_CNT} -gt 1 ]] ; then
  DISCOVERYPORT=$((PORT + 1))
  DISCOVERYURL="http://localhost:$DISCOVERYPORT"
  if $USE_HTTPS; then
    DISCOVERYURL="https://localhost:$DISCOVERYPORT"
  fi
fi

START=0
END=$((TARGET_CNT + PROXY_CNT - 1))

echo "Number of local cache directories (enter 0 for preconfigured filesystems):"
read test_fspath_cnt
if ! [[ ${test_fspath_cnt} =~ ^[0-9]+$ ]]; then
  printError "${test_fspath_cnt} is not a number"
fi
TEST_FSPATH_COUNT=${test_fspath_cnt}

# If not specified, CLDPROVIDER it will be empty and build
# will not include neither AWS nor GCP. As long as CLDPROVIDER
# is not equal to `aws` nor `gcp` it will be assumed to be empty.
CLDPROVIDER=""

echo Select Cloud Provider:
echo  1: Amazon Cloud
echo  2: Google Cloud
echo  3: None
echo Enter your choice:
read cld_provider
if [[ ${cld_provider} -eq 1 ]]; then
  CLDPROVIDER="aws"
elif [[ ${cld_provider} -eq 2 ]]; then
  CLDPROVIDER="gcp"
fi

if ! CLDPROVIDER=${CLDPROVIDER} make -C ${AISTORE_DIR} node; then
  printError "failed to compile 'aisnode' binary"
fi

mkdir -p $CONFDIR

# Not really used for local testing but to keep config.sh quiet
GRAPHITE_PORT=2003
GRAPHITE_SERVER="127.0.0.1"
CONFFILE_COLLECTD=$CONFDIR/collectd.conf
CONFFILE_STATSD=$CONFDIR/statsd.conf

#
# generate conf file(s) based on the settings/selections above
#
for (( c=START; c<=END; c++ )); do
  CONFDIR="$HOME/.ais$NEXT_TIER$c"
  INSTANCE=$c
  mkdir -p $CONFDIR
  CONFFILE="$CONFDIR/ais.json"
  LOGDIR="$LOGROOT/$c/log"
  source "${AISTORE_DIR}/ais/setup/config.sh"

  ((PORT++))
  ((PORT_INTRA_CONTROL++))
  ((PORT_INTRA_DATA++))
done

# run proxy and storage targets
CMD="${GOPATH}/bin/aisnode"
for (( c=START; c<=END; c++ )); do
  CONFDIR="$HOME/.ais${NEXT_TIER}$c"
  CONFFILE="$CONFDIR/ais.json"

  PROXY_PARAM="-config=${CONFFILE} -role=proxy -ntargets=${TARGET_CNT} $1 $2"
  TARGET_PARAM="-config=${CONFFILE} -role=target $1 $2"

  if [[ $c -eq 0 ]]; then
    export AIS_PRIMARYPROXY="true"
    runCmd "${CMD} ${PROXY_PARAM}"
    unset AIS_PRIMARYPROXY

    # Wait for the proxy to start up
    sleep 2
  elif [[ $c -lt ${PROXY_CNT} ]]; then
    runCmd "${CMD} ${PROXY_PARAM}"
  else
    runCmd "${CMD} ${TARGET_PARAM}"
  fi
done

if [[ $AUTHENABLED = "true" ]]; then
  # conf file for authn
  CONFDIR="$HOME/.authn"
  mkdir -p $CONFDIR
  CONFFILE="$CONFDIR/authn.json"
  LOGDIR="$LOGROOT/authn/log"
  source "${AISTORE_DIR}/ais/setup/authn.sh"

  if ! make -C ${AISTORE_DIR} authn; then
    printError "failed to compile 'authn' binary"
  fi
  runCmd "${GOPATH}/bin/authn -config=${CONFFILE}"
fi

sleep 0.1

echo "Done."
