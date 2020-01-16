#!/bin/bash

export CLDPROVIDER=$1
if ! [[ $CLDPROVIDER =~ ^(|aws|gcp)$ ]]; then
  echo "Invalid cloud provider ('$CLDPROVIDER'), expected one of ('', 'aws', 'gcp')"
  exit 1
fi

AISTORE_PATH=$GOPATH/src/github.com/NVIDIA/aistore
cd ${AISTORE_PATH}
make node

export FSPATHS=$(ls -d /ais/* | while read x; do echo -e "\"$x\": \"\""; done | paste -sd ",")

function start_node {
  # Required for `ais/setup/config.sh`.
  export CONFDIR=/etc/aisnode/$1
  export CONFFILE=${CONFDIR}/ais.json
  export CONFFILE_COLLECTD=${CONFDIR}/collectd.conf
  export CONFFILE_STATSD=${CONFDIR}/statsd.conf
  
  export PORT=$2
  export PROXYURL="http://localhost:51080"
  export LOGDIR=/var/log/aisnode/$1
  mkdir -p ${CONFDIR}
  mkdir -p ${LOGDIR}
  
  source ais/setup/config.sh
  
  AIS_DAEMONID="$1-${HOSTNAME}" AIS_PRIMARYPROXY="true" ${GOBIN}/aisnode \
    -config=${CONFFILE} \
    -role=$1 \
    -ntargets=1 \
    &
}

start_node "proxy" 51080
start_node "target" 51081

wait -n # Waits for first process to exit (hopefully it will never happen).
pkill -P $$ # If some process finished we must kill the remaining one.
