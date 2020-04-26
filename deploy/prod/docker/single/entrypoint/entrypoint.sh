#!/bin/bash

export CLDPROVIDER=$1
if ! [[ $CLDPROVIDER =~ ^(|aws|gcp)$ ]]; then
  echo "Invalid cloud provider ('$CLDPROVIDER'), expected one of ('', 'aws', 'gcp')"
  exit 1
fi

export FSPATHS=$(ls -d /ais/* | while read x; do echo -e "\"$x\": \"\""; done | paste -sd ",")

function start_node {
  # Required for `aisnode_config.sh`.
  export CONFDIR=/etc/aisnode/$1
  export CONFFILE=${CONFDIR}/ais.json
  export CONFFILE_COLLECTD=${CONFDIR}/collectd.conf
  export CONFFILE_STATSD=${CONFDIR}/statsd.conf
  
  export PORT=$2
  export PROXYURL="http://172.17.0.2:51080"
  export LOGDIR=/var/log/aisnode/$1
  mkdir -p ${CONFDIR}
  mkdir -p ${LOGDIR}
  
  source aisnode_config.sh

  AIS_DAEMONID="$1-${HOSTNAME}" AIS_IS_PRIMARY="true" bin/aisnode_${CLDPROVIDER} \
    -config=${CONFFILE} \
    -role=$1 \
    -ntargets=1 \
    &
}

start_node "proxy" 51080
start_node "target" 51081

wait -n # Waits for first process to exit (hopefully it will never happen).
pkill -P $$ # If some process finished we must kill the remaining one.
