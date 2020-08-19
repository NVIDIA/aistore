#!/bin/bash

export AIS_CLD_PROVIDERS=$1
if ! [[ $AIS_CLD_PROVIDERS =~ ^(|aws|gcp|azure)$ ]]; then
  echo "Invalid cloud provider ('$AIS_CLD_PROVIDERS'), expected one of ('', 'aws', 'gcp', 'azure')"
  exit 1
fi

export AIS_FS_PATHS=$(ls -d /ais/* | while read x; do echo -e "\"$x\": \"\""; done | paste -sd ",")

function start_node {
  # Required for `aisnode_config.sh`.
  export AIS_CONF_DIR=/etc/aisnode/$1
  export AIS_CONF_FILE=${AIS_CONF_DIR}/ais.json
  export COLLECTD_CONF_FILE=${AIS_CONF_DIR}/collectd.conf
  export STATSD_CONF_FILE=${AIS_CONF_DIR}/statsd.conf

  export PORT=$2
  export AIS_PRIMARY_URL="http://172.17.0.2:51080"
  export AIS_LOG_DIR=/var/log/aisnode/$1
  mkdir -p ${AIS_CONF_DIR}
  mkdir -p ${AIS_LOG_DIR}

  source aisnode_config.sh

  AIS_DAEMON_ID="$1-${HOSTNAME}" AIS_IS_PRIMARY="true" bin/aisnode_${AIS_CLD_PROVIDERS} \
    -config=${AIS_CONF_FILE} \
    -role=$1 \
    -ntargets=1 \
    &
}

start_node "proxy" 51080
start_node "target" 51081

wait -n # Waits for first process to exit (hopefully it will never happen).
pkill -P $$ # If some process finished we must kill the remaining one.
