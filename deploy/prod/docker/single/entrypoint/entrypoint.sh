#!/bin/bash

export AIS_FS_PATHS=$(ls -d /ais/* | while read x; do echo -e "\"$x\": \"\""; done | paste -sd ",")

function start_node {
  # Required for `aisnode_config.sh`.
  export AIS_CONF_DIR=/etc/aisnode/$1
  export AIS_CONF_FILE=${AIS_CONF_DIR}/ais.json
  export AIS_LOCAL_CONF_FILE=${AIS_CONF_DIR}/ais_local.json

  export PORT=$2
  export AIS_PRIMARY_URL="http://$(hostname -i):51080"
  export AIS_LOG_DIR=/var/log/aisnode/$1
  mkdir -p ${AIS_CONF_DIR}
  mkdir -p ${AIS_LOG_DIR}

  source utils.sh
  source aisnode_config.sh

  bin/aisnode \
    -config=${AIS_CONF_FILE} \
    -local_config=${AIS_LOCAL_CONF_FILE} \
    -role=$1 \
    -ntargets=1 \
    &
}

start_node "proxy" 51080
sleep 5 # Give some time to breath for the primary.
start_node "target" 51081

wait -n # Waits for first process to exit (hopefully it will never happen).
pkill -P $$ # If some process finished we must kill the remaining one.
