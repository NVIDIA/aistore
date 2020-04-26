#!/bin/bash

#
# Used to start up AIS, not actually used as container entrypoint since that
# is used to start a Jupyter instance.
#

if [[ $(id -u) -ne 0 ]]; then
  echo "This start script must be run as root - please use sudo!" > /dev/stderr
  exit 2
fi

export GOBIN=$GOPATH/bin
AISTORE_PATH=$GOPATH/src/github.com/NVIDIA/aistore

#
# Generate ais config file for CLI (for logins to the container, not needed otherwise)
#
RCFILE=$HOME/.config/ais/config.json
mkdir -p $(dirname $RCFILE)
cat >$RCFILE <<-EOM
  {
    "cluster": {
      "url": "http://127.0.0.1:51080",
      "default_ais_host": "http://127.0.0.1:51080",
      "default_docker_host": "http://127.0.0.1:51080"
    },
    "timeout": {
      "tcp_timeout": "60s",
      "http_timeout": "300s"
    }
  }
EOM

#
# Demo container creates a single directory under /ais and we don't pass volume mounts to it -
# so the data disk is ephemeral!
#
export FSPATHS=$(ls -d /ais/* | while read x; do echo -e "\"$x\": \"\""; done | paste -sd ",")

#
# Start a proxy or target node, using a pre-baked config under /etc/ais.
#
function start_node {
  CONFFILE=/etc/ais/$1/ais.json
  LOGDIR=/var/log/aisnode/$1
  mkdir -p ${LOGDIR}

  AIS_DAEMONID="$1-${HOSTNAME}" AIS_IS_PRIMARY="true" ${GOBIN}/aisnode \
    -config=${CONFFILE} \
    -role=$1 \
    -ntargets=1
}

start_node "proxy" 51080 &
sleep 2
start_node "target" 51081 &

echo "AIS cluster started"
sleep 5
/usr/bin/ais status

wait -n # Waits for first process to exit (hopefully it will never happen).
pkill -P $$ # If some process finished we must kill the remaining one, container will exit.