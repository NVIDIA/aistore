#!/bin/bash
set -e

export AIS_LOG_DIR=/var/log/ais
export AIS_CONF_DIR=/ais
export AIS_CONF_FILE=$AIS_CONF_DIR/ais.json
export AIS_LOCAL_CONF_FILE=$AIS_CONF_DIR/ais_local.json
export STATSD_CONF_FILE=$AIS_CONF_DIR/statsd.json
export COLLECTD_CONF_FILE=$AIS_CONF_DIR/collectd.json

export PORT=8081
export PROXY=$(cat ./inventory/proxy.txt)
export AIS_PRIMARY_URL='http://'$PROXY':8081'
export GRAPHITE_SERVER=$(cat ./inventory/graphana.txt)

export IOSTAT_TIME_LONG=1s
export IOSTAT_TIME_SHORT=100ms
FSP=
for disk in "$@"; do
    if [ -z "$FSP" ]; then
	FSP='"/ais/'$disk'": {}'
    else
        FSP=$FSP', "/ais/'$disk'": {}'
    fi
done
echo AIS_FS_PATHS are $FSP
#export AIS_FS_PATHS='"/ais/xvdb": {}, "/ais/xvdc": {}, "/ais/xvdd": {}, "/ais/xvde": {}'
export AIS_FS_PATHS=$FSP
export HOSTNAME_LIST=$(awk -vORS=, '{ print $1 }' ./inventory/cluster.txt | sed 's/,$//')
sudo rm -rf aisproxy.json || true
sudo rm -rf ais.json || true
sudo rm -rf ais_local.json || true
source /etc/profile.d/aispaths.sh
$AISSRC/deploy/dev/local/aisnode_config.sh

