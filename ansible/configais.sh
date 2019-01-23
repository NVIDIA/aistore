#!/bin/bash
set -e

export LOGDIR=/var/log/ais
export CONFDIR=/ais
export CONFFILE=$HOME/ais.json
export LOGLEVEL=3
export CLDPROVIDER=none
export PORT=8081
export PROXY=`cat ./inventory/proxy.txt`
export PROXYURL='http://'$PROXY':8081'
export CONFFILE_STATSD=$HOME/statsd.json
export CONFFILE_COLLECTD=$HOME/collectd.json
export GRAPHITE_SERVER=`cat ./inventory/graphana.txt`
export GRAPHITE_PORT=2003
export TESTFSPATHCOUNT=0
export c=0
export AUTHENABLED=false
export USE_HTTPS=false
export NON_ELECTABLE=false
export MIRROR_ENABLED=true
export IOSTAT_TIME=1s
export MIRROR_UTIL_THRESH=15
FSP=
for disk in "$@"; do
    if [ -z "$FSP" ]; then
	FSP='"/ais/'$disk'": " "'
    else
        FSP=$FSP', "/ais/'$disk'": " "'
    fi
done
echo FSPATHS are $FSP
#export FSPATHS='"/ais/xvdb": " ", "/ais/xvdc": " ", "/ais/xvdd": " ", "/ais/xvde": " "'
export FSPATHS=$FSP
export IPV4LIST=$(awk -vORS=, '{ print $1 }' ./inventory/cluster.txt | sed 's/,$//')
sudo rm -rf aisproxy.json || true
sudo rm -rf ais.json || true
source /etc/profile.d/aispaths.sh
$AISSRC/setup/config.sh

