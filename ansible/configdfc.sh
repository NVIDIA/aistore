#!/bin/bash
set -e

export LOGDIR=/var/log/dfc
export CONFDIR=/dfc
export CONFFILE=$HOME/dfc.json
export LOGLEVEL=3
export CLDPROVIDER=aws
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
FSP=
for disk in "$@"; do
    if [ -z "$FSP" ]; then
	FSP='"/dfc/'$disk'": " "'
    else
        FSP=$FSP', "/dfc/'$disk'": " "'
    fi
done
echo FSPATHS are $FSP
#export FSPATHS='"/dfc/xvdb": " ", "/dfc/xvdc": " ", "/dfc/xvdd": " ", "/dfc/xvde": " "'
export FSPATHS=$FSP
export IPV4LIST=$(awk -vORS=, '{ print $1 }' ./inventory/cluster.txt | sed 's/,$//')
sudo rm -rf dfcproxy.json || true
sudo rm -rf dfc.json || true
source /etc/profile.d/dfcpaths.sh
$DFCSRC/setup/config.sh

