#!/bin/bash
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
export TESTFSPATHCOUNT=0
export c=0
export FSPATHS='"/dfc/1": "", "/dfc/2": "", "/dfc/3": "", "/dfc/4": "", "/dfc/5": "", "/dfc/6": ""'
export IPV4LIST=$(awk -vORS=, '{ print $1 }' ./inventory/cluster.txt | sed 's/,$//')
source /etc/profile.d/dfcpaths.sh
$DFCSRC/setup/config.sh

