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
export TESTFSPATHCOUNT=0
export c=0
export FSPATHS='"/dfc/1": "", "/dfc/2": "", "/dfc/3": "", "/dfc/4": ""'
export IPV4LIST=$(awk -vORS=, '{ print $1 }' ./inventory/cluster.txt | sed 's/,$//')
$DFCSRC/setup/config.sh

