#!/bin/bash

source /var/ais_env/env
echo $AIS_PRIMARYPROXY
# Remove ais_env because this PRIMARY_PROXY value is only needed at initial deployment and is later managed via election
rm -f /var/ais_env/env

# Leave everything except the smap as /etc/ais is persistent to hold the bucket metadata - this happens anytime a new container is run
rm -f /etc/ais/smap.json
cp -fv $CONFFILE /etc/ais || exit 1 
cp -fv $STATSDCONF /opt/statsd/statsd.conf || exit 1
cp -fv $COLLECTDCONF /etc/collectd/collectd.conf || exit 1

service collectd start
node $STATSD_PATH/stats.js $STATSD_PATH/$STATSD_CONF&
/usr/local/bin/ais -config=/etc/ais/$(basename -- $CONFFILE) -role=$ROLE -ntargets=$TARGETS -alsologtostderr=true

