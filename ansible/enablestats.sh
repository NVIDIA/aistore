#!/bin/bash
set -e
sudo cp ~/statsd.json /opt/statsd/localConfig.js
sudo cp ~/collectd.json /etc/collectd/collectd.conf
cd /opt/statsd
nohup node ./stats.js ./localConfig.js &
sudo service collectd status
sudo service collectd restart
sudo service collectd status

