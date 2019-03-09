#!/bin/bash

function whinge {
    echo "$@" >/dev/stderr
    exit 1
}

[[ -n "$STATSDCONF" ]] || whinge "STATSDCONF not set"
cp -fv $STATSDCONF /opt/statsd/statsd.conf || whinge "failed to copy $STATSDCONF"

[[ -n "$COLLECTDCONF" ]] || whinge "COLLECTDCONF not set"
cp -fv $COLLECTDCONF /etc/collectd/collectd.conf || whinge "failed to copy $COLLECTDCONF"

service collectd start
node /opt/statsd/stats.js /opt/statsd/statsd.conf &
sleep 2 # token effort to allow statsd to set up shop before aisloader tries to connect

[[ -n "$RUNSCRIPTSOURCE" ]] || whinge "RUNSCRIPTSOURCE not set"
source "$RUNSCRIPTSOURCE" || whinge "Shell source of $RUNSCRIPTSOURCE failed"

PATH=/go/bin:$PATH

[[ -n "$MY_NODE" ]] || whinge "MY_NODE not set"

#
# Send output to stdout so we can see it with kubectl logs, and
# duplicate to a file in /tmp.
#
# XXX TODO work out a means of preserving this output automatically
#
run_aisloader "$MY_NODE" 2>&1 | tee /tmp/aisloader.out

#
# If we simply exit we'll get restarted (as part of a daemonset). So hang around and
# await termination by external means.
#
while :
do
    echo "Sleeping awaiting termination"
    sleep 300
done