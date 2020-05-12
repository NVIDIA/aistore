#!/bin/bash

function whinge {
    echo "$@" >/dev/stderr
    exit 1
}

[[ -n "$STATSD_CONF_FILE" ]] || whinge "$STATSD_CONF_FILE not set"
cp -fv $STATSD_CONF_FILE /opt/statsd/statsd.conf || whinge "failed to copy $STATSD_CONF_FILE"

#
# Somewhere to dump anything of interest that will be picked up but the
# gather_log.sh script.
#
mkdir /var/log/aismisc

#
# Results directory - gather_logs.sh will collect this.
#
mkdir /var/log/aisloader

node /opt/statsd/stats.js /opt/statsd/statsd.conf &
sleep 2 # token effort to allow statsd to set up shop before aisloader tries to connect

[[ -n "$RUNSCRIPTSOURCE" ]] || whinge "RUNSCRIPTSOURCE not set"
source "$RUNSCRIPTSOURCE" || whinge "Shell source of $RUNSCRIPTSOURCE failed"

PATH=/:$PATH

[[ -n "$MY_NODE" ]] || whinge "MY_NODE not set"

#
# Send output to stdout so we can see it with kubectl logs, and
# duplicate to a file in /tmp.
#
run_aisloader "$MY_NODE" 2>&1 | tee /var/log/aisloader/aisloader.out

#
# If we simply exit we'll get restarted (as part of a daemonset). So hang around and
# await termination by external means.
#
while :
do
    echo "Sleeping awaiting termination"
    sleep 300
done