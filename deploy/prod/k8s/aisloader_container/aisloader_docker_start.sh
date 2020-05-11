#!/bin/bash

#
# Start script for aisloader container. Requires RUNSCRIPTSOURCE to be set in the
# environment; we'll source that, and expect it to provide a function named
# 'run_aisloader'.
#

function whinge {
    echo "$@" >/dev/stderr
    exit 1
}

[[ -n "$RUNSCRIPTSOURCE" ]] || whinge "RUNSCRIPTSOURCE not set"
[[ -n "$MY_NODE" ]] || whinge "MY_NODE not set"

if [[ -n "$STATSD_CONF_FILE" ]]; then
    cp -fv $STATSD_CONF_FILE /opt/statsd/statsd.conf || whinge "failed to copy $STATSD_CONF_FILE"
    node /opt/statsd/stats.js /opt/statsd/statsd.conf &
    sleep 2 # token effort to allow statsd to set up shop before aisloader tries to connect
fi

#
# Somewhere to dump anything of interest that will be picked up but the
# gather_log.sh script.
#
mkdir /var/log/aismisc

#
# Results directory - gather_logs.sh will collect this.
#
mkdir /var/log/aisloader

PATH=/:$PATH


#
# Send output to stdout so we can see it with kubectl logs, and
# duplicate to a file in /tmp.
#
stdbuf -o0 bash "$RUNSCRIPTSOURCE" "$MY_NODE" 2>&1 | tee /var/log/aisloader/aisloader.out

#
# Exit and recycle. If run_aisloader errors and returns too soon then we'll
# tickle k8s restart rate-limiting. Under normal circumstances we just return
# to a fresh aisloader node worker state.
#
exit $?