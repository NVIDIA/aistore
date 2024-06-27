#!/bin/bash

echo "---------------------------------------------------"
echo "aisnode $AIS_NODE_ROLE container startup at $(date)"
echo "---------------------------------------------------"

[[ -f /git-showbranch.out ]] && cat /git-showbranch.out

cp -fv $AIS_CONF_FILE /etc/ais || exit 1
cp -fv $AIS_LOCAL_CONF_FILE /etc/ais || exit 1
cp -fv $STATSD_CONF_FILE /opt/statsd/statsd.conf || exit 1

# Use environment variables from /var/ais_env/env file
env_file=/var/ais_env/env
if [[ -f ${env_file} ]]; then
    source ${env_file}
fi

# Display cached .ais.smap file if it exists
if [[ -f /etc/ais/.ais.smap ]]; then
    cat <<-EOM
     --- BEGIN cached .ais.smap ---
     $(xmeta -x -in=/etc/ais/.ais.smap)
     --- END cached .ais.smap ---
EOM
else
    echo "No cached .ais.smap"
fi

# token effort to allow StatsD to set up shop before ais tries to connect
total_wait=0
[[ $total_wait -le 2 ]] && sleep 2

ARGS="-config=/etc/ais/$(basename -- $AIS_CONF_FILE) -local_config=/etc/ais/$(basename -- $AIS_LOCAL_CONF_FILE) -role=$AIS_NODE_ROLE"
if [[ "$AIS_NODE_ROLE" = "proxy" ]]; then
    ARGS+=" -ntargets=$TARGETS"
fi
echo "aisnode args: $ARGS"

while :
do
    if [[ -e /usr/bin/aisnode ]]; then
        # the production Dockerfile places ais here
        aisnode $ARGS
    else
        echo "Cannot find an ais binary or source tree"
        exit 2
    fi

    rc=$?   # exit code from aisnode

    # Logs will be present in `logsDir` directory of host

    # If the shutdown marker is present wait for the container to receive kill signal.
    # This is to ensure that the ais daemon scheduled to terminate isn't restarted by K8s.
    while [[ -f /var/ais_config/.ais.shutdown ]]; do
        echo "Waiting to receive kill signal"
        sleep 10
    done

    # Exit now if aisnode received SIGINT (see preStop lifecycle hook)
    [[ $rc -eq $((128 + 2)) ]] && exit 0

    # Ye olde debug hack - create this in the hostmount to cause us to
    # loop and restart on exit
    [[ -f "/etc/ais/debug_doloop" ]] || break
    echo "ais exited, restarting in loop per debug request in /etc/ais/debug_doloop"
    sleep 5 # slow any rapid-fail loops!

    # ... and use this to gate restart
    while [[ -f /etc/ais/debug_wait ]]; do
        echo "Waiting for /etc/ais/debug_wait to disappear"
        sleep 10
    done
done
