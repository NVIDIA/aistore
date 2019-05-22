#!/bin/bash

echo "---------------------------------------------------"
echo "aisnode $ROLE container startup at $(date)"
echo "---------------------------------------------------"

cp -fv $CONFFILE /etc/ais || exit 1 
cp -fv $STATSDCONF /opt/statsd/statsd.conf || exit 1

node /opt/statsd/stats.js /opt/statsd/statsd.conf &

#
# Somewhere to dump anything of interest that will be picked up but the
# gather_log.sh script.
#
mkdir /var/log/aismisc

#
# If this pod is part of the proxy daemonset then the initContainer in that
# daemonset spec has run to completion and checked whether the node we're
# hosted on is labelled as initial primary proxy. If it is, then the current
# pod is labelled as initial primary proxy (satisfying the selection criteria
# for the primary proxy headless service, so creating the DNS entry we
# require).
#
# It would be nice to consult metadata.labels in the Downward API to
# see if this pod has been labelled as described. Alas, the Downward API
# offers only the value from before the pod started and the initContainer ran.
# We also cannot use kubectl without granting this AIS pod more privs than
# is suitable. So instead we continue to rely on a file passed in a mount
# path.
#
if [[ -f /var/ais_env/env ]]; then
    echo "The initContainer detected running on node labelled as initial primary proxy"
    export AIS_PRIMARYPROXY=True
    is_primary=true
else
    # Caution - don't set AIS_PRIMARYPROXY false here, it just looks for any non-NULL value
    echo "The initContainer determined our node is not labelled as initial primary proxy"
    is_primary=false
fi

#
# An initcontainer runs to create a hash of the system uuid. If such a hash was found, we
# use it as the base of the AIS_DAEMONID
#
# If no uuid was found, we'll fallback to AIS_HOSTIP - the node IP provided in the pod
# environment.
#
# In both cases, we prefix the daemon id with the first letter of the ROLE - targets
# and proxies can run on the same node, so this disambiguates their daemon ids. This
# would fail if electable and non-electable proxies run on the same node - we could
# pass more detail on the role type in the environment if required.
#
if [[ -f /var/ais_env/uuid_env ]]; then
   UUID=$(cat /var/ais_env/uuid_env)
   export AIS_DAEMONID="${ROLE::1}$UUID"
else
   export AIS_DAEMONID="${ROLE::1}$AIS_HOSTIP"
fi
echo "Our ais daemon id will be $AIS_DAEMONID"

#
# During initial cluster deployment (helm install), there's no assured sequencing of the
# relative startup order of proxy & target pods on a node, not to mention across multiple nodes.
# So target pods can start before any proxy pods - and, even if all proxies started before targets,
# the proxies need some time to get their house in order. If a newly-minted target pod
# tries way too early then we might not yet have published the DNS entry for
# the ais-initial-primary-proxy service (that pod may still be starting, it may
# even still be in the process of downloading the appropriate container image) and
# registration attempts will short-circuit with name resolution failures.
# After a few retries AIS will give up, and that can be quite a short period
# in cases like that of name resolution failure. As a daemonset it will just be restarted,
# but if it happens too quickly k8s will apply a backoff time and can eventually
# place us in error state.
#
# A helm preinstall hook removes smap.json and helminstall.timestamp. We use the
# latter file to record the approximate time of initial cluster deployment - approximate
# because we only create it below and our execution may have been delayed by image download
# etc. Really, it is a measure of the first time this proxy or target container has
# ever started up up on this node since helm install.
#

tsf=/etc/ais/helminstall.timestamp
if [[ -f $tsf ]]; then
    install_age=$(( $(date +%s) - $(stat --format=%Y $tsf) ))
    early_startup=false
    [[ $install_age -lt 90 ]] && early_startup=true
    echo "Deployment age is ~${install_age}s, early_startup=$early_startup"
else
    touch $tsf
    early_startup=true
    echo "This is the first startup of this container for the current deployment"
    rm -f /etc/ais/*.agg
fi

#
# Decide the cases in which we must wait for the initial primary proxy to become pingable.
# 
# Once early startup is past, we never wait for the initial primary proxy - it's only
# required for initial startup and thereafter its absence must not be a barrier to nodes
# joining the cluster (some other proxy should have become primary).
#
# During early startup, all target and non-primary proxy nodes must wait for the initial primary
# to be resolvable and pingable.
#
must_wait=false
if $early_startup; then
    if [[ "$ROLE" == "target" ]]; then
        must_wait=true
    elif ! $is_primary; then
        must_wait=true
    fi
fi

#
# The following is informational. Since smap.json is removed by the preinstall hook, seeing
# an smap.json usually means this is an aisnode restart in an established cluster. But if
# an aisnode exits and we restart during early startup it may already have created an smap.json
# which will change the startup logic for the new instance - perhaps there's a case for
# removing the cached smap.json in the early start case, but we'll just log the existence
# or absence of the file.
#
if [[ -f /etc/ais/smap.json ]]; then
    echo "A cached smap.json is present"
else
    echo "No cached smap.json"
fi

total_wait=0
if $must_wait; then
    echo "Waiting for initial primary proxy to be resolvable and pingable"
    ping_result="failure"
    # k8s liveness will likely fail and restart us before the 120s period is over, anyway
    while [[ $total_wait -lt 120 ]]; do
        # Single success will end, otherwise wait at most 10s
        ping -c 1 -w 10 $PRIMARY_PROXY_SERVICE_HOSTNAME
        if [[ $? -eq 0 ]]; then
            ping_result="success"
            break
        fi

        if [[ $? -eq 1 ]]; then
            # could resolve but not ping, nearly there! the -w timeout means we
            # waited 10s during the ping.
            total_wait=$((total_wait + 10))
        else
            sleep 5                     # code 2 means resolve failure, treat any others the same
            total_wait=$((total_wait + 5))
        fi

        echo "Ping total wait time so far: $total_wait"
    done

    echo "Ping $ping_result; waited a total of around $total_wait seconds"

    #
    # Can resolve and ping, or gave up; regardless, introduce a brief snooze before proceeding
    # to give the initial primary a few moments to establish before further aisnode instances
    # start trying to register.
    #
    # XXX Should also consider the pingability of the proxy clusterIP service?
    #
    sleep 5
fi

# token effort to allow StatsD to set up shop before ais tries to connect
[[ $total_wait -le 2 ]] && sleep 2

ARGS="-config=/etc/ais/$(basename -- $CONFFILE) -role=$ROLE -ntargets=$TARGETS -alsologtostderr=true"

# See https://github.com/golang/go/issues/28466
export GODEBUG="madvdontneed=1"

while :
do
    if [[ -e /usr/local/bin/aisnode ]]; then
        # the production Dockerfile places ais here
        /usr/local/bin/aisnode $ARGS
    elif [[ -e /go/bin/aisnode ]]; then
        # debug/source image with a built binary, use that
        /go/bin/aisnode $ARGS
    elif [[ -d /go/src/github.com/NVIDIA/aistore/ais ]]; then
        # if running from source tree then add flags to assist the debugger
        (cd /go/src/github.com/NVIDIA/aistore/ais && go run -gcflags="all=-N -l" setup/aisnode.go $ARGS)
    else
        echo "Cannot find an ais binary or source tree"
    fi

    # Ye olde debug hack - create this in the hostmount to cause us to loop and restart on exit
    # XXX TODO remove me someday
    [[ -f "/etc/ais/debug_doloop" ]] || break
    echo "ais exited, restarting in loop per debug request in /etc/ais/debug_doloop"
    sleep 5 # slow any rapid-fail loops!

    # ... and use this to gate restart
    while [[ -f /etc/ais/debug_wait ]]; do
        echo "Waiting for /etc/ais/debug_wait to disappear"
        sleep 10
    done

done


#
# TODO: On non-zero return from ais preserve some logs into a hostPath mounted
# volume.
#

#
# If/when aisnode exits, aggregate aisnode logs in a persistent location so that we
# can see all logs across container restarts (kubectl logs only has the current and
# previous container instances available).
#
# XXX Needs some log rotation etc in time, just a quick fix for now. These files are
# removed on helm install - see earlier in this file.
#
cat /var/log/ais/aisnode.INFO >> /etc/ais/INFO.agg
cat /var/log/ais/aisnode.ERROR >> /etc/ais/ERROR.agg
cat /var/log/ais/aisnode.WARNING >> /etc/ais/WARNING.agg
