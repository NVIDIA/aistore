#!/bin/bash

cp -fv $CONFFILE /etc/ais || exit 1 
cp -fv $STATSDCONF /opt/statsd/statsd.conf || exit 1
cp -fv $COLLECTDCONF /etc/collectd/collectd.conf || exit 1

service collectd start
node /opt/statsd/stats.js /opt/statsd/statsd.conf &

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

#an initcontainer runs to create a hash of the uuid which is to be used as the AIS_DAEMONID
#If that file isn't found, then the default set by the daemonset is used as the AIS_DAEMONID
if [[ -f /var/ais_env/uuid_env ]]; then
   UUID=$(cat /var/ais_env/uuid_env)
   export AIS_DAEMONID=$UUID
   echo "Found UUID hash to set as DaemonID: $UUID"
fi
#
# There's no assured sequencing of the relative startup of proxy & target pods
# on a node, not to mention across multiple nodes. So target pods can start before
# any proxy pods - and, even if all proxies started before targets, the proxies
# need some time to get their house in order. If a newly-minted target pod
# tries way too early then we might not yet have published the DNS entry for
# the ais-initial-primary-proxy service (that pod may still be starting, it may
# even still be in the process of downloading the appropriate container image) and
# registration attempts will short-circuit with name resolution failures.
# After a few retries AIS will give up, and that can be quite a short period
# in cases like that of name resolution failure. As a daemonset it will just be restarted,
# but if it happens too quickly k8s will apply a backoff time and can eventually
# place us in error state.
#
# So if we're a target pod we'll insert a brief snooze to allow proxies to
# get ahead of the game and to avoid triggering the backoff/error mechanism
# too lightly. To avoid delay for the case of a new target joining a long-
# established cluster, we'll use the presence of a cached smap to imply that
# this is not initial deployment time. We'll also perform the sleep *after*
# AIS exits non-zero, and before start we'll loop for a limited time awaiting
# resolvability of the primary proxy service.
#
# But wait! There's more. If we are a non-primary proxy starting up at initial cluster
# deployment time (no cached smap) then we'll also try to contact the initial primary
# proxy URL, and so require a resolvable DNS name. So we perform this dance for
# everyone *except* the initial primary proxy.
#
ping_result="failure"
total_wait=0
if [[  -f /etc/ais/smap.json ]]; then
    # A contactable/resolvable initial primary is expressly not a requirement after initial deployment!
    echo "Cached smap.json present - assuming not initial AIS cluster deployment"
elif ! $is_primary; then
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
        done

        echo "Ping $ping_result; waited a total of around $total_wait seconds"

        #
        # Can resolve and ping, or gave up; regardless, introduce a brief snooze before proceeding
        # in the case of initial cluster deployment. The intention here is to give the initial
        # primary a few moments to establish before ais starts trying to register.
        #
        # XXX Should also consider the pingability of the proxy clusterIP service.
        [[ -f /etc/ais/smap.json ]] || sleep 5
fi

# token effort to allow statsd to set up shop before ais tries to connect
[[ $total_wait -le 2 ]] && sleep 2

ARGS="-config=/etc/ais/$(basename -- $CONFFILE) -role=$ROLE -ntargets=$TARGETS -alsologtostderr=true"

while :
do
    if [[ -e /usr/local/bin/ais ]]; then
        # the production Dockerfile places ais here
        /usr/local/bin/ais $ARGS
    elif [[ -e /go/bin/ais ]]; then
        # debug/source image with a built binary, use that
        /go/bin/ais $ARGS
    elif [[ -d /go/src/github.com/NVIDIA/aistore/ais ]]; then
        (cd /go/src/github.com/NVIDIA/aistore/ais && go run -gcflags="all=-N -l" setup/ais.go $ARGS)
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


