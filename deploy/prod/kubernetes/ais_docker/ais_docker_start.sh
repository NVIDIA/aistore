#!/bin/bash

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
    #
    # On helm install, a pre-install hook cleans up /etc/ais/smap.json (which is only ever
    # created on proxy nodes). Thus an existing smap.json *should* imply this is a pod restart
    # in a past helm release. This is not ironclad - for example if we label a new node
    # some time after initial helm install and the node previously ran a proxy then the
    # old smap may still be present.
    #
    # A contactable/resolvable initial primary is expressly not a requirement after initial deployment!
    #
    echo "Past smap.json exists - assuming not initial AIS cluster deployment"
elif [[ "$ROLE" == "target" || $is_primary == "false" ]]; then
        echo "No past smap.json and this pod is a target or non-primary proxy - waiting for primary to be pingable"
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
    if [[ -e /usr/local/bin/aisnode ]]; then
        # the production Dockerfile places ais here
        /usr/local/bin/aisnode $ARGS
    elif [[ -e /go/bin/aisnode ]]; then
        # debug/source image with a built binary, use that
        /go/bin/aisnode $ARGS
    elif [[ -d /go/src/github.com/NVIDIA/aistore/ais ]]; then
        (cd /go/src/github.com/NVIDIA/aistore/ais && GODEBUG=madvdontneed=1 go run -gcflags="all=-N -l" setup/aisnode.go $ARGS)
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


