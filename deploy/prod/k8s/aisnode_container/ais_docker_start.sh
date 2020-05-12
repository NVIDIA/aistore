#!/bin/bash

echo "---------------------------------------------------"
echo "aisnode $AIS_NODE_ROLE container startup at $(date)"
echo "---------------------------------------------------"

[[ -f /git-showbranch.out ]] && cat /git-showbranch.out 

cp -fv $AIS_CONF_FILE /etc/ais || exit 1
cp -fv $STATSD_CONF_FILE /opt/statsd/statsd.conf || exit 1

node /opt/statsd/stats.js /opt/statsd/statsd.conf &

#
# Somewhere to dump anything of interest that will be picked up but the
# gather_log.sh script.
#
mkdir /var/log/aismisc

#
# If /var/ais_env/env exists then the initContainer that runs only on
# prody pods has determined:
#  - that we're in initial cluster deployment (proxy clusterIP can't return an smap)
#  - that the current node is labeled as initial primary proxy
# We'll pass a hint on to the aisnode instance that it is likely the (initial) primary.
#
if [[ "${AIS_NODE_ROLE}" != "target" && -f /var/ais_env/env ]]; then
    echo "Running on node labeled as initial primary proxy during initial cluster deployment"
    export AIS_IS_PRIMARY=True
    is_primary=true
else
    #
    # This path is taken for:
    #  - all target pods
    #  - proxy pods during initial deployment starting up on a node that is not labeled
    #    as initial primary proxy
    #  - all proxy pods once initial k8s deployment is done
    # Caution - don't set AIS_IS_PRIMARY false here, it just looks for any non-NULL value
    #
    is_primary=false
fi

#
# An initcontainer runs to create a hash of the system uuid. If such a hash was found, we
# use it as the base of the AIS_DAEMON_ID
#
# If no uuid was found, we'll fallback to AIS_HOST_IP - the node IP provided in the pod
# environment.
#
# In both cases, we prefix the daemon id with the first letter of the AIS_NODE_ROLE - targets
# and proxies can run on the same node, so this disambiguates their daemon ids. This
# would fail if electable and non-electable proxies run on the same node - we could
# pass more detail on the role type in the environment if required.
#
if [[ -f /var/ais_env/uuid_env ]]; then
   UUID=$(cat /var/ais_env/uuid_env)
   export AIS_DAEMON_ID="${AIS_NODE_ROLE::1}$UUID"
else
   export AIS_DAEMON_ID="${AIS_NODE_ROLE::1}$AIS_HOST_IP"
fi
echo "Our ais daemon id will be $AIS_DAEMON_ID"

#
# Informational
#
if [[ -f /etc/ais/.ais.smap ]]; then
    cat <<-EOM
     --- BEGIN cached .ais.smap ---
     $(usr/local/bin/xmeta -x -in=/etc/ais/.ais.smap)
     --- END cached .ais.smap ---
EOM
else
    echo "No cached .ais.smap"
fi

#
# During initial cluster deployment (helm install), there's no assured sequencing of the
# relative startup order of proxy & target pods on a node, not to mention across multiple nodes.
# So target pods can start before any proxy pods - and, even if all proxies started before targets,
# the proxies need some time to get their house in order. These times can also be stretched
# by such things as container image download.
# 
# Thus we require that all pods (except the initial primary proxy during initial cluster
# deployment) wait here until they can both:
#  - resolve and ping the proxy clusterIP service; this should always be possible since the
#    DNS entry is created by virtue of defining the service, and a clusterIP service is
#    pingable even if it has no endpoints
#  - retrieve an smap from the clusterIP service; for an established cluster this is a given
#    so does not delay rolling upgrade; during initial cluster deployment the initial primary
#    proxy will respond on the v1/daemon endpoint as soon as it is bootstrapped and ready for
#    registrations (note that it won't respond on v1/health until an initial startup period
#    has passed).
#
total_wait=0
if ! $is_primary; then
    echo "Waiting for proxy clusterIP service ($CLUSTERIP_PROXY_SERVICE_HOSTNAME) to be resolvable and pingable"
    ping_result="failure"
    
    #
    # Note that a clusterIP service is pingable *once created*, irrespective of whether it is
    # yet backed by any endpoints. So the following wait tends to be pretty short.
    #
    elapsed=0
    while [[ $elapsed -lt 60 ]]; do
        # Single success will end, otherwise wait at most 10s
        ping -c 1 -w 10 $CLUSTERIP_PROXY_SERVICE_HOSTNAME
        if [[ $? -eq 0 ]]; then
            ping_result="success"
            break
        fi

        if [[ $? -eq 1 ]]; then
            # could resolve but not ping, nearly there! the -w timeout means we
            # waited 10s during the ping.
            echo "ping timeout after 10s, can resolve but not ping; retrying"
            elapsed=$((elapsed + 10))
        else
            echo "ping timeout after 10s, cannot resolve or other failure; retry in 5s to a max of 60s"
            sleep 5                     # code 2 means resolve failure, treat any others the same
            elapsed=$((elapsed + 5))
        fi

        echo "Ping total wait time so far: $elapsed"
    done
    total_wait=$((total_wait + elapsed))

    echo "Ping $ping_result; waited a total of around $elapsed seconds"
    [[ $ping_result == "failure" ]] && exit 2

    #
    # Now that the proxy clusterIP has a DNS entry and is pingable, wait until we're able
    # to retrieve a valid smap via the service. During initial deployment all non-primary
    # proxies along with target pods will wait here until the initial primary proxy
    # starts responding; for restarts in an established cluster we should succeed
    # immediately (and if it doesn't there's no point in proceeding past this point, anyway).
    #
    proxy_ok=false
    d_url="http://${CLUSTERIP_PROXY_SERVICE_HOSTNAME}:${CLUSTERIP_PROXY_SERVICE_PORT}/v1/daemon?what=smap"
    echo "Waiting for a 200 result on ${d_url}"
    elapsed=0
    while [[ $elapsed -lt 60 ]]; do
        d_code=$(curl -X GET -o /dev/null --silent -w "%{http_code}" $d_url)
        if [[ "$d_code" == "200" ]]; then
            echo "   ... success after ${elapsed}s"
            proxy_ok=true
            break
        else
            echo "   ... failed (code=$d_code) at ${elapsed}s, trying for up to 60s"
            elapsed=$((elapsed + 1))
            sleep 1
        fi
    done

    total_wait=$((total_wait + elapsed))

    $proxy_ok || exit 3
fi

# token effort to allow StatsD to set up shop before ais tries to connect
[[ $total_wait -le 2 ]] && sleep 2

ARGS="-config=/etc/ais/$(basename -- $AIS_CONF_FILE) -role=$AIS_NODE_ROLE -alsologtostderr=true -stderrthreshold=1"

$is_primary && ARGS+=" -ntargets=$TARGETS"
echo "aisnode args: $ARGS"

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
    exit 2
    fi

    rc=$?   # exit code from aisnode

    #
    # If/when aisnode exits, aggregate aisnode logs in a persistent
    # location so that we can see all logs across container restarts
    # (kubectl logs only has the current and previous container
    # instances available).
    #
    # XXX Needs some log rotation etc in time, just a quick fix for
    # now.
    #
    cat /var/log/ais/aisnode.INFO >> /etc/ais/INFO.agg
    cat /var/log/ais/aisnode.ERROR >> /etc/ais/ERROR.agg
    cat /var/log/ais/aisnode.WARNING >> /etc/ais/WARNING.agg

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
