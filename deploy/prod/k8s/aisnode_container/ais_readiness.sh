#!/bin/bash

health_url="http://${CLUSTERIP_PROXY_SERVICE_HOSTNAME}:${CLUSTERIP_PROXY_SERVICE_PORT}/v1/health"
our_health_url="http://localhost:${CLUSTERIP_PROXY_SERVICE_PORT}/v1/health"

#
# If nothing answers with an smap on the clusterIP service then we're in early deployment
# of a new cluster. If we assert not ready on the initial primary then other nodes can't
# contact us on the clusterIP service and it just slows initial cluster establishment.
# So for this early bootstrap phase we always indicate "ready".
#
stat=$(curl -X GET -o /dev/null --max-time 5 --silent -w "%{http_code}" "${health_url}")
if [[ "${stat}" != "200" ]]; then
    # Looks like early deployment; make a special case for the initial primary
    [[ -f /var/ais_env/env ]] && exit 0
fi

# otherwise tell the truth for this pod
stat=$(curl -X GET -o /dev/null --max-time 5 --silent -w "%{http_code}" "${our_health_url}")
[[ "${stat}" == "200" ]] && exit 0
exit 1
