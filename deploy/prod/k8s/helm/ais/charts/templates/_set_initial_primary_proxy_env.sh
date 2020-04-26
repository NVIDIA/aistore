{{- define "proxy.set_initial_primary_proxy_env" -}}
#!/bin/bash
#
# Run in an initContainer on proxy pods.  Waits until exactly one node is labeled
# to host the initial primary proxy, then for a pod on that labeled node sets up
# the environment for it to start as maybe-initial-primary proxy.

# MY_NODE and MY_POD are filled in proxy daemonset spec, using Downward API
#
# If no node is suitably labeled, this script will not complete and the
# and so, too, will the initContainer fail to complete.  A simple
# kubectl logs <podname> won't show anything - to view logs, use
# kubectl logs <podname> -c <init-container-name> - see the
# daemonset spec for the name of the init container.
#

filter="{{ .Values.proxy.initialPrimaryProxyNodeLabel.name }}={{ template "ais.fullname" . }}"

#
# XXX TODO should check the primary node is also labeled as a proxy node
#
function listmatchingnodes {
    kubectl get nodes --selector="$filter" -o="custom-columns=NAME:.metadata.name" | tail -n +2
}

while :
do
    n=$(listmatchingnodes | wc -l)
    [[ $n -eq 1 ]] && break

    echo "$n nodes labeled $filter, waiting for exactly 1"
    sleep 5
done

primary=$(listmatchingnodes)

envfile="{{ .Values.proxy.envMountPath.podPath }}/env"
rm -f $envfile

if [[ "$primary" == "$MY_NODE" ]]; then
    echo "initContainer complete - this pod is on the primary node $MY_NODE"

    #
    # Indicate to subsequent containers in this pod that we started on the primary node.
    #
    echo "export AIS_IS_PRIMARY=True" > $envfile
    
    #
    # Labeling the pod creates a DNS entry for the initial proxy service (by completing
    # the set of labels in the selector of the ais-initial-primary-proxy headless service).
    #
    kubectl label pod $MY_POD "$filter"
else
    echo "initContainer complete - not running on primary proxy node"
fi
{{end}}
