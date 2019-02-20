{{- define "proxy.set_initial_primary_proxy_env" -}}
#!/bin/bash
# MY_NODE is a environment with downward api to get the node of a pod
if  kubectl get nodes --show-labels | grep {{ .Values.proxy.initialPrimaryProxyNodeLabel.name }} | grep -w $MY_NODE ; then
    echo "This is the node ($MY_NODE) with label {{ .Values.proxy.initialPrimaryProxyNodeLabel.name }}. Setup environment"
    echo "export AIS_PRIMARYPROXY=True" > {{ .Values.proxy.envMountPath.podPath }}/env
    cat {{ .Values.proxy.envMountPath.podPath }}/env
fi
{{end}}
