{{- define "common.set_uuid_env" -}}
#!/bin/bash

#
# If k8s has extracted a suitable UUID from SMBIOS (not all SMBIOS are
# so helpful) then we hash that down to 8 characters and persist the
# hash in an agreed file for the container start script to query (for
# use a component of daemon id).
#
# This script run in an initContainer on proxy/ne-proxy/target pods.
# This script may run in parallel for a proxy and target on the same
# node, so keep in mind potential race conditions (e.g., we can't remove
# the file then recreate - it's the same file for both target and proxy;
# we also can't really validate the contents of an existing uuid hash
# file because our sibling may just be creating it).
#
# XXX Perhaps pass a disambiguating prefix to the initContainer and containers?
#

envfile="{{ .Values.proxy.envMountPath.podPath }}/uuid_env"

uuid=$(kubectl describe node $MY_NODE |  grep 'System UUID' | awk -F ' ' '{print $3}')

if [[ "$uuid" =~ ^[A-F0-9a-f]{8}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{12}$ ]]; then
   uuid_hash=$(echo $uuid | sha256sum | awk -F ' ' '{print $1}')
   echo ${uuid_hash:56} > $envfile
fi

{{end}}
