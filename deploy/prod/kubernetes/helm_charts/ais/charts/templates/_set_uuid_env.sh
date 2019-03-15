{{- define "common.set_uuid_env" -}}
#!/bin/bash

envfile="{{ .Values.proxy.envMountPath.podPath }}/uuid_env"
rm -f $envfile

uuid_hash=$(kubectl describe node $MY_NODE |  grep 'System UUID' | awk -F ' ' '{print $3}')

if [[ "$uuid" =~ ^{?[A-F0-9a-f]{8}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{12}}?$ ]]; then
   uuid_hash=$(echo $uuid | sham256sum | awk -F ' ' '{print $1}')
   echo ${uuid_hash:56} > $envfile
fi

{{end}}
