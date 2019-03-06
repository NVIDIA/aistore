#!/bin/bash
AISSRC="../../../../../ais/"
source config_defaults proxy
bash ${AISSRC}/setup/config.sh
source config_defaults target
bash ${AISSRC}/setup/config.sh
source config_defaults ne_proxy
bash ${AISSRC}/setup/config.sh

sed -i '1s/^/{{- define "target.config" -}}/' _ais_target.json
sed -i '1s/^/{{- define "proxy.config" -}}/' _ais_proxy.json
sed -i '1s/^/{{- define "ne_proxy.config" -}}/' _ais_ne_proxy.json
sed -i '1s/^/{{- define "collectd.config" -}}/' _collectd.json
sed -i '1s/^/{{- define "statsd.config" -}}/' _statsd.json

files=$(find . -type f -name "_*.json" -maxdepth 1)
for i in $files; do echo "{{- end -}}" >> $i; done

cp *.json charts/templates/

