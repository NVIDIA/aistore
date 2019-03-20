apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ template "graphite.fullname" . }}-statsd-configmap
  labels:
    app.kubernetes.io/name: {{ include "graphite.name" . }}
    helm.sh/chart: {{ include "graphite.chart" . }}
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/managed-by: {{ .Release.Service }}
data:
{{- range $key, $value := .Values.statsdConfigMaps }}
  {{ $key }}: |-
{{ $value | indent 4 }}
{{- end }}
