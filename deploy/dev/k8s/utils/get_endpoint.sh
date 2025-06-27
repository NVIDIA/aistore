#!/bin/bash

set -e

MANIFEST="$1"

PROXY_STATEFULSET=$(echo "$MANIFEST" | yq eval 'select(.kind == "StatefulSet" and .metadata.labels.type == "proxy")' -)
NAMESPACE=$(echo "$PROXY_STATEFULSET" | yq eval '.metadata.namespace' -)
PROXY_NAME=$(echo "$PROXY_STATEFULSET" | yq eval '.metadata.name' -)
PROTOCOL=$(echo "$PROXY_STATEFULSET" | yq eval '.spec.template.spec.containers[0].readinessProbe.httpGet.scheme' - | tr A-Z a-z)

PROXY_LB=$(echo "$MANIFEST" | yq eval 'select(.kind == "Service" and .spec.type == "LoadBalancer" and .metadata.labels.type == "proxy")' -)

if [ -n "$PROXY_LB" ]; then
  LB_NAME=$(echo "$PROXY_LB" | yq eval '.metadata.name' -)
  EXTERNAL_IP=$(kubectl get svc "$LB_NAME" -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
  EXTERNAL_PORT="8080"
else
  EXTERNAL_IP=$(kubectl get pod -n "$NAMESPACE" "${PROXY_NAME}-0" -o jsonpath='{.status.hostIP}')
  EXTERNAL_PORT=$(kubectl get pod -n "$NAMESPACE" "${PROXY_NAME}-0" -o jsonpath='{.spec.containers[0].ports[0].hostPort}')
fi

echo "${PROTOCOL}://$EXTERNAL_IP:$EXTERNAL_PORT"