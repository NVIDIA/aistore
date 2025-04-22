#!/bin/bash

set -e

LOCAL_DEVELOPMENT=${LOCAL_DEVELOPMENT:-false}

if [ "$LOCAL_DEVELOPMENT" = "true" ]; then
  make -C "../../../prod/k8s/aisnode_container" IMAGE_TAG=local-development build
  make -C "../../../prod/k8s/aisinit_container" IMAGE_TAG=local-development build
  if [ "$CLUSTER_TYPE" = "kind" ]; then
    kind load docker-image aistorage/aisnode:local-development
    kind load docker-image aistorage/ais-init:local-development
  elif [ "$CLUSTER_TYPE" = "minikube" ]; then
    minikube image load aistorage/aisnode:local-development
    minikube image load aistorage/ais-init:local-development
  fi
fi

patch_images_local_development() {
  yq '(. | select(.kind == "StatefulSet")).spec.template.spec.containers[] |= 
        (select(.name == "aisnode").image = "aistorage/aisnode:local-development") | 
      (. | select(.kind == "StatefulSet")).spec.template.spec.initContainers[] |= 
        (select(.name == "aisinit").image = "aistorage/ais-init:local-development")'
}

kubectl kustomize "$1" --load-restrictor LoadRestrictionsNone | kubectl apply -f -
kubectl kustomize "$2" --load-restrictor LoadRestrictionsNone | ( [ "$LOCAL_DEVELOPMENT" = "true" ] && patch_images_local_development || cat ) | kubectl apply -f -
sleep 5 && kubectl wait --for=condition=ready --timeout=2m pod/ais-proxy-0  # Give control plane time to create the pod before waiting
kubectl kustomize "$3" --load-restrictor LoadRestrictionsNone | ( [ "$LOCAL_DEVELOPMENT" = "true" ] && patch_images_local_development || cat ) | kubectl apply -f -
kubectl rollout status statefulset/ais-target --timeout=2m

source ./utils/export_endpoint.sh
echo -e "\nTo connect to the cluster: export AIS_ENDPOINT=\$AIS_ENDPOINT"