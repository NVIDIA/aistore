#!/bin/bash
METRICS_ENABLED=$(kubectl get deploy,svc -n kube-system | grep -q metrics-server; echo $?)
if [[ $METRICS_ENABLED == 0 ]]
then 
  # See https://github.com/prometheus-operator/kube-prometheus/
  minikube addons disable metrics-server
  tmpdir=$(mktemp -d)
  pushd $tmpdir
  git clone https://github.com/prometheus-operator/kube-prometheus.git

  pushd kube-prometheus
  # NOTE: Taken from https://github.com/prometheus-operator/kube-prometheus#quickstart.
  # Create the namespace and CRDs, and then wait for them to be available before creating the remaining resources.
  kubectl apply --server-side -f manifests/setup
  kubectl wait \
  	--for condition=Established \
  	--all CustomResourceDefinition \
  	--namespace=monitoring
  kubectl apply -f manifests/
  popd
  rm -rf $tmpdir
fi