#!/bin/bash -p

#
# Quick sample of how to install AIS on the k8s cluster
#

# helm dependency update
DRY=""
# DRY="--debug --dry-run"

	# --set target.resources.limits.cpu=20 \
	# --set ais_k8s.cluster_cidr="192.168.0.0/18" \
	# --set target.service.hostport=51081 \

helm install \
	--name=demo \
	--set image.aisnode.repository=quay.io/nvidia/aisnode \
	--set image.aisnode.tag=7 \
	--set image.pullPolicy=IfNotPresent \
	--set image.kubectl.repository=quay.io/nvidia/ais-kubectl \
	--set image.kubectl.tag=1 \
	--set image.pullSecretNames="{gmaltby-pull-secret}" \
	--set graphite.ais.pv.capacity=250Gi \
	--set graphite.ais.pv.path=/data/graphite \
	--set graphite.ais.pv.node=cpu01 \
	--set grafana.ais.pv.capacity=250Gi \
	--set grafana.ais.pv.path=/data/grafana \
	--set grafana.ais.pv.node=cpu01 \
	--set target.resources.requests.cpu=40 \
	--set target.resources.limits.cpu=44 \
	--set target.resources.requests.memory=80Gi \
	--set target.resources.limits.memory=120Gi \
	$DRY \
	charts/.
