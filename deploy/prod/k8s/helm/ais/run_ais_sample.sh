#!/bin/bash -p

#
# Wrapper for helm install of AIS - alternative to repeating all these
# runes on the cmdline: copy and customize this script.
#

############# BEGIN: Review customization from this point to the marker below #############
#
# AIS cluster name
#
AIS_NAME=demo

#
# Container images - select aisnode version, the kubectl version rarely changes
#
AISNODE_IMAGE=quay.io/nvidia/aisnode:2.5
KUBECTL_IMAGE=quay.io/nvidia/ais-kubectl:1

#
# *If* the images require a pull secret, then install the pull secret in k8s
# and quote the secret name here(not the secret itself!). Leave as empty
# string for public repos.
#
PULLSECRETNAME=""

#
# Mountpaths in AIS target nodes for use by AIS (as hostPath volumes). You must specify this.
# Target nodes are controlled by node labeling.
#
#MOUNTPATHS='{/ais/sda,/ais/sdb,/ais/sdc,/ais/sdd,/ais/sde,/ais/sdf,/ais/sdg,/ais/sdh,/ais/sdi,/ais/sdj}'
#MOUNTPATHS='{/ais/sdb,/ais/sdc,/ais/sdd}'
MOUNTPATHS=""

#
# Grafana & Graphite storage - the chart will create hostName PVs for these.
# Grafana is small (just worksheets etc) so assume they're to come from the
# same node as subdirectories of the same tree.
#
STATS_NODENAME="cpu01"
STATS_BASEPATH="/data"
STATS_SIZE="250Gi"

#
# By default we dedicate AIS nodes to AIS and don't restrict it on CPU/mem - it doesn't
# need much except when performing distributed sorts. If you need to restrict CPU/mem
# resource then use the following, otherwise leave as empty strings.
#
CPU_REQUESTS=""			# eg, 40
CPU_LIMITS=""			# eg 44
MEM_REQUESTS=""			# eg 120Gi
MEM_LIMITS=""			# eg 140Gi

#
# External ingress to cluster - pass the cluster CIDR as used in Kubespray
# and the hostport number that will be opened on target nodes and redirected
# to target pods there. If not opening external ingress (ie access to external
# storage clients) then leave AIS_K8S_CLUSTER_CIDR empty.
#
AIS_K8S_CLUSTER_CIDR=""			# eg 192.168.0.0/18
AIS_TARGET_HOSTPORT=51081		# don't change unless really necessary
AIS_GATEWAY_EXTERNAL_IP=""		# must be in metalLB pool range if used

############# END: Review customization above this point #############

if [[ -z "$MOUNTPATHS" ]]; then
	echo "Please fill MOUNTPATHS" >&2
	exit 2
fi

#
# Check whether dependencies have been pulled; we could pull every time but
# it's nice to know when bits are being updated
#
if [[ ! -f "charts/requirements.lock" ]]; then
	echo "You need to run 'helm dependency update' to pull in dependencies" >&2
	exit 2
fi

helm install \
	--name=AIS_NAME \
	--set image.pullPolicy=IfNotPresent \
	--set image.aisnode.repository=$(echo $AISNODE_IMAGE | cut -d: -f1) \
	--set image.aisnode.tag=$(echo $AISNODE_IMAGE | cut -d: -f2) \
	--set image.kubectl.repository=$(echo $KUBECTL_IMAGE | cut -d: -f1) \
	--set image.kubectl.tag=$(echo $KUBECTL_IMAGE | cut -d: -f2) \
	"${PULLSECRETNAME:+--set image.pullSecretNames={$PULLSECRETNAME}}" \
	--set target.mountPaths="$MOUNTPATHS" \
	--set graphite.ais.pv.node=$STATS_NODENAME \
	--set graphite.ais.pv.path=${STATS_BASEPATH}/graphite \
	--set graphite.ais.pv.capacity=${STATS_SIZE} \
	--set grafana.ais.pv.node=$STATS_NODENAME \
	--set grafana.ais.pv.path=${STATS_BASEPATH}/grafana \
	--set grafana.ais.pv.capacity=${STATS_SIZE} \
	${CPU_REQUESTS:+--set target.resources.requests.cpu=${CPU_REQUESTS}} \
	${CPU_LIMITS:+--set target.resources.limits.cpu=${CPU_LIMIT}} \
	${MEM_REQUESTS:++--set target.resources.requests.memory=${MEM_REQUESTS}} \
	${MEM_LIMITS:++--set target.resources.limits.memory=${MEM_LIMITS}} \
	${AIS_K8S_CLUSTER_CIDR:++--set ais_k8s.cluster_cidr="${AIS_K8S_CLUSTER_CIDR}"} \
	${AIS_K8S_CLUSTER_CIDR:++--set target.service.hostport=${AIS_TARGET_HOSTPORT}} \
	${AIS_GATEWAY_EXTERNAL_IP:++--set ingress.gateway.externalIP=${AIS_GATEWAY_EXTERNAL_IP}} \
	charts/.
