#
# Usage:
# $ CPU=8 MEMORY=16000 make -e try
#

CPU=4
MEMORY=7000
MODE="debug"
REGISTRY_URL ?= docker.io
IMAGE_REPO=aistore/aisnode-minikube
IMAGE_TAG=latest

# Run AIStore with 1 proxy 1 target on already running Minikube
minimal:
	@ ./minimal.sh

# Check if everything is installed correctly, start minikube and run minimal inside of it
try: stop
	@ minikube config set cpus $(CPU)
	@ minikube config set memory $(MEMORY)
	@ minikube start --driver=docker 
	@ ./minimal.sh

#  Gives you the ability to configure everything
dev:
	@ set -e
	@ ./utils/disable_vpn.sh && \
	  ./utils/deploy_minikube.sh && \
	  ./utils/minikube_registry.sh && \
	  ./utils/deploy_ais.sh

# This will skip the Minikube and Docker registry deployment
redeploy-ais:
	@ ./utils/disable_vpn.sh && \
	  ./utils/deploy_ais.sh

# Stops AIS running on Minikube
stop-ais:
	@ kubectl delete pod -l type=aisproxy
	@ kubectl delete pod -l type=aistarget

# Stops AIS cluster as well as Minikube
stop: 
	@ minikube delete

docker-build:
	@ docker build ./../../../ --force-rm -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) --build-arg MODE="$(MODE)" -f Dockerfile

docker-push:
	@ docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)

docker-all: docker-build docker-push
