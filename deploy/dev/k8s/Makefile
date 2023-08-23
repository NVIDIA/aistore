# Usage:
# $ make try CPU=8 MEMORY=16000
# $ make dev CPU=12 MEMORY=24000

# Defaults
CPU ?= 4
MEMORY ?= 7000
MODE ?= "debug"
REGISTRY_URL ?= docker.io
LOCAL_REGISTRY_URL ?= localhost:5000
DOCKER_IMAGE = aistorage/aisnode-minikube
IMAGE_TAG = latest

# Run AIStore cluster consisting of 1 proxy/gateway and 1 target
# Note that minikube must be already running
minimal:
	@ ./minimal.sh

# Check if everything is installed correctly, start minikube and run minimal inside of it
try: stop
	@ minikube config set cpus $(CPU)
	@ minikube config set memory $(MEMORY)
	@ minikube start --driver=docker
	@ ./minimal.sh

# Configure and deploy everything
dev: 
	@ set -e
	@ ./utils/disable_vpn.sh && \
	  ./utils/deploy_minikube.sh && \
	  ./utils/deploy_ais.sh

# Redeploy AIStore without minikube and Docker registry deployment
redeploy-ais:
	@ ./utils/disable_vpn.sh && \
	  ./utils/deploy_ais.sh

# Set up minikube and deploy a minimal cluster with the new aisnode container including local changes
minimal-local: 
	@ export USE_LOCAL_REGISTRY=true && \
	  export DEPLOY_METRICS=false && \
	  ./utils/deploy_minikube.sh
	@ make redeploy-local

# Rebuild the local container and restart the minimal cluster without restarting minikube
redeploy-local:
	@ export REGISTRY_URL=${LOCAL_REGISTRY_URL} && \
	  ./utils/build_aisnode.sh && \
	  ./minimal.sh

# Stops AIS running on minikube
stop-ais:
	@ kubectl delete pod -l type=aisproxy
	@ kubectl delete pod -l type=aistarget

# Stops AIS cluster as well as minikube
stop:
	@ minikube delete

docker-build:
	@ docker build ./../../../ --force-rm -t $(REGISTRY_URL)/$(DOCKER_IMAGE):$(IMAGE_TAG) --build-arg MODE="$(MODE)" -f Dockerfile

docker-push:
	@ docker push $(REGISTRY_URL)/$(DOCKER_IMAGE):$(IMAGE_TAG)

docker-all: docker-build docker-push
