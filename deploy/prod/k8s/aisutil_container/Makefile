#
# Usage:
#  $ env IMAGE_TAG="v3.3" make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/ais-util" IMAGE_TAG="v4.0" make all
#

DOCKER ?= docker
REGISTRY_URL ?= docker.io
IMAGE_REPO ?= aistorage/ais-util
IMAGE_TAG ?= .must_set_in_environment

.PHONY: all
all: build push

.PHONY: build
build:
	$(DOCKER) build -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile .

.PHONY: push
push:
	$(DOCKER) push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)
