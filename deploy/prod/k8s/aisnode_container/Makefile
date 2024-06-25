#
# Usage:
#  $ env IMAGE_TAG="3.3" make all
#  $ env IMAGE_TAG="3.3" make all_debug
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/aisnode" IMAGE_TAG="4.0" AIS_BACKEND_PROVIDERS="aws" make all
#

DOCKER ?= docker
REGISTRY_URL ?= docker.io
IMAGE_REPO ?= aistorage/aisnode
IMAGE_TAG ?= .must_set_in_environment

.PHONY: all all_debug
all: build push
all_debug: build_debug push_debug

.PHONY: build build_debug
build:
	$(DOCKER) build \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
		--build-arg providers=$(AIS_BACKEND_PROVIDERS) \
		"../../../../."

build_debug:
	$(DOCKER) build \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)-debug \
		--build-arg mode="debug" --build-arg providers=$(AIS_BACKEND_PROVIDERS) \
		"../../../../."

.PHONY: push push_debug
push:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)

push_debug:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)-debug


