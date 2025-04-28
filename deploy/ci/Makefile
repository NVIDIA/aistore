DOCKER          ?= docker
REGISTRY_URL    ?= docker.io
CI_IMAGE        ?= aistorage/gitlab-ci
IMAGE_TAG       ?= 1.3-podman
IMAGE_URL = $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG)

all: build push

build:
	DOCKER=$(DOCKER) IMAGE_URL=$(IMAGE_URL) ./build.sh
push:
	$(DOCKER) push $(IMAGE_URL)
