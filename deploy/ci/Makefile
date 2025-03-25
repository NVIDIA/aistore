DOCKER       ?= docker
REGISTRY_URL ?= docker.io
CI_IMAGE     ?= aistorage/gitlab-ci
IMAGE_TAG    ?= 1.0-podman
GO_VERSION   ?= 1.24.1
DOCKERFILE   ?= fedora.dockerfile

all: build push

build:
	$(DOCKER) build --network host --build-arg GO_VERSION=$(GO_VERSION) --no-cache -t $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG) -f $(DOCKERFILE) .
push:
	$(DOCKER) push $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG)
