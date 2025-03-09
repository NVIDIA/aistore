DOCKER       ?= docker
REGISTRY_URL ?= docker.io
CI_IMAGE     ?= aistorage/ci
IMAGE_TAG    ?= latest
GO_VERSION   ?= 1.24

all: build push

build:
	$(DOCKER) build --network host --build-arg GO_VERSION=$(GO_VERSION) --no-cache -t $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG) -f general.dockerfile .
push:
	$(DOCKER) push $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG)
