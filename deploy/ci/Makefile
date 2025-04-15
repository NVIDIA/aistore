DOCKER          ?= docker
REGISTRY_URL    ?= docker.io
CI_IMAGE        ?= aistorage/gitlab-ci
IMAGE_TAG       ?= 1.0-podman
DOCKERFILE      ?= fedora.dockerfile
GO_BASE_VERSION ?= 1.24
GO_VERSION      ?= $(shell curl -s https://go.dev/dl/?mode=json | jq -r '[.[] | select(.version | startswith("go$(GO_BASE_VERSION)"))][0].version | ltrimstr("go")')

all: build push

build:
	$(DOCKER) build --network host --build-arg GO_VERSION=$(GO_VERSION) --no-cache -t $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG) -f $(DOCKERFILE) .
push:
	$(DOCKER) push $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG)
