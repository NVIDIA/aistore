DOCKER          ?= docker
REGISTRY_URL    ?= docker.io
IMAGE_REPO      ?= aistorage/gitlab-ci
IMAGE_TAG       ?= latest
IMAGE_URL = $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)

all: build push

build:
	DOCKER=$(DOCKER) IMAGE_URL=$(IMAGE_URL) ./build.sh
push:
	$(DOCKER) push $(IMAGE_URL)
