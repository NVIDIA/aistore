#
# Usage:
#  $ env IMAGE_TAG="v3.1" make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/authn" IMAGE_TAG="v3.2" \
#        make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/authn" IMAGE_TAG="v3.2-beta" \
#        make all
#

DOCKER       ?= docker
REGISTRY_URL ?= docker.io
IMAGE_REPO   ?= aistorage/authn
IMAGE_TAG    ?= .must_set_in_environment

# Image that is used to build `authn` binary.
BUILDER_IMAGE   ?=
# Image that is used in final stage.
BASE_IMAGE      ?=

.PHONY: all
all: build push

.PHONY: build
build:
	$(DOCKER) build \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
		$(BUILD_ARGS) \
		"../../../../."

.PHONY: push
push:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)
