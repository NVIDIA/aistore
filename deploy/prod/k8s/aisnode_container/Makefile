#
# Usage:
#  $ env IMAGE_TAG="3.3" make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/aisnode" IMAGE_TAG="4.2" \
#        AIS_BACKEND_PROVIDERS="aws" \
#        make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/aisnode" IMAGE_TAG="4.2" \
#        AIS_BACKEND_PROVIDERS="aws" BUILD_MODE="debug" BASE_IMAGE="debian:bookworm" \
#        make all
#  Docker runtime required:
#  $ env PLATFORMS=linux/arm64 REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/aisnode" IMAGE_TAG="4.2" \
#        AIS_BACKEND_PROVIDERS="aws" BUILD_MODE="debug" \
#        make docker-buildx-multiarch
#

DOCKER       ?= docker
REGISTRY_URL ?= docker.io
IMAGE_REPO   ?= aistorage/aisnode
IMAGE_TAG    ?= .must_set_in_environment
PLATFORMS    ?= linux/amd64,linux/arm64

# Image that is used to build `aisnode` binary.
BUILDER_IMAGE   ?=
# Image that is used in final stage.
BASE_IMAGE      ?=
# By default we build image in "production" mode. It can be set to `debug`
# to enable extra checks.
BUILD_MODE      ?=
# Build tags to build `aisnode` binary.
BUILD_TAGS      ?=
# Commit SHA to add to the image for labeling.
COMMIT_SHA      ?=
# Optional volume mount for cached go mod
GOMODCACHE ?= $(shell go env GOMODCACHE)

# Path to source root
BUILD_CONTEXT = "../../../../."
BUILD_ARGS = --build-arg "mode=$(BUILD_MODE)" --build-arg "providers=$(AIS_BACKEND_PROVIDERS)" --build-arg "tags=$(BUILD_TAGS)"
ifneq ("$(BUILDER_IMAGE)","")
	BUILD_ARGS += --build-arg "BUILDER_IMAGE=$(BUILDER_IMAGE)"
endif
ifneq ("$(BASE_IMAGE)","")
	BUILD_ARGS += --build-arg "BASE_IMAGE=$(BASE_IMAGE)"
endif
# Add commit SHA label if provided
ifneq ("$(COMMIT_SHA)","")
	BUILD_ARGS += --label "org.opencontainers.image.revision=$(COMMIT_SHA)"
endif


.PHONY: all
all: build push

.PHONY: build
build:
	$(DOCKER) build \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
		$(BUILD_ARGS) \
		$(BUILD_CONTEXT)

.PHONY: push
push:
	$(DOCKER) push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)

# Build and push for all target platforms
.PHONY: docker-buildx-multiarch
docker-buildx-multiarch:
	$(DOCKER) buildx build \
		--platform $(PLATFORMS) \
		--push \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
		$(BUILD_ARGS) \
		$(BUILD_CONTEXT)

.PHONY: ci
ci:
	buildah build \
		--volume $(GOMODCACHE):/go/pkg/mod:ro \
		-f Dockerfile \
		-t $(CI_REGISTRY_IMAGE)/aisnode:$(CI_COMMIT_SHORT_SHA) \
		--build-arg mode="debug" \
		--build-arg providers="gcp aws oci" \
		--build-arg tags="ci" \
		$(BUILD_CONTEXT)
	buildah push $(CI_REGISTRY_IMAGE)/aisnode:$(CI_COMMIT_SHORT_SHA)