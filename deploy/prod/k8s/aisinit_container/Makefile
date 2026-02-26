#
# Usage:
#  $ env IMAGE_TAG="v3.25" make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/ais-init" IMAGE_TAG="v4.2" \
#        make all
#  Docker runtime required:
#  $ env IMAGE_REPO="aistorage/aisnode" IMAGE_TAG="4.2" make docker-buildx-multiarch
#

DOCKER       ?= docker
REGISTRY_URL ?= docker.io
IMAGE_REPO   ?= aistorage/ais-init
IMAGE_TAG    ?= .must_set_in_environment
PLATFORMS    ?= linux/amd64,linux/arm64

# Image that is used to build `aisinit` binary.
BUILDER_IMAGE   ?=
# Image that is used in final stage.
BASE_IMAGE      ?=
# Commit SHA to add to the image for labeling.
COMMIT_SHA      ?=

# Build arguments
BUILD_CONTEXT = "../../../../."

BUILD_ARGS =
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
	docker buildx build \
		--platform $(PLATFORMS) \
		--push \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
		$(BUILD_ARGS) \
		$(BUILD_CONTEXT)