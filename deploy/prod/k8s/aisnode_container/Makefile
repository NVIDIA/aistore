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

IMAGE_REPO    ?= aistorage/aisnode
BUILD_MODE    ?=
BUILD_TAGS    ?=
BUILD_CONTEXT  = "../../../../."

BUILD_ARGS = --build-arg "mode=$(BUILD_MODE)" \
             --build-arg "providers=$(AIS_BACKEND_PROVIDERS)" \
             --build-arg "tags=$(BUILD_TAGS)"

include ../../docker.mk

GOMODCACHE ?= $(shell go env GOMODCACHE)
VOLUME_ARGS =
ifneq ("$(GOMODCACHE)","")
	VOLUME_ARGS = --volume $(GOMODCACHE):/go/pkg/mod:rw
endif

.PHONY: ci
ci:
	buildah build \
        $(VOLUME_ARGS) \
        -f Dockerfile \
        -t $(CI_REGISTRY_IMAGE)/aisnode:$(CI_COMMIT_SHORT_SHA) \
        --build-arg mode="debug" \
        --build-arg providers="gcp aws oci" \
        --build-arg tags="ci" \
        $(BUILD_CONTEXT)
	buildah push $(CI_REGISTRY_IMAGE)/aisnode:$(CI_COMMIT_SHORT_SHA)