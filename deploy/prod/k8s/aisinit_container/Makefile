#
# Usage:
#  $ env IMAGE_TAG="v4.2" make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/ais-init" IMAGE_TAG="v4.2" make all
#
#  Docker runtime required:
#  $ env IMAGE_REPO="aistorage/ais-init" IMAGE_TAG="4.2" make docker-buildx-multiarch
#

IMAGE_REPO    ?= aistorage/ais-init
BUILD_CONTEXT  = "../../../../."

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
		-t $(CI_REGISTRY_IMAGE)/ais-init:$(CI_COMMIT_SHORT_SHA) \
		$(BUILD_CONTEXT)
	buildah push $(CI_REGISTRY_IMAGE)/ais-init:$(CI_COMMIT_SHORT_SHA)