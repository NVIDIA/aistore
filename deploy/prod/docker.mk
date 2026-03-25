# Utility file containing common makefile directives for various Makefiles that perform docker builds
#
# This must be included with e.g. `include ../../docker.mk` after modifications to the default variables below, but
#   before any targets that use the variables, e.g. BUILD_ARGS or BUILD_CONTEXT
#
# IMAGE_REPO must be uniquely defined in all makefiles that use this
# Other variables can simply be modified and overridden as needed

DOCKER       ?= docker
REGISTRY_URL ?= docker.io
IMAGE_TAG    ?= .must_set_in_environment
PLATFORMS    ?= linux/amd64,linux/arm64

BUILDER_IMAGE ?=
BASE_IMAGE    ?=
COMMIT_SHA    ?=

BUILD_CONTEXT ?= .
BUILD_ARGS    ?=

ifneq ("$(BUILDER_IMAGE)","")
    BUILD_ARGS += --build-arg "BUILDER_IMAGE=$(BUILDER_IMAGE)"
endif
ifneq ("$(BASE_IMAGE)","")
    BUILD_ARGS += --build-arg "BASE_IMAGE=$(BASE_IMAGE)"
endif
ifneq ("$(COMMIT_SHA)","")
    BUILD_ARGS += --label "org.opencontainers.image.revision=$(COMMIT_SHA)"
    BUILD_ARGS += --build-arg "version=$(COMMIT_SHA)"
endif

.PHONY: all build push docker-buildx-multiarch

all: build push

build:
	$(DOCKER) build \
        -f Dockerfile \
        -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
        $(BUILD_ARGS) \
        $(BUILD_CONTEXT)

push:
	$(DOCKER) push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)

docker-buildx-multiarch:
	docker buildx build \
        --platform $(PLATFORMS) \
        --push \
        -f Dockerfile \
        -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
        $(BUILD_ARGS) \
        $(BUILD_CONTEXT)