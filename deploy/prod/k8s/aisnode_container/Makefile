#
# Usage:
#  $ env IMAGE_TAG="3.3" make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/aisnode" IMAGE_TAG="4.0" \
#        AIS_BACKEND_PROVIDERS="aws" \
#        make all
#  $ env REGISTRY_URL="docker.io" IMAGE_REPO="aistorage/aisnode" IMAGE_TAG="4.0-devel" \
#        AIS_BACKEND_PROVIDERS="aws" BUILD_MODE="debug" BASE_IMAGE="debian:bookworm" \
#        make all
#

DOCKER       ?= docker
REGISTRY_URL ?= docker.io
IMAGE_REPO   ?= aistorage/aisnode
IMAGE_TAG    ?= .must_set_in_environment

# Image that is used to install necessary packages.
INSTALLER_IMAGE ?=
# Image that is used to build `aisnode` binary.
BUILDER_IMAGE   ?=
# Image that is used in final stage.
BASE_IMAGE      ?=
# By default we build image in "production" mode. It can be set to `debug`
# to enable extra checks.
BUILD_MODE      ?=

BUILD_ARGS = --build-arg "mode=$(BUILD_MODE)" --build-arg "providers=$(AIS_BACKEND_PROVIDERS)"
ifneq ("$(INSTALLER_IMAGE)","")
	BUILD_ARGS += --build-arg "INSTALLER_IMAGE=$(INSTALLER_IMAGE)"
endif
ifneq ("$(BUILDER_IMAGE)","")
	BUILD_ARGS += --build-arg "BUILDER_IMAGE=$(BUILDER_IMAGE)"
endif
ifneq ("$(BASE_IMAGE)","")
	BUILD_ARGS += --build-arg "BASE_IMAGE=$(BASE_IMAGE)"
endif


.PHONY: all all_debug
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


