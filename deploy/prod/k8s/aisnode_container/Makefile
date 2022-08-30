#
# Usage:
#  $ env IMAGE_TAG=3.3 make -e all
#  $ env IMAGE_TAG=3.3 make -e all_debug
#  $ env IMAGE_REPO=docker.io/aistorage/admin IMAGE_TAG=4.0 make -e all
#

REGISTRY_URL ?= docker.io
IMAGE_REPO=aistorage/aisnode
IMAGE_TAG=.must_set_in_environment

DOCKER=docker

build:
	$(DOCKER) build --quiet \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) \
		--build-arg providers=$(AIS_BACKEND_PROVIDERS) \
		"../../../../."

build_debug:
	$(DOCKER) build --quiet \
		-f Dockerfile \
		-t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)-debug \
		--build-arg mode="debug" --build-arg providers=$(AIS_BACKEND_PROVIDERS) \
		"../../../../."

push:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)

push_debug:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)-debug

all: build push

all_debug: build_debug push_debug
