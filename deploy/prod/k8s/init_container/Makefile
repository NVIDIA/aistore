#
# Usage:
#  $ env IMAGE_TAG=3.3 make -e all
#  $ env REGISTRY_URL=docker.io IMAGE_REPO=aistorage/ais-init IMAGE_TAG=4.0 make -e all
#

REGISTRY_URL ?= docker.io
IMAGE_REPO=aistorage/ais-init
IMAGE_TAG=.must_set_in_environment

all: build push

build:
	docker build --quiet -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile .

push:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)
