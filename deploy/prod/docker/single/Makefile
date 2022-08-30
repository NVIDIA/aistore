#
# Usage:
#  $ IMAGE_TAG=latest make -e all
#  $ IMAGE_REPO=aistorage/cluster-minimal IMAGE_TAG=4.0 make -e all
#

REGISTRY_URL ?= docker.io
IMAGE_REPO=aistorage/cluster-minimal
IMAGE_TAG=.must_set_in_environment
AISTORE_PATH=$(shell git rev-parse --show-toplevel)

all: build push

build:
	cp ${AISTORE_PATH}/deploy/dev/local/aisnode_config.sh aisnode_config.sh
	cp ${AISTORE_PATH}/deploy/conf/limits.conf limits.conf

	docker build -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile .

	rm -rf aisnode_config.sh
	rm -rf limits.conf

push:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)
