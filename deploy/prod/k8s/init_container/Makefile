#
# Usage:
#  $ env IMAGE_TAG=3.3 make -e all
#  $ env IMAGE_REPO=quay.io/aistore/admin IMAGE_TAG=4.0 make -e all
#

IMAGE_TAG=.must_set_in_environment
IMAGE_REPO=aistore/ais-init

all: build push

build:
	docker build --quiet -t $(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile .

push:
	docker push $(IMAGE_REPO):$(IMAGE_TAG)
