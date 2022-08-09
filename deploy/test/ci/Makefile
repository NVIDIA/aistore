REGISTRY_URL ?= docker.io
CI_IMAGE=aistore/ci
HDFS_CI_IMAGE=aistore/hdfs-ci
IMAGE_TAG=latest

all: build push

build:
	docker build --no-cache -t $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG) -f general.dockerfile .
	docker build --no-cache -t $(REGISTRY_URL)/$(HDFS_CI_IMAGE):$(IMAGE_TAG) -f hdfs.dockerfile .

push:
	docker push $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG)
	docker push $(REGISTRY_URL)/$(HDFS_CI_IMAGE):$(IMAGE_TAG)
