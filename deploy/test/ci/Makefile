REGISTRY_URL ?= docker.io
IMAGE_REPO=aistore/ci
IMAGE_TAG=latest

all: build push

build:
	docker build --no-cache -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f general.dockerfile .
	docker build --no-cache -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f hdfs.dockerfile .

push:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)
