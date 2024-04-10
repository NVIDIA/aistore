REGISTRY_URL ?= docker.io
CI_IMAGE=aistorage/ci
IMAGE_TAG=latest

all: build push

build:
	docker build --network host --no-cache -t $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG) -f general.dockerfile .
push:
	docker push $(REGISTRY_URL)/$(CI_IMAGE):$(IMAGE_TAG)
