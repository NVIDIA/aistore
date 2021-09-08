REGISTRY_URL ?= docker.io
IMAGE_REPO=aistore/ci

all: build push

build:
	docker build --no-cache -t $(REGISTRY_URL)/$(IMAGE_REPO):1.17 -f general.dockerfile .
	docker build --no-cache -t $(REGISTRY_URL)/$(IMAGE_REPO):4.0-hdfs -f hdfs.dockerfile .

push:
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):1.17
	docker push $(REGISTRY_URL)/$(IMAGE_REPO):4.0-hdfs
