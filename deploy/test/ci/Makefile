all: build push

build:
	docker build --pull --no-cache -t aistore/ci:1.15 -f Dockerfile .

push:
	docker push aistore/ci:1.15
