all: build push

build:
	docker build --no-cache -t aistore/ci:1.17 -f general.dockerfile .
	docker build --no-cache -t aistore/ci:4.0-hdfs -f hdfs.dockerfile .

push:
	docker push aistore/ci:1.17
	docker push aistore/ci:4.0-hdfs
