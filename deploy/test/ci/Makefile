all: build push

build:
	docker build --no-cache -t aistore/ci:1.16 -f general.dockerfile .
	docker build --no-cache -t aistore/ci:3.0-hdfs -f hdfs.dockerfile .

push:
	docker push aistore/ci:1.16
	docker push aistore/ci:3.0-hdfs
