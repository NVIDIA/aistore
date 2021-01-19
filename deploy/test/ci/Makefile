all: build push

build:
	docker build --no-cache -t aistore/ci:1.15 -f general.dockerfile .
	docker build --no-cache -t aistore/ci:1.15-hdfs -f hdfs.dockerfile .

push:
	docker push aistore/ci:1.15
	docker push aistore/ci:1.15-hdfs
