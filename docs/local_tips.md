### Tips

Assuming [local non-containerized deployment](/README.md#local-non-containerized), the following sequence will download up to 100 objects from the bucket called "myS3bucket" and then finds the corresponding cached objects locally, in the local and Cloud bucket directories:

```shell
$ cd $GOPATH/src/github.com/NVIDIA/aistore/ais/tests
$ BUCKET=myS3bucket go test -v -run=down
$ find /tmp/ais -type f | grep local
$ find /tmp/ais -type f | grep cloud
```

This, of course, assumes that all AIStore daemons are local and non-containerized (don't forget to run `make deploy` to make it happen) - and that the "test_fspaths" sections in their respective configurations point to the /tmp/ais.

To show all existing buckets, run:

```shell
$ cd $GOPATH/src/github.com/NVIDIA/aistore
$ BUCKET=x go test ./ais/tests -v -run=bucketnames
```

Note that the output will include both local and Cloud bucket names.

Further, to locate AIStore logs, run:

```shell
$ find $LOGDIR -type f | grep log
```

where $LOGDIR is the configured logging directory as per [AIStore configuration](/ais/setup/config.sh).

To terminate a running AIStore service and cleanup local caches, run:
```shell
$ make kill
$ make rmcache
```

Alternatively, run `make clean` to delete AIStore binaries and all the data that AIS had accumulated in your machine.
