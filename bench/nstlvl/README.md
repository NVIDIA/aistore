This is a micro-benchmark intended to help find out the impact (or lack of thereof) of POSIX directory nesting on the performance. Specifically, on the performance of random reading of files stored in very large directories.

By way of background, in AIStore (where we currently use local filesystems to store objects) the depth of POSIX directory nesting can be optimized - in some cases, and to a certain degree. This benchmark is a small step to find out.

```console
# run with all defaults:
$ go test -v -bench=. -benchmem

# generate and random-read 300K 4K-size files:
$ go test -v -bench=. -size=4096 -num=300000

# generate 8K-size files under `/ais/mpath` and run for at least 1m (to increase the number of iterations)
$ go test -v -bench=. -size=8192 -dir=/ais/mpath -benchtime=1m

# print usage and exit:
$ go test -bench=. -usage
```
