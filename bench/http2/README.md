## http2-bench

### Setup
1. Install [nghttp2](http://www.nghttp2.org). If you have apt-get installed, `./setup_nghttp.sh` will do all the required setup.
2. Start AIStore with any number of proxies and one target. Since nghttp2 does not follow redirects, the benchmarking scripts must point directly at a target.

### Usage

Each benchmark has four arguments:
- U: The URL of the target
- B: The bucket to get files from
- N: The number of files to get
- C: The number of concurrent clients to use (unavailable for `server_push_http2_bench`)

- `./server_push_http2_bench U B N` uses http2 with server push to get N files.
- `./no_server_push_http2_bench U B N C` uses http2 to get N files, with C clients.
- `./http1_bench U B N C` uses http1.1 to get N files, with C clients.

#### Examples

```console
$ ./server_push_http2_bench localhost:8081 local_benchmark_bucket 100
$ ./no_server_push_http2_bench localhost:8081 local_benchmark_bucket 100 5
$ ./http1_bench localhost:8081 local_benchmark_bucket 100 5
```

### Helpers
1. [put_files.go](put_files.go) puts a given number of files with a given size into AIStore. It has five command-line parameters:

- `-url`: the URL of the AIStore proxy (default: http://localhost:8080)
- `-files`: the number of files to put (default: 10)
- `-filesize`: the size of files to put, in KB (default: 1)
- `-bucket`: the name of the bucket to put them in (default: local_benchmark_bucket)
- `-workers`: The number of workers to use to put files (default: 10)

2. [create_local_bucket](create_local_bucket) creates an ais bucket in AIStore. It takes one parameter:

- B: The ais bucket to create

#### Examples

```console
$ ./create_local_bucket local_benchmark_bucket
$ go run put_files.go -files 80 -filesize 50 -bucket local_benchmark_bucket -workers 30
```

#### Limitations

- Read chunk size cannot be modified
- Redirects are not followed by nghttp2, so the benchmark must be pointed directly to one target
- [server_push_http2_bench](server_push_http2_bench) uses only one concurrent client
