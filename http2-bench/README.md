# dfcpub/http2-bench
## Setup
### Dependencies
- nghttp2 (see http://www.nghttp2.org )

### Setup
Install the required dependency, and then the benchmarks can be run. If you are using a distribution with apt-get, `setup_nghttp.sh` will do all required setup.

## Benchmarks
Before running any benchmark, ensure that you are running a DFC proxy with a single target. Currently, the nghttp2 benchmarks only support retrieving from one target.

Each benchmark has four arguments:
- U: The URL of the target.
- B: The bucket to get files from.
- N: The number of files to get.
- C: The number of concurrent clients to use (unavailable for `server_push_http2_bench`)

### Commands

- `./server_push_http2_bench U B N` uses http2 with server push to get N files.
- `./no_server_push_http2_bench U B N C` uses http2 to get N files, with C clients.
- `./http1_bench U B N C` uses http1.1 to get N files, with C clients.

#### Example Usage
```bash
./server_push_http2_bench localhost:8081 local_benchmark_bucket 100
./no_server_push_http2_bench localhost:8081 local_benchmark_bucket 100 5
./http1_bench localhost:8081 local_benchmark_bucket 100 5
```
### Helpers

[put_files.go](./put_files.go) is a helper for the benchmarks - it puts a given number of files with a given size into the DFC cache. It has 4 command-line parameters:

- `-url`: the URL of the DFC proxy (default: http://localhost:8080)
- `-files`: the number of files to put (default: 10)
- `-filesize`: the size of files to put, in MB (default: 1)
- `-bucket`: the name of the bucket to put them in (default: local_benchmark_bucket)
- `-workers`: The number of workers to use to put files (default: 10)

The script "create_local_bucket" creates a local bucket for test use. It takes two command like parameters:

- P: The URL of the proxy
- B: The local bucket to create

#### Example Usage
```bash
./create_local_bucket localhost:8080 local_benchmark_bucket
go run put_files.go -files 80 -filesize 50 -bucket local_benchmark_bucket -workers 30
```
## Limitations
- Read chunk size cannot be modified
- Redirects are not followed by nghttp2, so the benchmark must be pointed directly to one target.
- server_push_http2_bench uses only one concurrent client.
