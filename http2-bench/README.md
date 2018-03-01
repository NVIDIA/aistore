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
- C: The number of concurrent clients to use (unavailable for `bench_nghttp_push`)

### Commands

- `bench_nghttp_push U B N` uses http2 with server push to get N files.
- `bench_nghttp_nopush U B N C` uses http2 to get N files, with C clients.
- `bench_http1 U B N C` uses http1.1 to get N files, with C clients.

#### Example Usage
```bash
$ bench_nghttp_push localhost:8081 localbkt 100
$ bench_nghttp_nopush localhost:8081 localbkt 100 5
$ bench_http1 localhost:8081 localbkt 100 5
```
### Helpers

The go file "put_files" is a helper for the benchmarks - it puts a given number of files with a given size into the DFC cache. It has 4 command-line parameters:

- -files: the number of files to put
- -filesize: the size of files to put, in megabytes
- -bucket: the name of the bucket to put them in
- -workers: The number of workers to use to put files

The script "create_local_bucket" creates a local bucket for test use. It takes two command like parameters:

- P: The URL of the proxy
- B: The local bucket to create

#### Example Usage
```bash
$./create_local_bucket localhost:8080 localbkt
$go run put_files.go -files 80 -filesize 50 -bucket localbkt -workers 30
```
## Limitations
- Read chunk size cannot be modified
- Redirects are not followed by nghttp2, so the benchmark must be pointed directly to one target.
- bench_nghttp_push uses only one concurrent client.
