# Distributed Sort helpers (playground)

To make it easier to play and benchmark dsort feature, we made couple of scripts
which help to make it happen. We provide scripts for two languages: Golang and
Python3.

## Python 3

Before you start playing with scripts make sure that you have installed our
python client. This can be done by doing:
```shell
$ # Install python client
$ cd <path_to_repo>/dfcpub/python-client
$ pip3 install .

$ # Install playground helpers and scripts
$ cd <path_to_repo>/dfcpub/dsort/playground
$ pip3 install -e .
```

### put_tarballs.py

Thanks to this script you can distribute shards across the cluster.
User can set couple of general flags:

| Flag | Default value | Description |
|------|---------------|-------------|
| --ext | `.tar` | Extension for tarballs (either `.tar` or `.tgz`) |
| --bucket | `dsort-testing` | Bucket where shards will be put |
| --url | `http://localhost:8080` | Proxy url to which requests will be made |
| --shards | `20` | Number of shards to create |
| --iprefix | `shard-` | Prefix of input shard |
| --fsize | `1024` | Single file size (in bytes) inside the shard |
| --fcount | `20` | Number of files inside single shard |
| --cleanup | `false` | When true, the bucket will be deleted and created so all objects will be fresh |

Example:

```shell
(env) $ python3 put_tarballs.py --url http://proxyurl:9801 --shards 100 --fsize 1024 --fcount 100
```

This will create shards named: `INPUT_PREFIX+NUMBER+EXTENSION` like `shard-10.tar` with
given size and specified number.

### start_dsort.py

Starting dsort can be done via `curl` but this way it is a little bit harder to monitor
metrics for the run because user has to repeatedly `curl` them. This script aims
to automate this things. (Remember that this script does not allow to change
all possible settings, for more advanced usage please refer to API).

| Flag | Default value | Description |
|------|---------------|-------------|
| --ext | `.tar` | Extension for output shards (either `.tar`, `.tgz` or `.zip`) |
| --bucket | `dsort-testing` | Bucket where shards objects are stored |
| --url | `http://localhost:8080` | Proxy url to which requests will be made |
| --input | `shard-{0..10}` | Name template for input shard |
| --output | `new-shard-{0000..1000}` | Name template for output shard |
| --size | `1024*1024*10` | Size output of shard; note that the actual size will be rounded |
| --akind | `alphanumeric` | Kind of algorithm used to sort data |
| --adesc | `false` | Determines whether data should be sorted descending or ascending |
| --elimit | `20` | Limits number of concurrent shards extracted |
| --climit | `20` | Limits number of concurrent shards created |
| --mem | `60%` | Limits maximum of total memory until extraction starts spilling data to the disk, can be expressed in format: `60%` or `10GB` |
| --refresh | `5` | Metric refresh time (in seconds) |

Example:

```shell
(env) $ python3 -u start_dsort.py --url http://proxyurl:9801 --input "shard-{0..99}" --output "new-shard-{0..10000}" --outsize 10240 --elimit 40 --climit 20 --mem "80%" --refresh 2 | tee dsort.log
```

This will create new shards named: `OUTPUT_PREFIX+NUMBER+EXTENSION` like `new-shard-10.tar` with
specified size. Note that size will be rounded - if files are `1024` bytes and specified output
shard size is `2200` bytes the shard size will be `3*1024=3072`.


## Golang

### put_tarballs.go

Thanks to this script you can distribute shards across the cluster.
Script can operate in two modes: in-memory or disk (described below). User can
set couple of general flags:

| Flag | Default value | Description |
|------|---------------|-------------|
| -ext | `.tar` | Extension for tarballs (either `.tar` or `.tgz`) |
| -bucket | `dsort-testing` | Bucket where shards will be put |
| -url | `http://localhost:8080` | Proxy url to which requests will be made |
| -inmem | `true` | Determines if tarballs should be created on the fly in the memory or are will be read from the folder |
| -conc | `10` | Limits number of concurrent put requests (in-memory mode it also limits number of concurrent shards created) |
| -cleanup | `false` | When true the bucket will be deleted and created so all objects will be fresh |

#### Memory mode

Memory mode should be used when user wants to create shards on the fly and put
them on the cluster at the same time. This is really convenient when user wants
to test or benchmark functionality.

| Flag | Default value | Description |
|------|---------------|-------------|
| -iprefix | `shard-` | Prefix of input shard |
| -shards | `10` | Number of shards to create |
| -fsize | `1024*1024` | Single file size (in bytes) inside the shard |
| -fcount | `10` | Number of files inside single shard | 

Example:

```shell
$ go run put_tarballs.go -url http://proxyurl:9801 -conc 100 -inmem -shards 100 -fsize 1024 -fcount 100
```

This will create shards named: `INPUT_PREFIX+NUMBER+EXTENSION` like `shard-10.tar` with
given size and specified number.

#### Disk mode

Disk mode is for the users which already have shards created in specific folder and just want to put all
of them to the cluster. There is additional flag you have to set before using this mode.

| Flag | Default value | Description |
|------|---------------|-------------|
| -dir | `data` | Directory where the data will be created |

Remember that to use this mode you have to set in-memory flag to false `-inmem false`.

Example: 

```shell
$ go run put_tarballs.go -url http://proxyurl:9801 -conc 100 -inmem false -dir /tmp/data
```


### start_dsort.go

Starting dsort can be done via `curl` but this way it is a little bit harder to monitor
metrics for the run because user has to repeatedly `curl` them. This script aims
to automate this things. (Remember that this script does not allow to change
all possible settings, for more advanced usage please refer to API).

| Flag | Default value | Description |
|------|---------------|-------------|
| -ext | `.tar` | Extension for output shards (either `.tar`, `.tgz` or `.zip`)|
| -bucket | `dsort-testing` | Bucket where shards objects are stored |
| -url | `http://localhost:8080` | Proxy url to which requests will be made |
| -input | `shard-{0..10}` | Name template for input shard |
| -output | `new-shard-{0000..1000}` | Name template for output shard |
| -size | `1024*1024*10` | Size output of shard; note that the actual size will be rounded |
| -elimit | `20` | Limits number of concurrent shards extracted |
| -climit | `20` | Limits number of concurrent shards created |
| -mem | `60%` | Limits maximum of total memory until extraction starts spilling data to the disk, can be expressed in format: `60%` or `10GB` |
| -refresh | `5` | Metric refresh time (in seconds) |

Example:

```shell
$ go run start_dsort.go -url http://proxyurl:9801 -shards 100 -size 10240 -elimit 40 -climit 20 -mem 80 -refresh 2 | tee dsort.log
```

This will create new shards named: `OUTPUT_PREFIX+NUMBER+EXTENSION` like `new-shard-10.tar` with
specified size. Note that size will be rounded - if files are `1024` bytes and specified output
shard size is `2200` bytes the shard size will be `3*1024=3072`.
