# AISCLI
> Under development

AISCLI is a command-line interface that allows users to interact with the AIS cluster. It currently supports `list`, `smap`, `config`, `stats` and `status` commands.

## How To Use
1. To get started using the AISCLI tool, run `go install`. 

2. Export the `AIS_URL` environment variable (eg. `http://<YOUR_CLUSTER_IP>:<PORT>`) to configure the CLI tool to point to the AIS cluster.
 ```sh
 $ export AIS_URL=http://localhost:8080
 $ aiscli --help
 ```
 Should return the list of commands for the CLI


## Supported Commands

Currently most of the commands are directly from the [RESTful API](../docs/http_api.md).

### Querying Information

The CLI allows for users to query information about the cluster or daemons.

#### config

`aiscli config [DAEMON_ID]`

Returns the configuration of `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the configuration of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

#### list

`aiscli list`

Lists all of the Daemons in the AIS cluster

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose, -v` | `bool` | verbose option | `false` |

#### smap

`aiscli smap [DAEMON_ID]`
Returns the cluster map (smap) of the `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the smap of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

#### stats

`aiscli stats [DAEMON_ID]`
Returns the stats of the `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the stats of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

#### status

`aiscli status [OPTION]`

Returns the status of the `OPTION`. `OPTION` is either `proxy`, `target`, or `DAEMON_ID`. If `OPTION` is not set, it will return the status all the daemons in the AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |


### Object Commands

The CLI allows for users to interact with objects in the AIS cluster.

#### get

`aiscli object get --bucket <value> --key <value>`

Gets the object from the bucket. If `--outfile` is empty, it stores the file in a locally cached version in the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to retrieve the object | `""` |
| `--key` | string | key of the object | `""` |
| `--outfile` | string | name of the file to store the contents of the object | `""` |
| `--bprovider` | [Provider](#enums) | locality of the bucket | `""` |
| `--offset` | string | read offset | `""` |
| `--length` | string | read length |  `""` |
| `--checksum` | bool | validate the checksum of the object | `false` |
| `--props` | bool | returns the properties of object (size and version). It does not download the object. | `false` |

    
#### put

`aiscli object put --bucket <value> --key <value> --body <value>`

Put an object into the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to put the object | `""` |
| `--key` | string | key of the object | `""` |
| `--body` | string | file that contains the contents of the object | `""` |
| `--bprovider` | [Provider](#enums) | locality of the bucket | `""` |

#### delete

`aiscli object delete --bucket <value> --key <value>`

Deletes an object from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that contains the object | `""` |
| `--key` | string | key of the object | `""` |
| `--bprovider` | [Provider](#enums) | locality of the bucket | `""` |
| `--list` | string | comma separated list of objects for list delete| `""` |
| `--range` | string | start and end interval (eg. 1:100) for range delete | `""` |
| `--prefix` | string | prefix for range delete | `""` |
| `--regex` | string | regex for range delete | `""` |
| `--deadline` | string | amount of time (Go Duration string) before the request expires | `0s` (no deadline) |
| `--wait` | bool | wait for operation to finish before returning response | `true` |

#### rename

`aiscli object rename --bucket <value> --key  <value> --newkey <value>`

Rename object from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that holds the object | `""` |
| `--key` | string | old name of object | `""` |
| `--newkey` | string | new name of object | `""` |


### Bucket Commands

The CLI allows for users to interact with buckets in the AIS cluster.

#### create

`aiscli bucket create --bucket <value>`

Creates a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to be created | `""` |


#### delete 

`aiscli bucket delete --bucket <value>`

Deletes a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to be deleted | `""` |


#### rename

`aiscli bucket rename --bucket <value> --newbucket <value> `

Renames a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | old name of the bucket | `""` |
| `--newbucket` | string | new name of the bucket | `""` |


#### names

`aiscli bucket names`

Returns the names of the buckets

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bprovider` | [Provider](#enums) | returns `local` or `cloud` buckets. If empty, returns all bucket names. | `""` |
| `--regex` | string | pattern for bucket matching | `""` |


#### list

`aiscli bucket list --bucket <value>`

Lists all the objects in the bucket

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--bprovider` | [Provider](#enums) | locality of bucket | `""` |
| `--props` | string | comma separated list of properties to return with object names | `size,version` |
| `--regex` | string | pattern for object matching | `""` |
| `--prefix` | string | prefix for object matching | `""` |
| `--pagesize` | string | maximum number of object names returned in response | `1000` (cloud), `65536` (local) |
| `--limit` | string | limit of object count | `0` (unlimited) |


### Enums

| Enum | Values | Description |
| --- | --- | --- |
| Provider | `local`, `cloud`, `''` | Locality of bucket. If empty, AIS automatically determines the locality. |