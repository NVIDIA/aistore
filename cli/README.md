# AIS CLI
> Under development

## How To Use
1. To get started using the AIS CLI tool, run `deploy_cli.sh`. 

2. Export the `AIS_URL` environment variable (eg. `http://<YOUR_CLUSTER_IP>:<PORT>`) to configure the CLI tool to point to the AIS cluster.
 ```sh
 $ export AIS_URL=http://localhost:8080
 $ ais --help
 ```
 Should return the list of commands for the CLI


## Supported Commands

Currently most of the commands are directly from the [RESTful API](../docs/http_api.md).

### Querying Information

The CLI allows for users to query information about the cluster or daemons.

#### config

`ais config [DAEMON_ID]`

Returns the configuration of `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the configuration of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

#### list

`ais list`

Lists all of the Daemons in the AIS cluster

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose, -v` | `bool` | verbose option | `false` |

#### smap

`ais smap [DAEMON_ID]`
Returns the cluster map (smap) of the `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the smap of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

#### stats

`ais stats [DAEMON_ID]`
Returns the stats of the `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the stats of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

#### status

`ais status [OPTION]`

Returns the status of the `OPTION`. `OPTION` is either `proxy`, `target`, or `DAEMON_ID`. If `OPTION` is not set, it will return the status all the daemons in the AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |


#### setconfig

`ais setconfig [DAEMON_ID] [list of key=value]`

Set configurations for a specific daemon or the entire cluster via key-value pairs. To set configurations for the entire cluster, use `cluster` as the `DAEMON_ID`. For the list of available runtime configurations, see [here](../docs/configuration.md#runtime-configuration).

Example:

Setting configuration for entire cluster

 `ais setconfig cluster stats_time=10s`


### Object Commands

The CLI allows for users to interact with objects in the AIS cluster.

#### get

`ais object get --bucket <value> --key <value>`

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

`ais object put --bucket <value> --key <value> --body <value>`

Put an object into the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to put the object | `""` |
| `--key` | string | key of the object | `""` |
| `--body` | string | file that contains the contents of the object | `""` |
| `--bprovider` | [Provider](#enums) | locality of the bucket | `""` |

#### delete

`ais object delete --bucket <value> --key <value>`

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

`ais object rename --bucket <value> --key  <value> --newkey <value>`

Rename object from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that holds the object | `""` |
| `--key` | string | old name of object | `""` |
| `--newkey` | string | new name of object | `""` |


### Bucket Commands

The CLI allows for users to interact with buckets in the AIS cluster.

#### create

`ais bucket create --bucket <value>`

Creates a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to be created | `""` |


#### delete 

`ais bucket destroy --bucket <value>`

Destroys a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to be deleted | `""` |


#### rename

`ais bucket rename --bucket <value> --newbucket <value> `

Renames a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | old name of the bucket | `""` |
| `--newbucket` | string | new name of the bucket | `""` |


#### names

`ais bucket names`

Returns the names of the buckets.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bprovider` | [Provider](#enums) | returns `local` or `cloud` buckets. If empty, returns all bucket names. | `""` |
| `--regex` | string | pattern for bucket matching | `""` |


#### list

`ais bucket list --bucket <value>`

Lists all the objects along with some of the objects' properties. For the full list of properties, see [here](../docs/bucket.md#list-bucket).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--bprovider` | [Provider](#enums) | locality of bucket | `""` |
| `--props` | string | comma separated list of properties to return with object names | `name,size,version` |
| `--regex` | string | pattern for object matching | `""` |
| `--prefix` | string | prefix for object matching | `""` |
| `--pagesize` | string | maximum number of object names returned in response | `1000` (cloud), `65536` (local) |
| `--limit` | string | limit of object count | `0` (unlimited) |


#### setprops

`ais bucket setprops --bucket <value> [list of key=value]`

Sets bucket properties. For the available options, see [bucket-properties](../docs/bucket.md#properties-and-options).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--bprovider` | [Provider](#enums) | locality of bucket | `""` |
| `--json` | bool | use json as input | `false` |

Example: `ais bucket setprops --bucket mybucket 'mirror.enabled=true' 'mirror.copies=2'`

JSON equivalent example: `ais bucket setprops --bucket mybucket --json '{"mirror" : {"enabled": true, "copies" : 2}}'`


#### resetprops

`ais bucket resetprops --bucket <value>`

Reset bucket properties to cluster default.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--bprovider` | [Provider](#enums) | locality of bucket | `""` |


### Downloader

[AIS Downloader](../downloader/README.md) supports following types of download requests:

* **single** - download a single object
* **range** - download multiple objects based on a given naming pattern
* **status** - display status of a given download job
* **cancel** - cancel given download job
* **rm** - remove finished download job from the list
* **ls** - list current download jobs and their states


#### single

`ais download single --bucket <value> --link <value> --objname <value>`

Downloads a single object from `link` location and saves it as `objname` in provided `bucket`.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | bucket where the downloaded object is saved to | `""` |
| `--description` | string | description for the download request| `""` |
| `--link` | string | URL of where the object is downloaded from. | `""` |
| `--objname` | string | name of the object the download is saved as. If no objname is provided, the name will be the last element in the URL's path | `""` |

Examples:
* `ais download single --bucket local-lpr --link "https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000000.tgz" --objname "imagenet_train-000000.tgz"`
* `ais download single --bucket local-lpr --description "imagenet" --link "https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000000.tgz" --objname "imagenet_train-000000.tgz"` downloads an object and sets `imagenet` as description for the job (can be useful when listing downloads)

#### range

`ais download range --bucket <value> --base <value> --template <value>`

Downloads a list of objects depending on provided `template` from `base` and saves it to the provided `bucket`.
`base` and objects from `template` are concatenated with `/` so eg. for `base=https://storage.googleapis.com/lpr-vision` and `template=imagenet/imagenet_train-{000000..000140}.tgz` the resulting URL will be: `https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000000.tgz`, `https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000001.tgz`, ...

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | bucket where the downloaded object is saved to | `""` |
| `--description` | string | description for the download request| `""` |
| `--base` | string | base URL where the objects are located | `""` |
| `--template` | string | bash template describing names of the objects in the URL | `""` |

Examples:
* `ais download range --bucket local-lpr --base "https://storage.googleapis.com/lpr-vision" --template "imagenet/imagenet_train-{000000..000140}.tgz"` will download all objects in the range from `https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000000.tgz` to `https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000140.tgz`.
* `ais download range --bucket local-lpr --base "https://storage.googleapis.com/lpr-vision" --template "imagenet/imagenet_train-{000022..000140..2}.tgz"` same as above while skipping every other object in the specified range
#### status

`ais download status --id <value>`

Retrieves status of the download with provided `id` which is returned upon creation of every download job.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--id` | string | unique identifier of download job returned upon job creation | `""` |
| `--progress` | bool | if set, displays a progres bar that ilustrates the progress of the download | `false` |
| `--refresh` | int | refreshing rate of the progress bar (in miliseconds), works only if `--progress` flag is set | `1000` |

Examples:
* `ais download status --id "5JjIuGemR"` returns the condensed status of the download
* `ais download status --id "5JjIuGemR" --progress --refresh 500` creates progress bars for each currently downloading file and refreshes them every `500` milliseconds

#### cancel

`ais download cancel --id <value>`

Cancels download job given its id.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--id` | string | unique identifier of download job returned upon job creation | `""` |

Examples:
* `ais download cancel --id "5JjIuGemR"` cancels the download job

#### rm

`ais download rm --id <value>`

Remove finished download job from the list given its id.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--id` | string | unique identifier of download job returned upon job creation | `""` |

Examples:
* `ais download rm --id "5JjIuGemR"` removes the download job

#### ls

`ais download ls --regex <value>`

Lists downloads which descriptions match given `regex`.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | string | regex for the description of download requests | `""` |

Examples:
* `ais download ls` lists all downloads
* `ais download ls --regex "^downloads-(.*)"` lists all downloads which description starts with `downloads-` prefix

### Enums

| Enum | Values | Description |
| --- | --- | --- |
| Provider | `local`, `cloud`, `''` | Locality of bucket. If empty, AIS automatically determines the locality. |
