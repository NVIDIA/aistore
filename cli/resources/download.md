## Downloader

[AIS Downloader](/downloader/README.md) supports following types of download requests:

* **start** - download the object(s) from external source
* **status** - display status of a given download job
* **abort** - abort given download job
* **rm** - remove finished download job from the download list
* **ls** - list current download jobs and their states

## Command List

### start

`ais download start SOURCE DESTINATION`

Downloads the object(s) from `SOURCE` location and saves it as specified in `DESTINATION` location.
`SOURCE` location can be link to single or range download:
* `gs://lpr-vision/imagenet/imagenet_train-000000.tgz`
* `gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz`

Currently, the schemas supported for `SOURCE` location are:
* `gs://` - refers to Google Cloud Storage, eg. `gs://bucket/sub_folder/object_name.tar`
* `s3://` - refers to Amazon Web Services S3 storage, eg. `s3://bucket/sub_folder/object_name.tar`
* `ais://` - refers to AIS cluster. IP address and port number of the cluster's proxy should follow the protocol. If port number is omitted, "8080" is used. E.g, `ais://172.67.50.120:8080/bucket/imagenet_train-{0..100}.tgz`. Can be used to copy objects between buckets of the same cluster, or to download objects from any remote AIS cluster
* `http://` or `https://` - refers to external link somewhere on the web, eg. `http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso`

As for `DESTINATION` location, the only supported schema is `ais://` and the link should be constructed as follows: `ais://bucket/sub_folder/object_name.tar`, where:
* `ais://` - schema, specifying that the destination is AIS cluster
* `bucket` - bucket name where the object(s) will be stored
* `sub_folder/object_name.tar` - in case of downloading a single file, this will be the name of the object saved in AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--description, -desc` | string | description for the download request | `""` |
| `--timeout` | string | timeout for request to external resource | `""` |
| `--provider` | [Provider](../README.md#enums) | locality of the destination bucket | `""` or [default](../README.md#bucket-provider) |

Examples:
* `ais download start http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/ubuntu-18.04.1.iso` downloads object `ubuntu-18.04.1-desktop-amd64.iso` from the specified HTTP location and saves it in `ubuntu` bucket, named as `ubuntu-18.04.1.iso`.
The same result can be obtained with  `ais download start http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/` - note the lack of object name in the destination.
* `ais download start gs://lpr-vision/imagenet/imagenet_train-000000.tgz ais://local-lpr/imagenet_train-000000.tgz` downloads object `imagenet/imagenet_train-000000.tgz` from Google Cloud Storage from bucket `lpr-vision` and saves it in `local-lpr` bucket, named as `imagenet_train-000000.tgz`
* `ais download start --description "imagenet" gs://lpr-vision/imagenet/imagenet_train-000000.tgz ais://local-lpr/imagenet_train-000000.tgz` downloads an object and sets `imagenet` as description for the job (can be useful when listing downloads)
* `ais download start "gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz" ais://local-lpr/imagenet/` will download all objects in the range from `gs://lpr-vision/imagenet/imagenet_train-000000.tgz` to `gs://lpr-vision/imagenet/imagenet_train-000140.tgz` and save them in `local-lpr` bucket, inside `imagenet` subdirectory
* `ais download start --desc "subset-imagenet" "gs://lpr-vision/imagenet/imagenet_train-{000022..000140..2}.tgz" ais://local-lpr` same as above while skipping every other object in the specified range
* `ais download start "ais://172.100.10.10:8080/imagenet/imagenet_train-{0022..0140}.tgz" ais://local-lpr/set_1/` downloads all objects of another AIS cluster `172.100.10.10:8080` from bucket `imagenet` in the range from `imagenet_train-0022` to `imagenet_train-140` and saves them on the local AIS cluster into `local-lpr` bucket, inside `set_1` subdirectory


### abort

`ais download abort ID`

Aborts download job given its `ID`.

Examples:
* `ais download abort 5JjIuGemR` aborts the download job

### rm

`ais download rm ID`

Removes finished download job from the list given its `ID`.

Examples:
* `ais download rm 5JjIuGemR` removes the download job

### status

`ais download status ID`

Retrieves status of the download with provided `ID` which is returned upon creation of every download job.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--progress` | bool | if set, displays a progress bar that illustrates the progress of the download | `false` |
| `--refresh` | int | refreshing rate of the progress bar (in milliseconds), works only if `--progress` flag is set | `1000` |

Examples:
* `ais download status 5JjIuGemR` returns the condensed status of the download
* `ais download status 5JjIuGemR --progress --refresh 500` creates progress bars for each currently downloading file and refreshes them every `500` milliseconds

### ls

`ais download ls --regex <value>`

Lists downloads whose descriptions match given `regex`.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | string | regex for the description of download requests | `""` |

Examples:
* `ais download ls` lists all downloads
* `ais download ls --regex "^downloads-(.*)"` lists all downloads which description starts with `downloads-` prefix
