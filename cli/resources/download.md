## Downloader

[AIS Downloader](/downloader/README.md) supports following types of download requests:

* **begin** - download the object(s) from external source
* **status** - display status of a given download job
* **cancel** - cancel given download job
* **rm** - remove finished download job from the download list
* **ls** - list current download jobs and their states

## Command List

### begin

`ais download begin <source> <dest>`

Downloads the object(s) from `source` location and saves it as specified in `dest` location.
`source` location can be link to single or range download:
* `gs://lpr-vision/imagenet/imagenet_train-000000.tgz`
* `gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz`

Currently, the schemas supported for `source` location are:
* `gs://` - refers to Google Cloud Storage, eg. `gs://bucket/sub_folder/object_name.tar`
* `s3://` - refers to Amazon Web Services S3 storage, eg. `s3://bucket/sub_folder/object_name.tar`
* `http://` or `https://` - refers to external link somewhere on the web, eg. `http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso`

As for `dest` location, the only supported schema is `ais://` and the link should be constructed as follows: `ais://bucket/sub_folder/object_name.tar`, where:
* `ais://` - schema, specifying that the destination is AIS cluster
* `bucket` - bucket name where the object(s) will be stored
* `sub_folder/object_name.tar` - in case of downloading a single file, this will be the name of the object saved in AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--description, -desc` | string | description for the download request | `""` |
| `--timeout` | string | timeout for request to external resource | `""` |

Examples:
* `ais download begin http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/ubuntu-18.04.1.iso` downloads object `ubuntu-18.04.1-desktop-amd64.iso` from the specified HTTP location and saves it in `ubuntu` bucket, named as `ubuntu-18.04.1.iso`.  
The same result can be obtained with  `ais download begin http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/` - note the lack of object name in the destination.
* `ais download begin gs://lpr-vision/imagenet/imagenet_train-000000.tgz ais://local-lpr/imagenet_train-000000.tgz` downloads object `imagenet/imagenet_train-000000.tgz` from Google Cloud Storage from bucket `lpr-vision` and saves it in `local-lpr` bucket, named as `imagenet_train-000000.tgz`
* `ais download begin --description "imagenet" gs://lpr-vision/imagenet/imagenet_train-000000.tgz ais://local-lpr/imagenet_train-000000.tgz` downloads an object and sets `imagenet` as description for the job (can be useful when listing downloads)
* `ais download begin "gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz" ais://local-lpr/imagenet/` will download all objects in the range from `gs://lpr-vision/imagenet/imagenet_train-000000.tgz` to `gs://lpr-vision/imagenet/imagenet_train-000140.tgz` and save them in `local-lpr` bucket, inside `imagenet` subdirectory
* `ais download begin --desc "subset-imagenet" "gs://lpr-vision/imagenet/imagenet_train-{000022..000140..2}.tgz" ais://local-lpr` same as above while skipping every other object in the specified range


### cancel

`ais download cancel --id <value>`

Cancels download job given its id.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--id` | string | unique identifier of download job returned upon job creation | `""` |

Examples:
* `ais download cancel --id "5JjIuGemR"` cancels the download job

### rm

`ais download rm --id <value>`

Removes finished download job from the list given its id.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--id` | string | unique identifier of download job returned upon job creation | `""` |

Examples:
* `ais download rm --id "5JjIuGemR"` removes the download job

### status

`ais download status --id <value>`

Retrieves status of the download with provided `id` which is returned upon creation of every download job.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--id` | string | unique identifier of download job returned upon job creation | `""` |
| `--progress` | bool | if set, displays a progress bar that illustrates the progress of the download | `false` |
| `--refresh` | int | refreshing rate of the progress bar (in milliseconds), works only if `--progress` flag is set | `1000` |

Examples:
* `ais download status --id "5JjIuGemR"` returns the condensed status of the download
* `ais download status --id "5JjIuGemR" --progress --refresh 500` creates progress bars for each currently downloading file and refreshes them every `500` milliseconds

### ls

`ais download ls --regex <value>`

Lists downloads whose descriptions match given `regex`.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | string | regex for the description of download requests | `""` |

Examples:
* `ais download ls` lists all downloads
* `ais download ls --regex "^downloads-(.*)"` lists all downloads which description starts with `downloads-` prefix
