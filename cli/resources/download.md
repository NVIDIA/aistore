---
layout: post
title: DOWNLOAD
permalink: cli/resources/download
redirect_from:
 - cli/resources/download.md/
---

## Downloader

The CLI allows users to view and manage [AIS Downloader](/aistore/downloader/README.md) jobs.
It is used for downloading files and datasets from internet to AIS cluster.

### Start

`ais start download SOURCE DESTINATION`

Download the object(s) from `SOURCE` location and saves it as specified in `DESTINATION` location.
`SOURCE` location can be a link to single or range download:
* `gs://lpr-vision/imagenet/imagenet_train-000000.tgz`
* `"gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz"`

Currently, the schemas supported for `SOURCE` location are:
* `gs://` - refers to Google Cloud Storage, eg. `gs://bucket/sub_folder/object_name.tar`
* `s3://` - refers to Amazon Web Services S3 storage, eg. `s3://bucket/sub_folder/object_name.tar`
* `ais://` - refers to AIS cluster. IP address and port number of the cluster's proxy should follow the protocol. If port number is omitted, "8080" is used. E.g, `ais://172.67.50.120:8080/bucket/imagenet_train-{0..100}.tgz`. Can be used to copy objects between buckets of the same cluster, or to download objects from any remote AIS cluster
* `http://` or `https://` - refers to external link somewhere on the web, eg. `http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso`

As for `DESTINATION` location, the only supported schema is `ais://` and the link should be constructed as follows: `ais://bucket/sub_folder/object_name`, where:
* `ais://` - schema, specifying that the destination is AIS cluster
* `bucket` - bucket name where the object(s) will be stored
* `sub_folder/object_name` - in case of downloading a single file, this will be the name of the object saved in AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--description, --desc` | `string` | Description of the download job | `""` |
| `--timeout` | `string` | Timeout for request to external resource | `""` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais start download http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/ubuntu-18.04.1.iso` | Downloads object `ubuntu-18.04.1-desktop-amd64.iso` from the specified HTTP location and saves it in `ubuntu` bucket, named as `ubuntu-18.04.1.iso` |
| `ais start download http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/` | Downloads object `ubuntu-18.04.1-desktop-amd64.iso` from the specified HTTP location and saves it in `ubuntu` bucket under the same name. Note the lack of object name in the destination. |
| `ais start download gs://lpr-vision/imagenet/imagenet_train-000000.tgz ais://local-lpr/imagenet_train-000000.tgz` | Downloads object `imagenet/imagenet_train-000000.tgz` from Google Cloud Storage from bucket `lpr-vision` and saves it in `local-lpr` bucket, named as `imagenet_train-000000.tgz` |
| `ais start download --description "imagenet" gs://lpr-vision/imagenet/imagenet_train-000000.tgz ais://local-lpr/imagenet_train-000000.tgz` | Downloads an object and sets `imagenet` as description for the job (can be useful when listing downloads) |
| `ais start download "gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz" ais://local-lpr/imagenet/` | Downloads all objects in the range from `gs://lpr-vision/imagenet/imagenet_train-000000.tgz` to `gs://lpr-vision/imagenet/imagenet_train-000140.tgz` and saves them in `local-lpr` bucket, inside `imagenet` subdirectory |
| `ais start download --desc "subset-imagenet" "gs://lpr-vision/imagenet/imagenet_train-{000000..000140..2}.tgz" ais://local-lpr` | Same as above, while skipping every other object in the specified range |
| `ais start download "ais://172.100.10.10:8080/imagenet/imagenet_train-{0022..0140}.tgz" ais://local-lpr/set_1/` | Downloads all objects from another AIS cluster (`172.100.10.10:8080`), from bucket `imagenet` in the range from `imagenet_train-0022` to `imagenet_train--0140` and saves them on the local AIS cluster into `local-lpr` bucket, inside `set_1` subdirectory |

### Stop

`ais stop download JOB_ID`

Stop download job with given `JOB_ID`.

#### Examples

| Command | Explanation |
| --- | --- |
| `ais stop download 5JjIuGemR` | Stops the download job with ID `5JjIuGemR` |

### Remove

`ais rm download JOB_ID`

Remove the finished download job with given `JOB_ID` from the job list.

#### Examples

| Command | Explanation |
| --- | --- |
| `ais download rm 5JjIuGemR` | Removes the download job with ID `5JjIuGemR` from the job list |

### Show jobs and job status

`ais show download [JOB_ID]`

Show download jobs or status of a specific job.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Regex for the description of download jobs | `""` |
| `--progress` | `bool` | Displays progress bar | `false` |
| `--refresh` | `string` | Refresh rate of the progress bar | `"1s"` |
| `--verbose` | `bool` | Verbose output | `false` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais show download` | Shows all download jobs |
| `ais show download --regex "^downloads-(.*)"` | Shows all download jobs with descriptions starting with `download-` prefix |
| `ais show download 5JjIuGemR` | Shows status of the download job with ID `5JjIuGemR` |
| `ais show download 5JjIuGemR --progress --refresh 500` | Shows progress bars for each currently downloading file with refresh rate of `500` milliseconds |

### Wait

`ais wait download JOB_ID`

Wait for the download job with given `JOB_ID` to finish.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh rate | `1s` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais wait download 5JjIuGemR` | Waits for the download job with ID `5JjIuGemR` to finish |
