The CLI allows users to view and manage [AIS Downloader](/downloader/README.md) jobs.
It is used for downloading files and datasets from internet to AIS cluster.

## Start download job

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

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--description, --desc` | `string` | Description of the download job | `""` |
| `--timeout` | `string` | Timeout for request to external resource | `""` |

### Examples

#### Download single file

Download object `ubuntu-18.04.1-desktop-amd64.iso` from the specified HTTP location and saves it in `ubuntu` bucket, named as `ubuntu-18.04.1.iso`

```bash
$ ais create bucket ubuntu
ubuntu bucket created
$ ais start download http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/ubuntu-18.04.1.iso
cudIYMAqg
$ ais show download cudIYMAqg --progress
Files downloaded:              0/1 [---------------------------------------------------------]  0 %
ubuntu-18.04.1.iso 431.7MiB/1.8GiB [============>--------------------------------------------] 23 %
All files successfully downloaded.
$ ais ls ais://ubuntu
Name			Size	Version
ubuntu-18.04.1.iso	1.82GiB	1
```

#### Download range of files from GCP

Download all objects in the range from `gs://lpr-vision/imagenet/imagenet_train-000000.tgz` to `gs://lpr-vision/imagenet/imagenet_train-000140.tgz` and saves them in `local-lpr` bucket, inside `imagenet` subdirectory.

```bash
$ ais create local-lpr
local-lpr bucket created
$ ais start download "gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz" ais://local-lpr/imagenet/
QdwOYMAqg
$ ais show download QdwOYMAqg --progress --refresh 500ms
Files downloaded:                              0/141 [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000003.tgz 474.6MiB/945.6MiB [==============================>------------------------------] 50 %
imagenet/imagenet_train-000011.tgz 240.4MiB/946.4MiB [==============>----------------------------------------------] 25 %
imagenet/imagenet_train-000025.tgz   2.0MiB/946.3MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000013.tgz   1.0MiB/946.7MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000014.tgz 248.2MiB/945.7MiB [===============>---------------------------------------------] 26 %
imagenet/imagenet_train-000017.tgz 222.8KiB/946.1MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000024.tgz 468.8MiB/946.0MiB [=============================>-------------------------------] 50 %
imagenet/imagenet_train-000005.tgz 450.0MiB/945.8MiB [============================>--------------------------------] 48 %
imagenet/imagenet_train-000007.tgz 211.5MiB/946.2MiB [=============>-----------------------------------------------] 22 %
imagenet/imagenet_train-000008.tgz 814.8KiB/946.8MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000023.tgz  94.8KiB/946.9MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000000.tgz 194.4MiB/946.1MiB [============>------------------------------------------------] 21 %
imagenet/imagenet_train-000001.tgz 393.2MiB/945.8MiB [========================>------------------------------------] 42 %
imagenet/imagenet_train-000002.tgz   1.7MiB/946.7MiB [-------------------------------------------------------------]  0 %
```

#### Download range of files from another AIS cluster

Download all objects from another AIS cluster (`172.100.10.10:8080`), from bucket `imagenet` in the range from `imagenet_train-0022` to `imagenet_train-0140` and saves them on the local AIS cluster into `local-lpr` bucket, inside `set_1` subdirectory.

```bash
$ ais start download "ais://172.100.10.10:8080/imagenet/imagenet_train-{0022..0140}.tgz" ais://local-lpr/set_1/
QdwOYMAqg 
$ ais show download QdwOYMAqg --progress --refresh 500ms
Files downloaded:                     0/141 [-------------------------------------------------------------]  0 %
imagenet_train-000003.tgz 474.6MiB/945.6MiB [==============================>------------------------------] 50 %
imagenet_train-000011.tgz 240.4MiB/946.4MiB [==============>----------------------------------------------] 25 %
imagenet_train-000025.tgz   2.0MiB/946.3MiB [-------------------------------------------------------------]  0 %
imagenet_train-000013.tgz   1.0MiB/946.7MiB [-------------------------------------------------------------]  0 %
imagenet_train-000014.tgz 248.2MiB/945.7MiB [===============>---------------------------------------------] 26 %
imagenet_train-000017.tgz 222.8KiB/946.1MiB [-------------------------------------------------------------]  0 %
imagenet_train-000024.tgz 468.8MiB/946.0MiB [=============================>-------------------------------] 50 %
imagenet_train-000005.tgz 450.0MiB/945.8MiB [============================>--------------------------------] 48 %
imagenet_train-000007.tgz 211.5MiB/946.2MiB [=============>-----------------------------------------------] 22 %
imagenet_train-000008.tgz 814.8KiB/946.8MiB [-------------------------------------------------------------]  0 %
imagenet_train-000023.tgz  94.8KiB/946.9MiB [-------------------------------------------------------------]  0 %
imagenet_train-000000.tgz 194.4MiB/946.1MiB [============>------------------------------------------------] 21 %
imagenet_train-000001.tgz 393.2MiB/945.8MiB [========================>------------------------------------] 42 %
imagenet_train-000002.tgz   1.7MiB/946.7MiB [-------------------------------------------------------------]  0 %
```

## Stop download job

`ais stop download JOB_ID`

Stop download job with given `JOB_ID`.

## Remove download job

`ais rm download JOB_ID`

Remove the finished download job with given `JOB_ID` from the job list.

## Show download jobs and job status

`ais show download [JOB_ID]`

Show download jobs or status of a specific job.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Regex for the description of download jobs | `""` |
| `--progress` | `bool` | Displays progress bar | `false` |
| `--refresh` | `duration` | Refresh rate of the progress bar | `1s` |
| `--verbose` | `bool` | Verbose output | `false` |

### Examples

#### Show progress of given download job

Show progress bars for each currently downloading file with refresh rate of 500 ms.

```console
$ ais show download 5JjIuGemR --progress --refresh 500ms
Files downloaded:                              0/141 [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000003.tgz 474.6MiB/945.6MiB [==============================>------------------------------] 50 %
imagenet/imagenet_train-000011.tgz 240.4MiB/946.4MiB [==============>----------------------------------------------] 25 %
imagenet/imagenet_train-000025.tgz   2.0MiB/946.3MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000013.tgz   1.0MiB/946.7MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000014.tgz 248.2MiB/945.7MiB [===============>---------------------------------------------] 26 %
```

#### Show download job which description match given regex

Show all download jobs with descriptions starting with `download ` prefix. 

```console
$ ais show download --regex "^downloads (.*)"
JobID		 Status		 Errors	 Description
cudIYMAqg	 Finished	 0	 downloads whole imagenet bucket
fjwiIEMfa	 Finished	 0	 downloads range lpr-bucket from gcp://lpr-bucket
```

## Wait for download job

`ais wait download JOB_ID`

Wait for the download job with given `JOB_ID` to finish.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh rate | `1s` |
