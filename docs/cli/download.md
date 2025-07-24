# Start, Stop, and monitor downloads

AIS Downloader is intended for downloading massive numbers of files (objects) and datasets from both Cloud Storage (buckets) and Internet.

Here's the command's help as of v3.30:

```console
$ ais download --help
NAME:
   ais download - (alias for "job start download") Download files and objects from remote sources, e.g.:
     - 'ais download http://example.com/file.tar ais://bucket/'                            - download from HTTP into AIS bucket;
     - 'ais download s3://bucket/file.tar ais://local-bucket/'                             - download from S3 into AIS bucket;
     - 'ais download gs://bucket/file.tar ais://local/ --progress'                         - download from GCS with progress monitoring;
     - 'ais download s3://bucket ais://local --sync'                                       - download and sync entire S3 bucket;
     - 'ais download "gs://bucket/file-{001..100}.tar" ais://local/'                       - download range of files using template;
     - 'ais download --hf-model bert-base-uncased --hf-file config.json ais://local/'      - download specific file from HuggingFace model;
     - 'ais download --hf-dataset squad --hf-file train-v1.1.json ais://local/ --hf-auth'  - download dataset file with authentication;
     - 'ais download --hf-model bert-large-uncased ais://local/ --blob-threshold 100MB'    - download model with size-based routing (large files get individual jobs).

USAGE:
   ais download SOURCE DESTINATION [command options]

OPTIONS:
   blob-threshold     Utilize built-in blob-downloader for remote objects greater than the specified (threshold) size
                      in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')
   description,desc   job description
   download-timeout   Server-side time limit for downloading a single file from remote source;
                      valid time units: ns, us (or µs), ms, s (default), m, h
   hf-auth            HuggingFace authentication token for private repositories
   hf-dataset         HuggingFace dataset repository name, e.g.:
                      --hf-dataset squad
                      --hf-dataset glue
                      --hf-dataset lhoestq/demo1
   hf-file            Specific file to download from HF repository (optional, downloads entire repository if not specified)
   hf-model           HuggingFace model repository name, e.g.:
                      --hf-model bert-base-uncased
                      --hf-model microsoft/DialoGPT-medium
                      --hf-model openai/whisper-large-v2
   hf-revision        HuggingFace repository revision/branch/tag (default: main)
   limit-bph          Maximum download speed, or more exactly: maximum download size per target (node) per hour, e.g.:
                      '--limit-bph 1GiB' (or same: '--limit-bph 1073741824');
                      the value is parsed in accordance with the '--units' (see '--units' for details);
                      omitting the flag or specifying '--limit-bph 0' means that download won't be throttled
   max-conns          Maximum number of connections each target can make concurrently (up to num mountpaths)
   object-list,from   Path to file containing JSON array of object names to download
   progress           Show progress bar(s) and progress of execution in real time
   progress-interval  Download progress interval for continuous monitoring;
                      valid time units: ns, us (or µs), ms, s (default), m, h
   sync               Fully synchronize in-cluster content of a given remote bucket with its (Cloud or remote AIS) source;
                      the option is, effectively, a stronger variant of the '--latest' (option):
                      in addition to bringing existing in-cluster objects in-sync with their respective out-of-band updates (if any)
                      it also entails removing in-cluster objects that are no longer present remotely;
                      like '--latest', this option provides operation-level control over synchronization
                      without requiring to change the corresponding bucket configuration: 'versioning.synchronize';
                      see also:
                        - 'ais show bucket BUCKET versioning'
                        - 'ais bucket props set BUCKET versioning'
                        - 'ais start rebalance'
                        - 'ais ls --check-versions'
   timeout            Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                      valid time units: ns, us (or µs), ms, s (default), m, h
   units              Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                      iec - IEC format, e.g.: KiB, MiB, GiB (default)
                      si  - SI (metric) format, e.g.: KB, MB, GB
                      raw - do not convert to (or from) human-readable format
   wait               Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   help, h            Show help
```

For details and background, please see the [downloader's own readme](/docs/downloader.md).

## Table of Contents
- [Start download job](#start-download-job)
- [Stop download job](#stop-download-job)
- [Remove download job](#remove-download-job)
- [Show download jobs and job status](#show-download-jobs-and-job-status)
- [Wait for download job](#wait-for-download-job)

## Start download job

`ais start download SOURCE DESTINATION`

or, same:

`ais start download SOURCE DESTINATION`

Download the object(s) from `SOURCE` location and saves it as specified in `DESTINATION` location.
`SOURCE` location can be a link to single or range download:
* `gs://lpr-vision/imagenet/imagenet_train-000000.tgz`
* `"gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz"`

Currently, the schemas supported for `SOURCE` location are:
* `ais://` - refers to AIS cluster. IP address and port number of the cluster's proxy should follow the protocol. If port number is omitted, "8080" is used. E.g, `ais://172.67.50.120:8080/bucket/imagenet_train-{0..100}.tgz`. Can be used to copy objects between buckets of the same cluster, or to download objects from any remote AIS cluster
* `aws://` or `s3://` - refers to Amazon Web Services S3 storage, eg. `s3://bucket/sub_folder/object_name.tar`
* `azure://` or `az://` - refers to Azure Blob Storage, eg. `az://bucket/sub_folder/object_name.tar`
* `gcp://` or `gs://` - refers to Google Cloud Storage, eg. `gs://bucket/sub_folder/object_name.tar`
* `http://` or `https://` - refers to external link somewhere on the web, eg. `http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso`

As for `DESTINATION` location should be in form `schema://bucket/sub_folder/object_name`:
* `schema://` - schema specifying the provider of the destination bucket (`ais://`, `aws://`, `azure://`, `gcp://`)
* `bucket` - bucket name where the object(s) will be stored
* `sub_folder/object_name` - in case of downloading a single file, this will be the name of the object saved in AIS cluster.

If the `DESTINATION` bucket doesn't exist, a new bucket with the default properties (as defined by the global configuration) will be automatically created.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--description, --desc` | `string` | Description of the download job | `""` |
| `--timeout` | `string` | Timeout for request to external resource | `""` |
| `--sync` | `bool` | Start a special kind of downloading job that synchronizes the contents of cached objects and remote objects in the cloud. In other words, in addition to downloading new objects from the cloud and updating versions of the existing objects, the sync option also entails the removal of objects that are not present (anymore) in the remote bucket | `false` |
| `--max-conns` | `int` | max number of connections each target can make concurrently (up to num mountpaths) | `0` (unlimited - at most #mountpaths connections) |
| `--limit-bph` | `string` | max downloaded size per target per hour | `""` (unlimited) |
| `--object-list,--from` | `string` | Path to file containing JSON array of strings with object names to download | `""` |
| `--progress` | `bool` | Show download progress for each job and wait until all files are downloaded | `false` |
| `--progress-interval` | `duration` | Progress interval for continuous monitoring. The usual unit suffixes are supported and include `s` (seconds) and `m` (minutes). Press `Ctrl+C` to stop. | `"10s"` |
| `--wait` | `bool` | Wait until all files are downloaded. No progress is displayed, only a brief summary after downloading finishes | `false` |

### Examples

#### Download single file

Download object `ubuntu-18.04.1-desktop-amd64.iso` from the specified HTTP location and saves it in `ubuntu` bucket, named as `ubuntu-18.04.1.iso`

```bash
$ ais create ubuntu
ubuntu bucket created

$ ais start download http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso ais://ubuntu/ubuntu-18.04.1.iso
cudIYMAqg
Run `ais show job download cudIYMAqg` to monitor the progress of downloading.

$ ais show job cudIYMAqg --progress
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
"local-lpr" bucket created
$ ais start download "gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz" ais://local-lpr/imagenet/
QdwOYMAqg
Run `ais show job download QdwOYMAqg` to monitor the progress of downloading.
$ ais show job download QdwOYMAqg
Download progress: 0/141 (0.00%)
$ ais show job download QdwOYMAqg --progress --refresh 500ms
Files downloaded:                              0/141 [--------------------------------------------------------------] 0 %
imagenet/imagenet_train-000006.tgz 192.7MiB/947.0MiB [============>-------------------------------------------------| 00:08:52 ]   1.4 MiB/s
imagenet/imagenet_train-000015.tgz 238.8MiB/946.3MiB [===============>----------------------------------------------| 00:05:42 ]   2.1 MiB/s
imagenet/imagenet_train-000022.tgz  31.2MiB/946.5MiB [=>------------------------------------------------------------| 00:24:35 ] 703.1 KiB/s
imagenet/imagenet_train-000043.tgz  38.5MiB/945.9MiB [==>-----------------------------------------------------------| 00:12:50 ]   1.1 MiB/s
imagenet/imagenet_train-000009.tgz  47.9MiB/946.9MiB [==>-----------------------------------------------------------| 00:23:36 ] 632.9 KiB/s
imagenet/imagenet_train-000013.tgz 181.9MiB/946.7MiB [===========>--------------------------------------------------| 00:15:40 ] 681.5 KiB/s
imagenet/imagenet_train-000014.tgz 215.3MiB/945.7MiB [=============>------------------------------------------------| 00:06:21 ]   1.6 MiB/s
imagenet/imagenet_train-000018.tgz  51.8MiB/945.9MiB [==>-----------------------------------------------------------| 00:22:05 ] 645.0 KiB/s
imagenet/imagenet_train-000000.tgz  36.6MiB/946.1MiB [=>------------------------------------------------------------| 00:30:02 ] 527.0 KiB/s
```

Errors may happen during the download.
Downloader logs and persists all errors, so they can be easily accessed during and after the run.

```console
$ ais show job download QdwOYMAqg
Download progress: 64/141 (45.39%)
Errors (10) occurred during the download. To see detailed info run `ais show job download QdwOYMAqg -v`
$ ais show job download QdwOYMAqg -v
Download progress: 64/141 (45.39%)
Progress of files that are currently being downloaded:
	imagenet/imagenet_train-000002.tgz: 16.39MiB/946.91MiB (1.73%)
	imagenet/imagenet_train-000023.tgz: 113.81MiB/946.35MiB (12.03%)
	...
Errors:
	imagenet/imagenet_train-000049.tgz: request failed with 404 status code (Not Found)
	imagenet/imagenet_train-000123.tgz: request failed with 404 status code (Not Found)
	...
```

The job details are also accessible after the job finishes (or when it has been aborted).

```console
$ ais show job download QdwOYMAqg
Done: 120 files downloaded, 21 errors
$ ais show job download QdwOYMAqg -v
Done: 120 files downloaded, 21 errors
Errors:
	imagenet/imagenet_train-000049.tgz: request failed with 404 status code (Not Found)
	imagenet/imagenet_train-000123.tgz: request failed with 404 status code (Not Found)
	...
```

#### Download range of files from GCP with limited connections

Download all objects in the range from `gs://lpr-vision/imagenet/imagenet_train-000000.tgz` to `gs://lpr-vision/imagenet/imagenet_train-000140.tgz` and saves them in `local-lpr` bucket, inside `imagenet` subdirectory.
Since each target can make only 1 concurrent connection we only see 4 files being downloaded (started on a cluster with 4 targets).

```bash
$ ais create local-lpr
local-lpr bucket created
$ ais start download "gs://lpr-vision/imagenet/imagenet_train-{000000..000140}.tgz" ais://local-lpr/imagenet/ --conns=1
QdwOYMAqg
$ ais show job download QdwOYMAqg --progress
Files downloaded:                              0/141 [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000003.tgz 474.6MiB/945.6MiB [==============================>------------------------------] 50 %
imagenet/imagenet_train-000011.tgz 240.4MiB/946.4MiB [==============>----------------------------------------------] 25 %
imagenet/imagenet_train-000025.tgz   2.0MiB/946.3MiB [-------------------------------------------------------------]  0 %
imagenet/imagenet_train-000013.tgz   1.0MiB/946.7MiB [-------------------------------------------------------------]  0 %
```

#### Download range of files from another AIS cluster

Download all objects from another AIS cluster (`172.100.10.10:8080`), from bucket `imagenet` in the range from `imagenet_train-0022` to `imagenet_train-0140` and saves them on the local AIS cluster into `local-lpr` bucket, inside `set_1` subdirectory.

```bash
$ ais start download "ais://172.100.10.10:8080/imagenet/imagenet_train-{0022..0140}.tgz" ais://local-lpr/set_1/
QdwOYMAqg
Run `ais show job download QdwOYMAqg` to monitor the progress of downloading.
$ ais show job download QdwOYMAqg --progress --refresh 500ms
Files downloaded:                     0/120 [--------------------------------------------------------------] 0 %
imagenet_train-000022.tgz  31.2MiB/946.5MiB [=>------------------------------------------------------------| 00:24:35 ] 703.1 KiB/s
imagenet_train-000043.tgz  38.5MiB/945.9MiB [==>-----------------------------------------------------------| 00:12:50 ]   1.1 MiB/s
imagenet_train-000093.tgz  47.9MiB/946.9MiB [==>-----------------------------------------------------------| 00:23:36 ] 632.9 KiB/s
imagenet_train-000040.tgz 181.9MiB/946.7MiB [===========>--------------------------------------------------| 00:15:40 ] 681.5 KiB/s
imagenet_train-000059.tgz 215.3MiB/945.7MiB [=============>------------------------------------------------| 00:06:21 ]   1.6 MiB/s
imagenet_train-000123.tgz  51.8MiB/945.9MiB [==>-----------------------------------------------------------| 00:22:05 ] 645.0 KiB/s
imagenet_train-000076.tgz  36.6MiB/946.1MiB [=>------------------------------------------------------------| 00:30:02 ] 527.0 KiB/s
```

#### Download whole GCP bucket

Download all objects contained in `gcp://lpr-vision` bucket and save them into the `lpr-vision-copy` AIS bucket.
Note that this feature is only available when `ais://lpr-vision-copy` is connected to backend cloud bucket `gcp://lpr-vision`.

```console
$ ais bucket props set ais://lpr-vision-copy backend_bck=gcp://lpr-vision
Bucket props successfully updated
"backend_bck.name" set to:"lpr-vision" (was:"")
"backend_bck.provider" set to:"gcp" (was:"")
$ ais start download gs://lpr-vision ais://lpr-vision-copy
QdwOYMAqg
Run `ais show job download QdwOYMAqg` to monitor the progress of downloading.
```

#### Sync whole GCP bucket

There are times when we suspect or know that the content of the cloud bucket that we previously downloaded has changed.
By default, the downloader just downloads new objects or updates the outdated ones, and it doesn't check if the cached objects are no present in the cloud.
To change this behavior, you can specify `--sync` flag to enforce downloader to remove cached objects which are no longer present in the cloud.

```console
$ ais ls --no-headers gcp://lpr-vision | wc -l
50
$ ais bucket props set ais://lpr-vision-copy backend_bck=gcp://lpr-vision
Bucket props successfully updated
"backend_bck.name" set to:"lpr-vision" (was:"")
"backend_bck.provider" set to:"gcp" (was:"")
$ ais ls --no-headers --cached ais://lpr-vision-copy | wc -l
0
$ ais start download gs://lpr-vision ais://lpr-vision-copy
QdwOYMAqg
Run `ais show job download QdwOYMAqg` to monitor the progress of downloading.
$ ais wait download QdwOYMAqg
$ ais ls --no-headers --cached ais://lpr-vision-copy | wc -l
50
$ # Remove some objects from `gcp://lpr-vision`
$ ais ls --no-headers gcp://lpr-vision | wc -l
40
$ ais ls --no-headers --cached ais://lpr-vision-copy | wc -l
50
$ ais start download --sync gs://lpr-vision ais://lpr-vision-copy
fjwiIEMfa
Run `ais show job download fjwiIEMfa` to monitor the progress of downloading.
$ ais wait download fjwiIEMfa
$ ais ls --no-headers --cached ais://lpr-vision-copy | wc -l
40
$ diff <(ais ls gcp://lpr-vision) <(ais ls --cached ais://lpr-vision-copy) | wc -l
0
```

> Job starting, stopping (i.e., aborting), and monitoring commands all have equivalent *shorter* versions. For instance `ais start download` can be expressed as `ais start download`, while `ais wait download Z8WkHxwIrr` is the same as `ais wait Z8WkHxwIrr`.

#### Download GCP bucket objects with prefix

Download objects contained in `gcp://lpr-vision` bucket which start with `dir/prefix-` and save them into the `lpr-vision-copy` AIS bucket.
Note that this feature is only available when `ais://lpr-vision-copy` is connected to backend cloud bucket `gcp://lpr-vision`.

```console
$ ais bucket props set ais://lpr-vision-copy backend_bck=gcp://lpr-vision
Bucket props successfully updated
"backend_bck.name" set to:"lpr-vision" (was:"")
"backend_bck.provider" set to:"gcp" (was:"")
$ ais start download gs://lpr-vision/dir/prefix- ais://lpr-vision-copy
QdwOYMAqg
Run `ais show job download QdwOYMAqg` to monitor the progress of downloading.
```

#### Download multiple objects from GCP

Download all objects contained in `objects.txt` file.
The source and each object name from the file are concatenated (with `/`) to get full link to the external object.

```bash
$ cat objects.txt
["imagenet/imagenet_train-000013.tgz", "imagenet/imagenet_train-000024.tgz"]
$ ais start download gs://lpr-vision ais://local-lpr --object-list=objects.txt
QdwOYMAqg
Run `ais show job download QdwOYMAqg` to monitor the progress of downloading.
$ # `gs://lpr-vision/imagenet/imagenet_train-000013.tgz` and `gs://lpr-vision/imagenet/imagenet_train-000024.tgz` have been requested
$ ais show job download QdwOYMAqg --progress --refresh 500ms
Files downloaded:                       0/2 [--------------------------------------------------------------] 0 %
imagenet_train-000013.tgz  31.2MiB/946.5MiB [=>------------------------------------------------------------| 00:24:35 ] 703.1 KiB/s
imagenet_train-000023.tgz  38.5MiB/945.9MiB [==>-----------------------------------------------------------| 00:12:50 ]   1.1 MiB/s
```

## Stop download job

`ais stop download JOB_ID`

Stop download job with given `JOB_ID`.

## Remove download job

`ais job rm download JOB_ID`

Remove the finished download job with given `JOB_ID` from the job list.

## Show download jobs and job status

`ais show job download [JOB_ID]`

Show download jobs or status of a specific job.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Regex for the description of download jobs | `""` |
| `--progress` | `bool` | Displays progress bar | `false` |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds) | `1s` |
| `--verbose` | `bool` | Verbose output | `false` |

### Examples

#### Show progress of given download job

Show progress bars for each currently downloading file with refresh rate of 500 ms.

```console
$ ais show job download 5JjIuGemR --progress --refresh 500ms
Files downloaded:                              0/141 [--------------------------------------------------------------] 0 %
imagenet/imagenet_train-000006.tgz 192.7MiB/947.0MiB [============>-------------------------------------------------| 00:08:52 ]   1.4 MiB/s
imagenet/imagenet_train-000015.tgz 238.8MiB/946.3MiB [===============>----------------------------------------------| 00:05:42 ]   2.1 MiB/s
imagenet/imagenet_train-000022.tgz  31.2MiB/946.5MiB [=>------------------------------------------------------------| 00:24:35 ] 703.1 KiB/s
imagenet/imagenet_train-000043.tgz  38.5MiB/945.9MiB [==>-----------------------------------------------------------| 00:12:50 ]   1.1 MiB/s
```

#### Show download job which description match given regex

Show all download jobs with descriptions starting with `download ` prefix.

```console
$ ais show job download --regex "^downloads (.*)"
JOB ID		 STATUS		 ERRORS	 DESCRIPTION
cudIYMAqg	 Finished	 0	 downloads whole imagenet bucket
fjwiIEMfa	 Finished	 0	 downloads range lpr-bucket from gcp://lpr-bucket
```

## Wait for download job

`ais wait download JOB_ID`

Wait for the download job with given `JOB_ID` to finish.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds). Ctrl-C to stop monitoring. | `1s` |
| `--progress` | `bool` | Displays progress bar | `false` |
