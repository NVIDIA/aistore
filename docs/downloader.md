---
layout: post
title: DOWNLOADER
permalink: /docs/downloader
redirect_from:
 - /downloader.md/
 - /docs/downloader.md/
---

## Why Downloader?

It probably won't be much of an exaggeration to say that the majority of popular AI datasets are available on the Internet and public remote buckets.
Those datasets are often growing in size, thus continuously providing a wealth of information to research and analyze.

It is, therefore, appropriate to ask a follow-up question: how to efficiently work with those datasets?
And what happens if the dataset in question is *larger* than the capacity of a single host?
What happens if it is large enough to require a cluster of storage servers?

> The often cited paper called [Revisiting Unreasonable Effectiveness of Data in Deep Learning Era](https://arxiv.org/abs/1707.02968) lists a good number of those large and very popular datasets, as well as the reasons to utilize them for training.

Meet **Internet Downloader** - an integrated part of the AIStore.
AIS cluster can be easily deployed on any commodity hardware, and AIS **downloader** can then be used to quickly populate AIS buckets with any contents from a given location.

## Features

> By way of background, AIStore supports a number of [3rd party Backend providers](/docs/providers.md) and utilizes the providers' SDKs to access the corresponding backends.
> For Amazon S3, that would be `aws-sdk-go` SDK, for Azure - `azure-storage-blob-go`, and so on.
> Each SDK can be **conditionally linked** into AIS executable - the decision (to link or not to link) is made prior to deployment.

This has a certain implication for the Downloader.
Namely:

Downloadable source can be both an Internet link (or links) or a remote bucket accessible via the corresponding backend implementation.
You can, for instance, download a Google Cloud bucket via its Internet location that would look something like: `https://www.googleapis.com/storage/.../bucket-name/...`.

However.
When downloading a remote bucket (**any** remote bucket), it is always **preferable** to have the corresponding SDK linked-in.
Downloader will then detect the SDK "presence" at runtime and use a wider range of options available via this SDK.

Other supported features include:

* Can download a single file (object), a range, an entire bucket, **and** a virtual directory in a given remote bucket.
* Easy to use with [command line interface](/docs/cli/download.md).
* Versioning and checksum support allows for an optimal download of the same source location multiple times to *incrementally* update AIS destination with source changes (if any).

The rest of this document describes these and other capabilities in greater detail and illustrates them with examples.

## Example

Downloading jobs run asynchronously; you can monitor the progress of each specific job.
The following example runs two jobs, each downloading 10 objects (gzipped tarballs in this case) from a given Google Cloud bucket:

```console
$ ais job start download "gs://lpr-imagenet/train-{0001..0010}.tgz" ais://imagenet
5JjIuGemR
Run `ais show job download 5JjIuGemR` to monitor the progress of downloading.
$ ais job start download "gs://lpr-imagenet/train-{0011..0020}.tgz" ais://imagenet
H9OjbW5FH
Run `ais show job download H9OjbW5FH` to monitor the progress of downloading.
$ ais show job download
JOB ID           STATUS          ERRORS  DESCRIPTION
5JjIuGemR        Finished        0       https://storage.googleapis.com/lpr-imagenet/imagenet_train-{0001..0010}.tgz -> ais://imagenet
H9OjbW5FH        Finished        0       https://storage.googleapis.com/lpr-imagenet/imagenet_train-{0011..0020}.tgz -> ais://imagenet
```

For more examples see: [Downloader CLI](/docs/cli/download.md)

## Request to download

AIS Downloader supports 4 (four) request types:

* **Single** - download a single object.
* **Multi** - download multiple objects provided by JSON map (string -> string) or list of strings.
* **Range** - download multiple objects based on a given naming pattern.
* **Backend** - given optional prefix and optional suffix, download matching objects from the specified remote bucket.

> Prior to downloading, make sure destination bucket already exists.
> To create a bucket using AIS CLI, run `ais bucket create`, for instance:
>
> ```console
> $ ais bucket create imagenet
> ```
>
> Also, see [AIS API](/docs/http_api.md) for details on how to create, destroy, and list storage buckets.
> For Python-based clients, a better starting point could be [here](/docs/overview.md#python-client).

The rest of this document is structured around supported *types of downloading jobs* and can serve as an API reference for the Downloader.

## Table of Contents

- [Single (object) download](#single-download)
- [Multi (object) download](#multi-download)
- [Range (object) download](#range-download)
- [Backend download](#backend-download)
- [Aborting](#aborting)
- [Status (of the download)](#status)
- [List of downloads](#list-of-downloads)
- [Remove from list](#remove-from-list)

## Single Download

The request (described below) downloads a *single* object and is considered the most basic.
This request returns *id* on successful request which can then be used to check the status or abort the download job.

### Request JSON Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`bucket.name` | `string` | Bucket where the downloaded object is saved to. | No |
`bucket.provider` | `string` | Determines the provider of the bucket. By default, locality is determined automatically. | Yes |
`bucket.namespace` | `string` | Determines the namespace of the bucket. | Yes |
`description` | `string` | Description for the download request. | Yes |
`timeout` | `string` | Timeout for request to external resource. | Yes |
`limits.connections` | `int` | Number of concurrent connections each target can make. | Yes |
`limits.bytes_per_hour` | `int` | Number of bytes the cluster can download in one hour. | Yes |
`link` | `string` | URL of where the object is downloaded from. | No |
`object_name` | `string` | Name of the object the download is saved as. If no objname is provided, the name will be the last element in the URL's path. | Yes |

### Sample Request

#### Single object download

```bash
$ curl -Li -H 'Content-Type: application/json' -d '{
  "type": "single",
  "bucket": {"name": "ubuntu"},
  "object_name": "ubuntu.iso",
  "link": "http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso"
}' -X POST 'http://localhost:8080/v1/download'
```

## Multi Download

A *multi* object download requires either a map or a list in JSON body:
* **Map** - in map, each entry should contain `custom_object_name` (key) -> `external_link` (value). This format allows object names to not depend on automatic naming as it is done in *list* format.
* **List** - in list, each entry should contain `external_link` to resource. Objects names are created from the base of the link.

This request returns *id* on successful request which can then be used to check the status or abort the download job.

### Request JSON Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`bucket.name` | `string` | Bucket where the downloaded object is saved to. | No |
`bucket.provider` | `string` | Determines the provider of the bucket. By default, locality is determined automatically. | Yes |
`bucket.namespace` | `string` | Determines the namespace of the bucket. | Yes |
`description` | `string` | Description for the download request. | Yes |
`timeout` | `string` | Timeout for request to external resource. | Yes |
`limits.connections` | `int` | Number of concurrent connections each target can make. | Yes |
`limits.bytes_per_hour` | `int` | Number of bytes the cluster can download in one hour. | Yes |
`objects` | `array` or `map` | The payload with the objects to download. | No |

### Sample Request

#### Multi Download using object map

```bash
$ curl -Li -H 'Content-Type: application/json' -d '{
  "type": "multi",
  "bucket": {"name": "ubuntu"},
  "objects": {
    "train-labels.gz": "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz",
    "t10k-labels-idx1.gz": "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz",
    "train-images.gz": "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"
  }
}' -X POST 'http://localhost:8080/v1/download'
```

#### Multi Download using object list

```bash
$ curl -Li -H 'Content-Type: application/json' -d '{
  "type": "multi",
  "bucket": {"name": "ubuntu"},
  "objects": [
    "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz",
    "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz",
    "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"
  ]
}' -X POST 'http://localhost:8080/v1/download'
```

## Range Download

A *range* download retrieves (in one shot) multiple objects while expecting (and relying upon) a certain naming convention which happens to be often used.
This request returns *id* on successful request which can then be used to check the status or abort the download job.

Namely, the *range* download expects the object name to consist of prefix + index + suffix, as described below:

### Range Format

Consider a website named `randomwebsite.com/some_dir/` that contains the following files:
- `object1log.txt`
- `object2log.txt`
- `object3log.txt`
- ...
- `object1000log.txt`

To populate AIStore with objects in the range from `object200log.txt` to `object300log.txt` (101 objects total), use the *range* download.

### Request JSON Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`bucket.name` | `string` | Bucket where the downloaded object is saved to. | No |
`bucket.provider` | `string` | Determines the provider of the bucket. By default, locality is determined automatically. | Yes |
`bucket.namespace` | `string` | Determines the namespace of the bucket. | Yes |
`description` | `string` | Description for the download request. | Yes |
`timeout` | `string` | Timeout for request to external resource. | Yes |
`limits.connections` | `int` | Number of concurrent connections each target can make. | Yes |
`limits.bytes_per_hour` | `int` | Number of bytes the cluster can download in one hour. | Yes |
`subdir` | `string` | Subdirectory in the `bucket` where the downloaded objects are saved to. | Yes |
`template` | `string` | Bash template describing names of the objects in the URL. | No |

### Sample Request

#### Download a (range) list of objects

```bash
$ curl -Lig -H 'Content-Type: application/json' -d '{
  "type": "range",
  "bucket": {"name": "test"},
  "template": "randomwebsite.com/some_dir/object{200..300}log.txt"
}' -X POST 'http://localhost:8080/v1/download'
```

#### Download a (range) list of objects into a subdirectory inside a bucket

```bash
$ curl -Lig -H 'Content-Type: application/json' -d '{
  "type": "range",
  "bucket": {"name": "test"},
  "template": "randomwebsite.com/some_dir/object{200..300}log.txt",
  "subdir": "some/subdir/"
}' -X POST 'http://localhost:8080/v1/download'
```

#### Download a (range) list of objects, selecting every tenth object

```bash
$ curl -Lig -H 'Content-Type: application/json' -d '{
  "type": "range",
  "bucket": {"name": "test"},
  "template": "randomwebsite.com/some_dir/object{1..1000..10}log.txt"
}' -X POST 'http://localhost:8080/v1/download'
```

**Tip:** use `-g` option in curl to turn off URL globbing parser - it will allow to use `{` and `}` without escaping them.

## Backend download

A *backend* download prefetches multiple objects which names match provided prefix and suffix and are contained in a given remote bucket.

### Request JSON Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`bucket.name` | `string` | Bucket where the downloaded object is saved to. | No |
`bucket.provider` | `string` | Determines the provider of the bucket. | Yes |
`bucket.namespace` | `string` | Determines the namespace of the bucket. | Yes |
`description` | `string` | Description for the download request. | Yes |
`sync` | `bool` | Synchronizes the remote bucket: downloads new or updated objects (regular download) + checks and deletes cached objects if they are no longer present in the remote bucket. | Yes |
`prefix` | `string` | Prefix of the objects names to download. | Yes |
`suffix` | `string` | Suffix of the objects names to download. | Yes |

### Sample Request

#### Download objects from a remote bucket

```bash
$ curl -Liv -H 'Content-Type: application/json' -d '{
  "type": "backend",
  "bucket": {"name": "lpr-vision", "provider": "gcp"},
  "prefix": "imagenet/imagenet_train-",
  "suffix": ".tgz"
}' -X POST 'http://localhost:8080/v1/download'
```

## Aborting

Any download request can be aborted at any time by making a `DELETE` request to `/v1/download/abort` with provided `id` (which is returned upon job creation).

### Request JSON Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`id` | `string` | Unique identifier of download job returned upon job creation. | No |

### Sample Request

#### Abort download

```console
$ curl -Li -H 'Content-Type: application/json' -d '{"id": "5JjIuGemR"}' -X DELETE 'http://localhost:8080/v1/download/abort'
```

## Status

The status of any download request can be queried at any time using `GET` request with provided `id` (which is returned upon job creation).

### Request JSON Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`id` | `string` | Unique identifier of download job returned upon job creation. | No |

### Sample Request

#### Get download status

```console
$ curl -Li -H 'Content-Type: application/json' -d '{"id": "5JjIuGemR"}' -X GET 'http://localhost:8080/v1/download'
```

## List of Downloads

The list of all download requests can be queried at any time. Note that this has the same syntax as [Status](#status) except the `id` parameter is empty.

### Request Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`regex` | `string` | Regex for the description of download requests. | Yes |

### Sample Requests

#### Get list of all downloads

```console
$ curl -Li -X GET 'http://localhost:8080/v1/download'
```

#### Get list of downloads with description starting with a digit

```console
$ curl -Li -H 'Content-Type: application/json' -d '{"regex": "^[0-9]"}' -X GET 'http://localhost:8080/v1/download'
```

## Remove from List

Any aborted or finished download request can be removed from the [list of downloads](#list-of-downloads) by making a `DELETE` request to `/v1/download/remove` with provided `id` (which is returned upon job creation).

### Request JSON Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
`id` | `string` | Unique identifier of download job returned upon job creation. | No |

### Sample Request

#### Remove download job from the list

```console
$ curl -Li -H 'Content-Type: application/json' -d '{"id": "5JjIuGemR"}' -X DELETE 'http://localhost:8080/v1/download/remove'
```
