## Why Downloader?

It is a well-known fact that some of the most popular AI datasets are available on the Internet.

> See, for instance, [Revisiting Unreasonable Effectiveness of Data in Deep Learning Era](https://arxiv.org/abs/1707.02968) - the paper lists a good number of very large and very popular datasets.

Given that fact, it is only natural to ask the follow-up question: how to work with those datasets? And what happens if the dataset in question is *larger* than a single host? Meaning, what happens if it is large enough to warrant (and require) a distributed storage system?

Meet **Internet downloader** - an integrated part of the AIStore. AIStore cluster can be quickly deployed locally to the compute clients, and the **downloader** can then be used to quickly populate a specified AIS bucket with the objects from a given Internet location.

## Download Request

AIS *Downloader* supports 4 types of download requests:

* *Single* - download a single object
* *Multi* - download multiple objects provided by JSON map (string -> string) or list of strings
* *Range* - download multiple objects based on a given naming pattern
* *Cloud* - given optional prefix and optional suffix, download matching objects from the specified cloud bucket

> Prior to downloading, make sure that AIS (destination) bucket already exists. See [AIS API](/docs/http_api.md) for details on how to create, destroy, and list storage buckets. For Python-based clients, a better starting point could be [here](/README.md#python-client). Error is returned when provided bucket does not exist.

The rest of this document is structured around all supported types of downloads:

## Table of Contents
- [Single (object) download](#single-download)
- [Multi (object) download](#multi-download)
- [Range (object) download](#range-download)
- [Cloud download](#cloud-download)
- [Aborting](#aborting)
- [Status (of the download)](#status)
- [List of downloads](#list-of-downloads)
- [Remove from list](#remove-from-list)

## Single Download

The request (described below) downloads a *single* object and is considered the most basic.
This request returns *id* on successful request which can then be used to check the status or abort the download job.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the downloaded object is saved to. |
**provider** | **string** | Determines which bucket (`ais` or `cloud`) should be used. By default, locality is determined automatically | Yes
**description** | **string** | Description for the download request | Yes
**timeout** | **string** | Timeout for request to external resource. | Yes
**link** | **string** | URL of where the object is downloaded from. |
**objname** | **string** | Name of the object the download is saved as. If no objname is provided, the name will be the last element in the URL's path. | Yes

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Single object download | POST /v1/download | `curl -Liv -X POST 'http://localhost:8080/v1/download?bucket=ubuntu&objname=ubuntu.iso&link=http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso'` |

## Multi Download

A *multi* object download requires either a map or a list in JSON body:
* **Map** - in map, each entry should contain `custom_object_name` (key) -> `external_link` (value). This format allows object names to not depend on automatic naming as it is done in *list* format.
* **List** - in list, each entry should contain `external_link` to resource. Objects names are created from the base of the link (query parameters are stripped).

This request returns *id* on successful request which can then be used to check the status or abort the download job.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the downloaded objects are saved to. |
**provider** | **string** | Determines which bucket (`ais` or `cloud`) should be used. By default, locality is determined automatically. | Yes
**description** | **string** | Description for the download request | Yes
**timeout** | **string** | Timeout for request to external resource. | Yes

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Multi download using object map | POST /v1/download | `curl -Liv -X POST -H 'Content-Type: application/json' -d '{"train-labels.gz": "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz", "t10k-labels-idx1.gz": "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz", "train-images.gz": "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"}' http://localhost:8080/v1/download?bucket=yann-lecun` |
| Multi download using object list |  POST /v1/download | `curl -Liv -X POST -H 'Content-Type: application/json' -d '["http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"]' http://localhost:8080/v1/download?bucket=yann-lecun` |

## Range Download

A *range* download retrieves (in one shot) multiple objects while expecting (and relying upon) a certain naming convention which happens to be often used.
This request returns *id* on successful request which can then be used to check the status or abort the download job.

Namely, the *range* download expects the object name to consist of prefix + index + suffix, as described below:

### Range Format

Consider a website named `randomwebsite.com/some_dir/` that contains the following files:
- object1log.txt
- object2log.txt
- object3log.txt
- ...
- object1000log.txt

To populate AIStore with objects in the range from `object200log.txt` to `object300log.txt` (101 objects total), use the *range* download.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the downloaded objects are saved to. |
**subdir** | **string** | Subdirectory in the **bucket** where the downloaded objects are saved to. | Yes
**provider** | **string** | Determines which bucket (`ais` or `cloud`) should be used. By default, locality is determined automatically. | Yes
**description** | **string** | Description for the download request | Yes
**timeout** | **string** | Timeout for request to external resource. | Yes
**base** | **string** | Base URL of the object used to formulate the download URL. |
**template** | **string** | Bash template describing names of the objects in the URL. |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Download a (range) list of objects | POST /v1/download | `curl -Livg -X POST 'http://localhost:8080/v1/download?bucket=test321&template=randomwebsite.com/some_dir/object{200..300}log.txt'` |
| Download a (range) list of objects into a subdirectory inside a bucket | POST /v1/download | `curl -Livg -X POST 'http://localhost:8080/v1/download?bucket=test321&subdir=some/subdir/&template=randomwebsite.com/some_dir/object{200..300}log.txt'` |
| Download a (range) list of objects, selecting every tenth | POST /v1/download | `curl -Livg -X POST 'http://localhost:8080/v1/download?bucket=test321&template=randomwebsite.com/some_dir/object{1..1000..10}log.txt'` |

**Tip:** use `-g` option in curl to turn off URL globbing parser - it will allow to use `{` and `}` without escaping them.

## Cloud download

A *cloud* download prefetches multiple objects which names match provided prefix and suffix and are contained in a given cloud bucket.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Cloud bucket from which the data will be prefetched |
**timeout** | **string** | Timeout for request to external resource | Yes
**prefix** | **string** | Prefix of the objects names | Yes
**suffix** | **string** | Suffix of the objects names | Yes

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Download a list of objects from cloud bucket | POST /v1/download | `curl -L -X POST 'http://localhost:8080/v1/download?bucket=lpr-vision&prefix=imagenet/imagenet_train-&suffix=.tgz'`|

## Aborting

Any download request can be aborted at any time by making a `DELETE` request to `/v1/download/abort` with provided `id` (which is returned upon job creation).

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**id** | **string** | Unique identifier of download job returned upon job creation. |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Abort download | DELETE /v1/download/abort | `curl -Liv -X DELETE 'http://localhost:8080/v1/download/abort?id=5JjIuGemR'`|

## Status

The status of any download request can be queried at any time using `GET` request with provided `id` (which is returned upon job creation).

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**id** | **string** | Unique identifier of download job returned upon job creation. |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Get download status | GET /v1/download | `curl -Liv -X GET 'http://localhost:8080/v1/download?id=5JjIuGemR'`|


## List of Downloads

The list of all download requests can be queried at any time. Note that this has the same syntax as [Status](#status) except the `id` parameter is empty.

### Request Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**regex** | **string** | Regex for the description of download requests. | Yes

### Sample Requests

| Operation | HTTP action | Example |
|--|--|--|
| Get list of all downloads | GET /v1/download/ | `curl -Liv -X GET http://localhost:8080/v1/download`|
| Get list of downloads with description starting with a digit | GET /v1/download/ | `curl -Liv -X GET 'http://localhost:8080/v1/download?regex=^[0-9]'`|

## Remove from List

Any aborted or finished download request can be removed from the [list of downloads](#list-of-downloads) by making a `DELETE` request to `/v1/download/remove` with provided `id` (which is returned upon job creation).

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**id** | **string** | Unique identifier of download job returned upon job creation. |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Remove from list | DELETE /v1/download/remove | `curl -Liv -X DELETE 'http://localhost:8080/v1/download/remove?id=5JjIuGemR'`|
