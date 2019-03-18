## Why Downloader?
It is a well-known fact that some of the most popular AI datasets are available on the Internet.

> See, for instance, [Revisiting Unreasonable Effectiveness of Data in Deep Learning Era](https://arxiv.org/abs/1707.02968) - the paper lists a good number of very large and very popular datasets.

Given that fact, it is only natural to ask the follow-up question: how to work with those datasets? And what happens if the dataset in question is *larger* than a single host? Meaning, what happens if it is large enough to warrant (and require) a distributed storage system?

Meet **Internet downloader** - an integrated part of the AIStore. AIStore cluster can be quickly deployed locally to the compute clients, and the **downloader** can then be used to quickly populate a specified AIS bucket with the objects from a given Internet location.

## Download Request

AIStore supports 4 types of download requests:

* *Single* - download a single object
* *Multi* - download multiple objects provided by JSON map (string -> string) or list of strings
* *Range* - download multiple objects based on a given naming pattern
* *Bucket* - given object name, optional prefix and optional suffix, download matching objects from the specified cloud bucket

> - Prior to downloading, make sure that AIS (destination) bucket already exists. See [AIS API](/docs/http_api.md) for details on how to create, destroy, and list storage buckets. For Python-based clients, a better starting point could be [here](/README.md#python-client).

The rest of this document is structured around all supported types of downloads:

## Table of Contents
- [Single (object) download](#single-download)
- [Multi (object) download](#multi-download)
- [Range (object) download](#list-download)
- [(cloud) Bucket download](#bucket-download)
- [Cancellation](#cancellation)
- [Status of the download](#status-of-the-download)

## Single download

The request (described below) downloads a *single* object and is considered the most basic.
This request returns *id* on successful request which can then be used to check the status or cancel the download job.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the downloaded object is saved to. |
**bprovider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality is determined automatically | Yes
**timeout** | **string** | Timeout for request to external resource. | Yes
**link** | **string** | URL of where the object is downloaded from. |
**objname** | **string** | Name of the object the download is saved as. If no objname is provided, the name will be the last element in the URL's path. | Yes

### Sample Request

| Operation | HTTP action  | Example  | Notes |
|--|--|--|--|
| Single Object Download | POST /v1/download | `curl -Liv -X POST 'http://localhost:8080/v1/download?bucket=ubuntu&objname=ubuntu.iso&link=http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso'`| Header authorization is not required to make this download request. It is just provided as an example. |

## Multi download

A *multi* object download requires either a map or a list (**object_list**) in JSON body.
This request returns *id* on successful request which can then be used to check the status or cancel the download job.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the downloaded objects are saved to. |
**bprovider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality is determined automatically. | Yes
**timeout** | **string** | Timeout for request to external resource. | Yes

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Multi Download Using Object Map | POST /v1/download | `curl -Liv -X POST -H 'Content-Type: application/json' -d '{"t10k-images-idx3-ubyte.gz": "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz", "t10k-labels-idx1-ubyte.gz": "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz", "train-images-idx3-ubyte.gz": "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"}' http://localhost:8080/v1/download?bucket=yann-lecun` |
| Multi Download Using Object List |  POST /v1/download | `curl -Liv -X POST -H 'Content-Type: application/json' -d '["http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"]' http://localhost:8080/v1/download?bucket=yann-lecun` |

## Range download

A *range* download retrieves (in one shot) multiple objects while expecting (and relying upon) a certain naming convention which happens to be often used.
This request returns *id* on successful request which can then be used to check the status or cancel the download job.

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
**bucket** | **string** |  Bucket where the downloaded objects are saved to. |
**bprovider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality is determined automatically. | Yes
**timeout** | **string** | Timeout for request to external resource. | Yes
**base** | **string** | Base URL of the object used to formulate the download URL. |
**template** | **string** | Bash template describing names of the objects in the URL. |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Download a (range) list of objects | POST /v1/download | `curl -Livg -X POST 'http://localhost:8080/v1/download?bucket=test321&base=randomwebsite.com/some_dir/&template=object{200..300}log.txt'` |
| Download a (range) list of objects, selecting every tenth | POST /v1/download | `curl -Livg -X POST 'http://localhost:8080/v1/download?bucket=test321&base=randomwebsite.com/some_dir/&template=object{1..1000..10}log.txt'` |

**Tip:** use `-g` option in curl to turn of URL globbing parser - it will allow to use `{` and `}` without escaping them.

## Bucket download

A *bucket* download prefetches multiple objects which are contained in given cloud bucket.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**timeout** | **string** | Timeout for request to external resource | Yes
**prefix** | **string** | Prefix of the object name | Yes
**suffix** | **string** | Suffix of the object name | Yes

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Download a list of objects from cloud bucket | POST /v1/download/bucket | `curl -L -X POST 'http://localhost:8080/v1/download/bucket/lpr-vision?prefix=imagenet/imagenet_train-&suffix=.tgz'`|

## Cancellation

Any download request can be cancelled at any time by making a DELETE request to the **downloader**.

### Request Query Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**id** | **string** | Unique identifier of download job returned upon job creation. |

### Sample Request

| Operation | HTTP action  | Example |
|--|--|--|
| Cancel Download | DELETE v1/download | `curl -Liv -X DELETE http://localhost:8080/v1/download?id=76794751-b81f-4ec6-839d-a512a7ce5612`|

### Status of the download

The status of any download request can be queried at any time.

### Request Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**id** | **string** | Unique identifier of download job returned upon job creation. |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Get Download Status | GET /v1/download/ | `curl -Liv -X GET http://localhost:8080/v1/download?id=76794751-b81f-4ec6-839d-a512a7ce5612`|
