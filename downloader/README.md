## Why Downloader?
It is a well-known fact that some of the most popular AI datasets have Internet addresses.

> See, for instance, [Revisiting Unreasonable Effectiveness of Data in Deep Learning Era](https://arxiv.org/abs/1707.02968) - the paper lists a good number of very large and very popular datasets.

Given that fact, it is only natural to ask the follow-up question: how to work with those datasets? And what happens if the dataset in question is *larger* than a single host? Meaning, what happens if it is large enough to warrant (and require) a distributed storage system?

Meet **Internet downloader** - an integrated part of the AIStore. AIStore cluster can be quickly deployed locally to the compute clients, and the **downloader** can be then used to quickly populate a specified (distributed) AIS bucket with the objects from a given Internet location.

## Download Request

AIStore supports 3 types of download requests:

* *Single* - download a single object
* *Multi* - download multiple objects
* *List* - download multiple objects based on a given naming pattern

> - Prior to downloading, make sure that AIS (destination) bucket already exists. See [AIS API](/docs/http_api.md) for details on how to create, destroy, and list storage buckets. For Python-based clients, a better starting point could be [here](/README.md#python-client).

Rest of this document is structured around these 3 supported types of downloads:

## Table of Contents
- [Single-object download](#single-object-download)
- [Multi-object download](#multi-object-download)
- [List download](#list-download)
- [Cloud bucket download](#bucket-download)
- [Cancellation](#cancellation)
- [Status of the download](#status-of-the-download)

## Single-object download

The request (described below) downloads a *single* object and is the most basic of the three.

### Request Body Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the download will be saved to |
**bck_provider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality will be determined automatically | Yes
**timeout** | **string** | Timeout for request to external resource | Yes
**link** | **string** | URL of where the object will be downloaded from. |
**objname** | **string** | Name of the object the download will be saved as. If no objname is provided, then the objname will be the last element in the URL's path. | Yes
**headers** | **JSON object** | JSON object where the key is a header name and the value is the corresponding header value (string). These values are used as the header values for when AIS actually makes the GET request to download the object from the link. | Yes

### Sample Request

| Operation | HTTP action  | Example  | Notes |
|--|--|--|--|
| Single Object Download | POST /v1/download/single | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"bucket": "ubuntu", "objname": "ubuntu.iso", "headers":  {  "Authorization": "Bearer AbCdEf123456" }, "link": "http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso"}' http://localhost:8080/v1/download/single`| Header authorization is not required to make this download request. It is just provided as an example. |

## Multi-object download

A *multi* object download requires either a map (denoted as **object_map** below) or a list (**object_list**).

### Request Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the downloaded objects will be saved to. |
**bck_provider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality will be determined automatically | Yes
**timeout** | **string** | Timeout for request to external resource | Yes
**headers** | **object** | JSON object where the key is a header name and the value is the corresponding header value(string). These values are used as the header values for when AIS actually makes the GET request to download the object. | Yes
**object_map** | **JSON object** | JSON object where the key (string) is the objname the object will be saved as and value (string) is a URL pointing to some file. | Yes
**object_list** | **JSON array** | JSON array where each item is a URL (string) pointing to some file. The objname for each file will be the last element in the URL's path. | Yes

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Multi Download Using Object Map | POST /v1/download/multi | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"bucket": "yann-lecun", "object_map": {"t10k-images-idx3-ubyte.gz": "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz", "t10k-labels-idx1-ubyte.gz": "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz", "train-images-idx3-ubyte.gz": "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"}}' http://localhost:8080/v1/download/multi` |
| Multi Download Using Object List |  POST /v1/download/multi  | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"bucket": "yann-lecun", "object_list": ["http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"]}' http://localhost:8080/v1/download/multi` |

## List download

A *list* download retrieves (in one shot) multiple objects while expecting (and relying upon) a certain naming convention which happens to be often used.

Namely, the *list* download expects the object name to consist of prefix + index + suffix, as described below:

### List Format

Consider a website named `randomwebsite.com/some_dir/` that contains the following files:
- object1log.txt
- object2log.txt
- object3log.txt
- ...
- object1000log.txt

To populate AIStore with objects in the range from `object200log.txt` to `object300log.txt` (101 objects total), use the *list* download.

### Request Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the downloaded objects will be saved to. |
**bck_provider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality will be determined automatically | Yes
**timeout** | **string** | Timeout for request to external resource | Yes
**headers** | **JSON object** | JSON object where the key is a header name and the value is the corresponding header value (string). These values are used as the header values for when AIS actually makes the GET request to download each object. | Yes
**base** | **string** | The base URL of the object that will be used to formulate the download url |
**template** | **string** | Bash template describing names of the objects |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Download a list of objects | POST /v1/download/list | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"bucket": "test321",  "base": "randomwebsite.com/some_dir/",  "template": "object{1..1000}log.txt"}' http://localhost:8080/v1/download/list` |
| Download a list of objects, selecting ever tenth | POST /v1/download/list | `curl -L -i -v -X POST -H 'Content-Type: application/json' -d '{"bucket": "test321",  "base": "randomwebsite.com/",  "template": "some_dir/object{1..1000..10}log.txt"}' http://localhost:8080/v1/download/list` |

## Bucket download

A *bucket* download prefetches multiple objects which are contained in given cloud bucket.

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Download a list of objects from cloud bucket | POST /v1/download/bucket | `curl -L -X POST 'http://localhost:8080/v1/download/bucket/lpr-vision?prefix=imagenet/imagenet_train-&suffix=.tgz'`|

## Cancellation

Any download request can be cancelled at any time by making a DELETE request to the **downloader**.

### Request Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the download was supposed to be saved to |
**bck_provider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality will be determined automatically | Yes
**link** | **string** | URL of the object that was added to the download queue |
**objname** | **string** | Name of the object the download was supposed to be saved as |

### Sample Request

| Operation | HTTP action  | Example |
|--|--|--|
| Cancel Download | DELETE v1/download | `curl -L -i -v -X DELETE -H 'Content-Type: application/json' -d '{"bucket": "ubuntu", "objname": "ubuntu.iso", "link": "http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso"}' http://localhost:8080/v1/download`|

### Status of the download

Status of any download request can be queried at any time.

### Request Parameters

Name | Type | Description | Optional?
------------ | ------------- | ------------- | -------------
**bucket** | **string** | Bucket where the download was supposed to be saved to |
**bck_provider** | **string** | Determines which bucket (`local` or `cloud`) should be used. By default, locality will be determined automatically | Yes
**link** | **string** | URL of the object that was added to the download queue |
**objname** | **string** | Name of the object the download was supposed to be saved as |

### Sample Request

| Operation | HTTP action | Example |
|--|--|--|
| Get Download Status | GET /v1/download/ | `curl -L -i -v -X GET -H 'Content-Type: application/json' -d '{"bucket": "ubuntu", "objname": "ubuntu.iso", "link": "http://releases.ubuntu.com/18.04.1/ubuntu-18.04.1-desktop-amd64.iso"}' http://localhost:8080/v1/download`|
