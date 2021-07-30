---
layout: post
title: RPROXY
permalink: docs/rproxy
redirect_from:
 - docs/rproxy.md/
---

## AIS as HTTP(S) proxy: modes of operation

As HTTP(S) proxy, AIS currently supports two distinct (and very different) modes of operation:

* proxy-ing 3rd party (Cloud-based) object storage
* proxy-ing AIS targets

Most of this README describes the first mode of operation that can be further referred to as "cloud".

## AIS as HTTP(S) proxy vis-à-vis 3rd party object storages

AIS can be designated as HTTP(S) proxy vis-à-vis 3rd party object storages. This mode of operation is limited to Google Cloud Storage (GCS) and requires two settings:

1. HTTP(s) client side: set the `http_proxy` (`https_proxy` - for HTTPS) environment:

```console
$ export http_proxy=<AIS PROXY URL>
```

2. AIS configuration: set `rproxy=cloud` in the [configuration](/deploy/dev/local/aisnode_config.sh):

```json
    "http": {
        "use_https":    false,
        "rproxy":       "cloud",
        "rproxy_cache": true,
        ...
    }
```

### First cold GET followed by multiple warm GETs

AIS executes cold GET if and only when the designated object is not stored (by the AIS), **or** the object has a bad checksum, **or** the object's version is outdated. All subsequent GET calls will be terminated by the AIS itself.

Following are two concrete GET commands to retrieve a given named object from a GCS bucket called (in the example) `gcp-public-data-landsat`.

Note that the GET URLs are formatted as per the GCS guidelines and **unmodified**. Note also the difference in time it takes to execute the first (*cold*) GET versus subsequent (*warm*) ones:

The first call to Google Cloud Storage(GCS):

```console
$ curl -L -X GET http://storage.googleapis.com/gcp-public-data-landsat/LT08/PRE/040/021/LT80400212013126LGN01/LT80400212013126LGN01_B10.TIF
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                   Dload  Upload   Total   Spent    Left  Speed
 100 60.9M    0 60.9M    0     0  20.3M      0 --:--:--  0:00:02 --:--:-- 20.3M
```

The second GET of the same object:

```console
$ curl -L -X GET http://storage.googleapis.com/gcp-public-data-landsat/LT08/PRE/040/021/LT80400212013126LGN01/LT80400212013126LGN01_B10.TIF
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                   Dload  Upload   Total   Spent    Left  Speed
 100 60.9M    0 60.9M    0     0   402M      0 --:--:-- --:--:-- --:--:--  403M
```

GET performance increased from 20M/sec to 400M/sec.

### GCS requests

When caching objects, AIStore supports **two URL schemas** for HTTP requests to GCS:

* XML API
* JSON API

Below are the examples of requesting the same object using these two schemas. AIStore handles both types of URLs the same way: the requested object is downloaded and cached upon the first request; all the subsequent JSON (and XML) API requests will retrieve the object from the AIS storage.

Example XML API:

```console
$ curl -L -X GET http://storage.googleapis.com/gcp-public/LT08/PRE/B10.TIF
```

Example JSON API:

```console
$ curl -L -X GET http://www.googleapis.com/storage/v1/b/gcp-public/o/LT08%2FPRE%2fB10.TIF
```

### Caching limitations

* HTTPS: AIStore handles HTTPS requests transparently and without caching. In other words, every HTTPS GET will be a *cold* GET.
* GCS: As far as transparent caching, AIStore currently supports only Google Cloud Storage.

## AIS as HTTP(S) proxy vis-à-vis AIS targets

AIS supports a second (and special) mode whereby an AIS gateway serves as a **reverse proxy vis-à-vis AIS targets**.

The corresponding use case entails [configuring `rproxy=target`](/deploy/dev/local/aisnode_config.sh) and is intended to support (kernel-based) clients with a preference for a single (or a few) persistent storage connection(s).
```json
    "http": {
        "use_https":    false,
        "rproxy":       "target",
        "rproxy_cache": true,
        ...
    }
```

Needless to say, this mode of operation will clearly have performance implications.
