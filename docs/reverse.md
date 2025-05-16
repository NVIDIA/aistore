**August 2024 UPDATE**: `reverse-proxying`, `ht://` backend, and associated capabilities described in this document are _obsolete_. The corresponding functionality is not supported and most likely will be eventually completely removed.

## AIS as reverse proxy

AIS can be designated as HTTP proxy vis-à-vis 3rd party object storages. This mode of operation requires:

1. HTTP(s) client side: set the `http_proxy` (`https_proxy` - for HTTPS) environment
2. Disable proxy for AIS cluster IP addresses/hostnames (for `curl` use option `--noproxy`)

Note that `http_proxy` is supported by most UNIX systems and is recognized by most (but not all) HTTP clients:

> HTTP(S) based datasets can only be used with clients which support an option of overriding the proxy for certain hosts (for e.g. `curl ... --noproxy=$(curl -s ais-gateway:port/v1/cluster?what=target_ips)`), where `ais-gateway:port` (above) denotes a `hostname:port` address of any AIS gateway.

```console
$ export http_proxy=<AIS proxy IPv4 or hostname>
```

In combination, these two settings have an effect of redirecting all **unmodified** client-issued HTTP(S) requests to the AIS proxy/gateway with subsequent execution transparently from the client perspective. AIStore will on the fly create a bucket to store and cache HTTP(S) reachable files all the while supporting the entire gamut of functionality including ETL.

## Public HTTP(S) Dataset

It is standard in machine learning community to publish datasets in public domains, so they can be accessed by everyone.
AIStore has integrated tools like [downloader](/docs/downloader.md) which can help in downloading those large datasets straight into provided AIS bucket.
However, sometimes using such tools is not a feasible solution.

For other cases AIStore has ability to act as a reverese-proxy when accessing **any** URL.
This enables downloading any HTTP(S) based content into AIStore cluster.
Assuming that proxy is listening on `localhost:8080`, one can use it as reverse-proxy to download `http://storage.googleapis.com/pub-images/images-train-000000.tar` shard into AIS cluster:

```console
$ curl -sL --max-redirs 3 -x localhost:8080 --noproxy "$(curl -s localhost:8080/v1/cluster?what=target_ips)" \
  -X GET "http://storage.googleapis.com/minikube/minikube-0.6.iso.sha256" \
  > /dev/null
```

Alternatively, an object can also be downloaded using the `get` and `cat` CLI commands.
```console
$ ais get http://storage.googleapis.com/minikube/minikube-0.7.iso.sha256 minikube-0.7.iso.sha256
```

This will cache shard object inside the AIStore cluster.
We can confirm this by listing available buckets and checking the content:

```console
$ ais ls
AIS Buckets (1)
  ais://local-bck
AWS Buckets (1)
  aws://ais-test
HTTP(S) Buckets (1)
  ht://ZDdhNTYxZTkyMzhkNjk3NA (http://storage.googleapis.com/minikube/)
$ ais ls ht://ZDdhNTYxZTkyMzhkNjk3NA
NAME                                 SIZE
minikube-0.6.iso.sha256	              65B
```

Now, when the object is accessed again, it will be served from AIStore cluster and will **not** be re-downloaded from HTTP(S) source.

Under the hood, AIStore remembers the object's source URL and associates the bucket with this URL.
In our example, bucket `ht://ZDdhNTYxZTkyMzhkNjk3NA` will be associated with `http://storage.googleapis.com/minikube/` URL.
Therefore, we can interchangeably use the associated URL for listing the bucket as show below.

```console
$ ais ls http://storage.googleapis.com/minikube
NAME                                  SIZE
minikube-0.6.iso.sha256	              65B
```

> Note that only the last part (`minikube-0.6.iso.sha256`) of the URL is treated as the object name.

Such connection between bucket and URL allows downloading content without providing URL again:

```console
$ ais object cat ht://ZDdhNTYxZTkyMzhkNjk3NA/minikube-0.7.iso.sha256 > /dev/null # cache another object
$ ais ls ht://ZDdhNTYxZTkyMzhkNjk3NA
NAME                     SIZE
minikube-0.6.iso.sha256  65B
minikube-0.7.iso.sha256  65B
```


