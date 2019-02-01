### AIStore as an HTTP proxy

1. Set the field `rproxy` to `cloud` or `target` in  [the configuration](../ais/setup/config.sh) prior to deployment.
2. Set the environment variable `http_proxy` for HTTP requests and/or `https_proxy` for HTTPS requests (supported by most UNIX systems) to the primary proxy URL of your AIStore cluster.

```shell
$ export http_proxy=<PRIMARY-PROXY-URL>
```

When these two are set, AIStore will act as a reverse proxy for your outgoing HTTP requests.

### Caching

AIS executes cold GETs if and only if the designated object is not stored (by the AIS), or the object has a bad checksum, or the object's version is outdated. The next GET calls to the same object reads data from local storage increasing performance.


First call to Google Cloud Storage(GCS):

```shell
curl -L -X GET http://storage.googleapis.com/gcp-public-data-landsat/LT08/PRE/040/021/LT80400212013126LGN01/LT80400212013126LGN01_B10.TIF
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                   Dload  Upload   Total   Spent    Left  Speed
 100 60.9M    0 60.9M    0     0  20.3M      0 --:--:--  0:00:02 --:--:-- 20.3M

```

Next calls to GCS:

```shell
curl -L -X GET http://storage.googleapis.com/gcp-public-data-landsat/LT08/PRE/040/021/LT80400212013126LGN01/LT80400212013126LGN01_B10.TIF
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                   Dload  Upload   Total   Spent    Left  Speed
 100 60.9M    0 60.9M    0     0   402M      0 --:--:-- --:--:-- --:--:--  403M
```

Read speed increased from 20M/sec to 400M/sec.

### GCS requests

When caching objects, AIStore supports two URL schemas for HTTP requests to GCS: XML API and JSON API URLs. Below are examples of requesting the same object using different schemas. AIStore treats both types of URLs in the same way. It results in that the object is downloaded and cached by the first request, and then all following JSON and XML API requests receives the object from local storage.

XML API:

```shell
curl -L -X GET http://storage.googleapis.com/gcp-public/LT08/PRE/B10.TIF
```

JSON API:

```shell
curl -L -X GET http://www.googleapis.com/storage/v1/b/gcp-public/o/LT08%2FPRE%2fB10.TIF
```

### Caching limitations

As of version 2.0 AIStore supports caching only for HTTP GET requests to GCS. HTTPS requests to GCS and requests to other Cloud providers are not cached.
