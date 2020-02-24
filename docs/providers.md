## Cloud Providers

AIS can be deployed as a fast tier in front of several storage backends. Supported cloud providers include: AIS (`ais`) itself, as well as AWS (`aws`), GCP (`gcp`), and all S3 and Google Cloud compliant storages.

* For additional information on working with buckets, please refer to [bucket readme](./bucket.md)
* For API reference, see [the RESTful API reference and examples](./http_api.md)
* For AIS command-line management, see [CLI](/cli/README.md)

### AIS <=> AIS

Remote AIS cluster is specified in the `cloud` section of the [configuration](./configuration.md).

Example:
```
"cloud": {
  "ais": {
    "cluster-uuid": [
      "http://192.168.4.2:8080",
      "https://ais-cluster.example.org"
    ]
  }
}
```
Multiple remote AIS cluster URLs can be provided for the same usual reasons that include redundancy and (future) load balancing.
On failure to connect via any one of those multiple URLs AIS (client) cluster will retry with another URL.

`AIS` provider enables multi-tier architecture of AIS clusters (AIS behind AIS).

![AIS-behind-AIS](./images/ais-behind-ais.png)

#### AWS

[Amazon Web Services Simple Storage System (AWS S3)](https://aws.amazon.com/s3/)

#### GCP

[Google Cloud Platform](https://cloud.google.com/)
