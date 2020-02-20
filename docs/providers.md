## Cloud Providers

AIS handles storage buckets which originate from external Clouds.
Supported cloud providers include: AIS, AWS, GCP (and all GCS and S3-compliant object storages)

For additional information on working with buckets please refer to [bucket readme](/docs/bucket.md)  
For additional information on buckets API please refer to [the RESTful API reference and examples](http_api.md)  

### AIS

Do not confuse with local deployment of AIS cluster.
With provider `ais` configured, cloud buckets of deployed AIS cluster, are local buckets of another, remote AIS cluster.  
To deploy cluser with `AIS` provider, update `cloud` field with remote AIS cluster URLs in config. Example:  
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
