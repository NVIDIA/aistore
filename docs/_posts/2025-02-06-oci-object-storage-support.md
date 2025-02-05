---
layout: post
title: "Arrival of native backed OCI Object Storage support"
date: February 6, 2025
author: Ed McClanahan
categories: aistore oci
--- 

Oracle Cloud Infrastructure ("OCI") has been supported via OCI's Amazon S3 Compatibility
API for quite some time. As such, AIStore's support for the S3 protocol has enabled OCI's
Object Storage to be utilized as a backend. But OCI also provides a more optimized API.
AIStore has now added support for using OCI Object Storage as a backend via this OCI-native
protocol.

## Deploying OCI Backend Support

As with other backends, OCI Object Storage utilizing their OCI-native protocol is enabled
by including `oci` in the `AIS_BACKEND_PROVIDERS` environment variable. Doing so will
trigger the OCI Object Storage being linked and configured utilizing the OCI config file
found at `~/.oci/config`. See [AIS Getting Started](https://aistore.nvidia.com/docs/getting_started.md) for a helpful guide on
deployment steps.

You can override the location of this config file by setting the `OCI_CLI_CONFIG_FILE`
environment variable. Several of the values searched for in this configuration file may
also be overridden by setting various other environment variables:

| Config File Key Name | ENV variable to override                 |
| -------------------- | ---------------------------------------- |
| tenancy              | OCI_TENANCY_OCID                         |
| user                 | OCI_USER_OCID                            |
| region               | OCI_REGION                               |
| fingerprint          | OCI_FINGERPRINT                          |
| key_file             | OCI_PRIVATE_KEY (PEM-formatted contents) |

Note that the `OCI_PRIVATE_KEY` environment variable should actually contain the (unencrypted)
contents of the `key_file`. For an (unencrypted) private key file located at `~/.oci/prikey.pem`,
the `OCI_PRIVATE_KEY` environment variable may be set simply by:

```sh
$ export OCI_PRIVATE_KEY=$(cat ~/.oci/prikey.pem)
```

While all existing buckets implicitly belong to specific OCI Compartments, in order to perform
operations like listing all buckets, AIStore must also be provided the OCID for a particular
Compartment by setting the `OCI_COMPARTMENT_OCID` environment variable to it's OCID.

### Sample Local Deployment

Here we will deploy a single storage target single proxy (gateway) AIStore instance with our
desired OCI backend. First things first, ensure you have a properly formatted OCI CLI Config
File (as described in
[OCI Config File Setup](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm)):

```sh
$ ls ~/.oci
config
prikey.pem
pubkey.pem
...
$ cat ~/.oci/config
[DEFAULT]
user=ocid1.user.oc1..aaaa...
fingerprint=01:23:...
tenancy=ocid1.tenancy.oc1..aaaa...
region=us-ashburn-1
key_file=~/.oci/prikey.pem
```

Be sure to set the CompartmentID's OCID environment variable:

```sh
$ export OCI_COMPARTMENT_OCID="ocid1.compartment.oc1..aaaa..."
```

Now launch a simple AIStore instance:

```sh
$ AIS_BACKEND_PROVIDERS="oci" make kill clean cli deploy <<< $'1\n1'
```

## Accessing OCI Object Storage

As with other supported backends, the OCI Object Storage buckets are referenced with either the
`oc:` or `oci:` prefix. For a comparison between OCI CLI commands and their equivalent using the
AIStore CLI, see the following:

| OCI CLI                                               | AIS CLI equivalent               |
| ----------------------------------------------------- | -------------------------------- |
| oci os bucket list                                    | ais ls oc: --all                 |
| oci os bucket get --bucket-name bname                 | ais bucket show oc://bname       |
| oci os object list -bn bname                          | ais ls oc://bname                |
| oci os object put -bn bname --file fname --name oname | ais put fname oc://dir/oname     |
| oci os object head -bn bname --name oname             | ais object show oc://bname/oname |
| oci os object get -bn bname --name oname --file fname | ais get oc://bname/oname fname   |
| oci os object delete -bn bname --force --name oname   | ais rmo oc://bname/oname         |

Note the addition of the `--all` on the `ais ls` command. This may be required if AIStore has
yet to cache anything from any bucket from the configured OCI Object Storage. Similarly, the
`ais object show` command may need to be adorned with `--not-cached` if that particular object
has yet to be accessed via AIStore.

## Additional Settings Available

Several behaviors of access to OCI Object Storage may be tuned via environment variables:

| ENV variable                        | Default | Tuning Effect                                         |
| :---------------------------------- | ------: | :---------------------------------------------------- |
| OCI_MAX_PAGE_SIZE                   |    1000 | Max objects per page in bucket listing                |
| OCI_MAX_DOWNLOAD_SEGMENT_SIZE       |  256MiB | ["MPD"] Multi-threaded Object Download segment size   |
| OCI_MULTI_PART_DOWNLOAD_THRESHOLD   |  512MiB | Object size trigger to use MPD                        |
| OCI_MULTI_PART_DOWNLOAD_MAX_THREADS |      16 | Max concurrent segment downloads per MPD of an object |
| OCI_MAX_UPLOAD_SEGMENT_SIZE         |  256MiB | ["MPU"] Multi-threaded Object Upload segment size     |
| OCI_MULTI_PART_UPLOAD_THRESHOLD     |  512MiB | Object size trigger to use MPU                        |
| OCI_MULTI_PART_UPLOAD_MAX_THREADS   |      16 | Max concurrent segment uploads per MPU of an object   |
| OCI_MULTI_PART_THREAD_POOL_SIZE     |    1024 | Max concurrent threads for all concurrent MPDs & MPUs |

### Multi-Part Performance Tuning Examples

To demonstrate the advantage of "multi part upload/download" tuning, let's start with a simple test file
that is practical for accessing OCI Object Storage remotely:

```sh
$ dd if=/dev/zero of=z bs=100000 count=100
100+0 records in
100+0 records out
10000000 bytes (10 MB, 9.5 MiB) copied, 0.00584804 s, 1.7 GB/s
```

Next, let's shrink some of those "MULTI_PART" environment variables considerably:

```sh
$ export OCI_MAX_DOWNLOAD_SEGMENT_SIZE="100000"
$ export OCI_MULTI_PART_DOWNLOAD_THRESHOLD="200000"
$ export OCI_MULTI_PART_DOWNLOAD_MAX_THREADS="64"

$ export OCI_MAX_UPLOAD_SEGMENT_SIZE="100000"
$ export OCI_MULTI_PART_UPLOAD_THRESHOLD="200000"
$ export OCI_MULTI_PART_UPLOAD_MAX_THREADS="64"
```

Restart your local AIStore instance and perform a PUT operation on that test file. Note that the
test file `z` is much larger than the value of `OCI_MAX_UPLOAD_SEGMENT_SIZE`, thus a Multi-Part
Upload will be used.

```sh
$ time ais put z oc://edmc/z
PUT "z" => oc://edmc/z

real  0m2.879s
user  0m0.018s
syd   0m0.023s
```

Next, let's up those thresholds to well exceed the size of the test file `z`:

```sh
$ export OCI_MULTI_PART_DOWNLOAD_THRESHOLD="200000000"
$ export OCI_MULTI_PART_UPLOAD_THRESHOLD="200000000"
```

Restart your local AIStore instance and repeat the PUT operation. Here, the Multi-Part Upload
functionality will be bypassed.

```sh
$ time ais put z oc://edmc/z
PUT "z" => oc://edmc/z

rea;  0m15.428s
user  0m0.019s
sys   0m0.026s
```

With the Multi-Part Download threshold also forcing single-threaded downloading for now, we
will see something like:

```sh
$ ais object evict oc://edmc/z
evicted "z" from oc://edmc

$ time ais get --yes oc://edmc/z z
GET z from oc://edmc as z (9.54MiB)

real  0m10.880s
user  0m0.018s
syd   0m0.033s
```

Finally, let's return those threshold values to the original values:

```sh
$ export OCI_MULTI_PART_DOWNLOAD_THRESHOLD="200000"
$ export OCI_MULTI_PART_UPLOAD_THRESHOLD="200000"
```

Restartinfg your local AIStore instance one last time and repeat the GET operation. Here, the
Multi-Part Download functionality will be enabled.

```sh
$ ais object evict oc://edmc/z
evicted "z" from oc://edmc

$ time ais get --yes oc://edmc/z z
GET z from oc://edmc as z (9.54MiB)

real  0m3.761s
user  0m0.025s
syd   0m0.029s
```

To summarize, we crafted a modest test file comparing upload (PUT) and download (GET)
performance both single-threaded and multi-threaded techniques by adjusting the thresholds
at which the "Multi-Part" functionality was triggered. These results are summarized here:

| Operation      | Single-threaded | Multi-threaded |
| :------------- | --------------: | -------------: |
| [PUT] Upload   |       0m15.428s |       0m2.879s |
| [GET] Download |       0m10.880s |       0m3.761s |

## Conclusion

Native support for OCI Object Storage is now available as an AIStore backend. This support
includes tunable optimizations for uploading and downloading of large objects between an
AIStore (it's client and it's local cache) and the OCI Object Storage infrastructure.

Looking ahead, we will be focusing on leveraging the native OCI Object Storage support to
produce more optimal performance relative to what is possible when utilizing the S3 protocol
translation/proxy capability,

## References

- [AIStore](https://github.com/NVIDIA/aistore)
- [AIS Getting Started](https://aistore.nvidia.com/docs/getting_started.md)
- [AIS as a Fast-Tier](https://aistore.nvidia.com/blog/2023/11/27/aistore-fast-tier)
- [AIS Command Line Interface](https://aistore.nvidia.com/docs/cli)
- [OCI Object Storage](https://www.oracle.com/cloud/storage/object-storage/)
- [OCI Command Line Interface](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/cliconcepts.htm)
- [OCI Config File Setup](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm)
