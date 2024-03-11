## S3 bucket inventory

Quoting [Amazon S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html):

> "Amazon S3 Inventory provides comma-separated values (CSV), Apache optimized row columnar (ORC) or Apache Parquet output files that list your objects and their corresponding metadata on a daily or weekly basis for an S3 bucket or objects with a shared prefix (that is, objects that have names that begin with a common string)."

AIStore fully supports **listing remote S3** buckets _via_ their own (remote) inventories.

In other words, instead of performing the corresponding SDK call (`ListObjectsV2`, in this case), AIStore will - behind the scenes - utilize existing bucket inventory.

But note: the capability is explicitly provided to list **very large** remote buckets.

## Recommended usage (examples)

To put it in more concrete terms, let's say there's a bucket `s3://abc` that contains 10,100,000 objects, and in particular:
* 10 million objects under `s3://abc/large/`
* 100K in `s3://abc/small/`

Given such (ballpark) sizes, it might stand to reason to employ inventory as follows:

```console
## see '--help' for details
$ ais ls s3://abc --all --inventory
$ ais ls s3://abc --all --prefix large --inventory
$ ais ls s3://abc --all --prefix small
```

Notwithstanding CLI examples above, the feature (as always) is provided via AIStore APIs as well.

> For background and references, please lookup `apc.HdrInventory` in [CLI](https://github.com/NVIDIA/aistore/tree/main/cmd/cli/cli) and [Go API](https://github.com/NVIDIA/aistore/blob/main/api/ls.go).

## Managing inventories

As of Q1 2024, the operations to enable, list, disable inventories are _scripted_. The scripts themselves can be located in [`deploy/dev/scripts/s3`](https://github.com/NVIDIA/aistore/tree/main/deploy/dev/s3), and include:

| script | description |
| --- | --- |
| `delete-bucket-inventory.sh` | disable inventory for a bucket |
| `put-bucket-inventory.sh` | enable inventory or modify inventory settings for a bucket |
| `list-bucket-inventory.sh` | show existing inventories for a bucket |
| `get-bucket-inventory.sh` | show detailed info about a given inventory |
| `put-bucket-policy.sh` | grant access to the bucket (so that remote S3 could store periodically generated inventories in the  bucket) |

All scripts have only one required argument: bucket name. For the rest arguments, their default values are:

- inventory `ID` = inventory ID
- inventory prefix = `.inventory`
- frequency = `Weekly`

Example of `list` (a concise output):

```
$ ./deploy/dev/aws/list-inventory.sh -b ais-vm
ID      PREFIX  FREQUENCY
1234    inv-all Daily
```

Example of `get` (more detailed):

```
$ ./deploy/scripts/aws/get-bucket-inventory.sh -b ais-vm -n 1234
ID      1234
Prefix  inv-all
Frequency       Daily
Enabled true
Format  CSV
Fields  ["Size","ETag"]
```
