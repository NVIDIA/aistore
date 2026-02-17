# GCP Per-Bucket Credentials

By default, all GCS operations use a single set of credentials - the service account
referenced by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable (or the ambient
credentials on GCE/GKE). This is sufficient when all GCS buckets belong to the same
GCP project and service account.

`extra.gcp.application_creds` lifts that restriction: it lets each bucket in AIS carry
its own GCP service-account JSON path, completely overriding the global default for that
bucket's operations.

**Table of Contents**

* [The `extra.gcp` section](#the-extragcp-section)
* [Setting per-bucket credentials](#setting-per-bucket-credentials)
* [Credentials file format](#credentials-file-format)
* [Registering a bucket that the default credentials cannot reach](#registering-a-bucket-that-the-default-credentials-cannot-reach)
* [Multiple buckets, multiple GCP projects](#multiple-buckets-multiple-gcp-projects)
* [Startup behavior](#startup-behavior)
* [Clearing per-bucket credentials](#clearing-per-bucket-credentials)
* [See also](#see-also)

----

## The `extra.gcp` section

To inspect the GCP-specific knobs for a bucket:

```console
$ ais bucket props show gs://abc extra.gcp
PROPERTY                         VALUE
extra.gcp.application_creds
```

or in JSON to see all supported fields regardless of whether they are set:

```console
$ ais bucket props show gs://abc extra.gcp --json
{
    "extra": {
        "gcp": {
            "application_creds": "-"
        }
    }
}
```

> **Note on `-`**: a dash in `--json` output means the field is supported but currently
> unset (no value configured). It is not a valid value to assign.

## Setting per-bucket credentials

```console
$ ais bucket props set gs://abc extra.gcp.application_creds /etc/ais/sa-team-b.json
"extra.gcp.application_creds" set to: "/etc/ais/sa-team-b.json" (was: "")
```

From this point on, every operation on `gs://abc` - GET, PUT, HEAD, LIST - will
authenticate using the service account in `/etc/ais/sa-team-b.json` instead of the
cluster-wide default.

The path must be **absolute and clean** (no `..` components). AIS validates this when the
property is set and returns an error if the path is relative or malformed.

## Credentials file format

The value is a file path to a standard GCP service-account JSON key:

```json
{
  "type": "service_account",
  "project_id": "my-gcp-project",
  "private_key_id": "...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n...",
  "client_email": "sa-team-b@my-gcp-project.iam.gserviceaccount.com",
  ...
}
```

AIS reads the `project_id` from the file at the time the session is first established.
The file path - not the credentials themselves - is what is stored in the bucket's BMD
entry, so secrets never leave the node's filesystem.
> The JSON key file itself is never replicated across nodes or transmitted over the network.
> Each AIS target must have access to the specified path locally.

In Kubernetes, service-account keys are typically mounted as files via a Secret volume:

```yaml
volumes:
  - name: gcp-sa
    secret:
      secretName: team-b-gcp-sa
volumeMounts:
  - name: gcp-sa
    mountPath: /etc/ais/sa
    readOnly: true
```

which makes `/etc/ais/sa/key.json` a normal readable file from AIS's perspective.

## Registering a bucket that the default credentials cannot reach

If the cluster's default service account has no access to the bucket, use `--skip-lookup`
to bypass the initial `HEAD` check:

```console
$ ais create gs://team-b-data --skip-lookup \
  --props="extra.gcp.application_creds=/etc/ais/sa-team-b.json"
"gs://team-b-data" created
```

Then verify access:

```console
$ ais ls gs://team-b-data
NAME             SIZE
...
```

> `--skip-lookup` registers the bucket in BMD without verifying reachability. Use it only
> when you know the bucket exists and the per-bucket credentials are correct.

## Multiple buckets, multiple GCP projects

`extra.gcp.application_creds` is per-bucket, so you can mix projects freely:

```console
$ ais create gs://#proj-a/dataset --skip-lookup \
  --props="extra.gcp.application_creds=/etc/ais/sa-proj-a.json"

$ ais create gs://#proj-b/dataset --skip-lookup \
  --props="extra.gcp.application_creds=/etc/ais/sa-proj-b.json"
```

`gs://#proj-a/dataset` and `gs://#proj-b/dataset` are distinct BMD entries with separate
on-disk paths, separate session caches, and independent credentials. The namespace
component (e.g., `#proj-a`, `#proj-b`) disambiguates same-name buckets across projects.

AIS caches the GCP client session keyed by credentials file path, so the overhead of
creating a new `*storage.Client` is paid once per unique credentials file across the
lifetime of the target process.

## Startup behavior

AIS initializes the default GCP session lazily - on first use, not at target startup.
A missing or invalid `GOOGLE_APPLICATION_CREDENTIALS` will not prevent the target from
starting

> An error will occur only when accessing a bucket that requires the default (missing or invalid) credentials.

Per-bucket sessions (`extra.gcp.application_creds`) are resolved the same way: on
demand, then cached.

## Clearing per-bucket credentials

To revert a bucket to the cluster-wide default:

```console
$ ais bucket props set gs://abc extra.gcp.application_creds ""

"extra.gcp.application_creds" set to: "" (was: "/etc/ais/sa-team-b.json")
```

## See also

- [AWS Profiles and S3 Endpoints](/docs/cli/aws_profile_endpoint.md)
- [Bucket Properties](/docs/bucket.md#bucket-properties)
- [Namespaced Buckets](/docs/bucket.md#namespaces)
