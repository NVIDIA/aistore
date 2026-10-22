In this document:

- [Generating self-signed certificates](#generating-self-signed-certificates)
- [Deploying: 4 targets, 1 gateway, 6 mountpaths, AWS backend](#deploying-4-targets-1-gateway-6-mountpaths-aws-backend)
- [Accessing HTTPS-based cluster](#accessing-https-based-cluster)
- [Testing with self-signed certificates](#testing-with-self-signed-certificates)
- [Observability: TLS related alerts](#observability-tls-related-alerts)
- [Updating and reloading X.509 certificates](#updating-and-reloading-x509-certificates)
- [Public and intra-cluster TLS configuration](#public-and-intra-cluster-tls-configuration)
- [kTLS transmit offload](#ktls-transmit-offload)
- [Further references](#further-references)

## Generating self-signed certificates

Creating a self-signed certificate along with its private key and a Certificate Authority (CA) certificate using [OpenSSL](https://www.openssl.org/) involves steps:

* First, we create a self-signed Certificate Authority (CA) certificate (`ca.crt`).
* Second, create a Certificate Signing Request (`server.csr`).
* Finally, based on the CSR and CA we create the server certs (`server.key` and `server.crt`) - as follows:

```console
openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt -days 1024 -nodes -subj "/CN=localhost" -extensions v3_ca -config <(printf "[req]\ndistinguished_name=req\nx509_extensions=v3_ca\n[ v3_ca ]\nsubjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1\nbasicConstraints=CA:TRUE\n")

openssl req -new -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/C=US/ST=California/L=Santa Clara/O=NVIDIA/OU=AIStore/CN=localhost" -config <(printf "[req]\ndistinguished_name=req\nreq_extensions = v3_req\n[ v3_req ]\nsubjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1\n")

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256 -extfile <(printf "[ext]\nsubjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n") -extensions ext
```

> **Important:** make sure to specify correct DNS. For local deployments, `localhost` and `127.0.0.1` domain must be provided - otherwise certificate validation will fail.

## Deploying: 4 targets, 1 gateway, 6 mountpaths, AWS backend

For a **new** Local Playground cluster, use a certificate whose Subject Alternative Names cover the proxy and target addresses used by clients and other AIS nodes. The certificates above cover `localhost` and `127.0.0.1`. To change an existing cluster's protocol, see [Switching a cluster to HTTPS](switch_https.md).

From the repository root, after confirming there is no existing Local Playground instance using the same paths, start a new cluster with certificate verification enabled for AIS-initiated TLS connections and the AWS backend built in:

```sh
TAGS=aws AIS_USE_HTTPS=true AIS_SKIP_VERIFY_CRT=false \
  AIS_SERVER_CRT="$PWD/server.crt" AIS_SERVER_KEY="$PWD/server.key" \
  AIS_CLIENT_CA_TLS="$PWD/ca.crt" AIS_CLIENT_AUTH_TLS=0 \
  make deploy <<< $'4\n1\n6'
```

This starts four targets and one gateway with six mountpaths per target, using the `server.crt` and `server.key` created in the current directory. `TAGS=aws` builds the AWS-backend variant; omit it if you do not need that backend. Configure AWS credentials on the AIS targets before accessing S3; refer to [AWS environment variables](/docs/environment-vars.md#aws-s3). `AIS_CLIENT_CA_TLS` supplies the CA for AIS-initiated outbound TLS verification and also supplies server-side `ClientCAs` when client authentication verifies certificates. This non-mTLS example explicitly sets `AIS_CLIENT_AUTH_TLS=0` (no client certificate requested). The CLI uses a separate `AIS_CLIENT_CA` setting for its trust.

## Accessing HTTPS-based cluster

For the new Local Playground cluster above, supply the CA that signed `server.crt` to the CLI and verify the hostname or IP address against the certificate:

```sh
AIS_ENDPOINT=https://127.0.0.1:8080 AIS_CLIENT_CA="$PWD/ca.crt" \
  AIS_SKIP_VERIFY_CRT=false ais show cluster
```

`AIS_CLIENT_CA` overrides the CLI's `cluster.client_ca_tls` setting and augments system-trusted roots. If verification fails, check the certificate's Subject Alternative Names, CA file, validity, and endpoint address. For mutual TLS, configure server-side `AIS_CLIENT_CA_TLS` and `AIS_CLIENT_AUTH_TLS` as well as CLI-side `AIS_CRT` and `AIS_CRT_KEY`; see [Public and intra-cluster TLS configuration](#public-and-intra-cluster-tls-configuration).

## Testing with self-signed certificates

The previous example verifies the server certificate. The following variables control different sides of the TLS connection:

| var name | description | the corresponding cluster configuration |
| -- | -- | -- |
| `AIS_USE_HTTPS`          | when false, we use plain HTTP with all the TLS config (below) simply **ignored** | "net.http.use_https" |
| `AIS_SERVER_CRT`         | aistore cluster X.509 certificate | "net.http.server_crt" |
| `AIS_SERVER_KEY`         | certificate's private key | "net.http.server_key"|
| `AIS_DOMAIN_TLS`         | NOTE: not supported, must be empty (domain, hostname, or SAN registered with the certificate) | "net.http.domain_tls"|
| `AIS_CLIENT_CA_TLS`      | CA file for AIS-initiated TLS verification and, when enabled, server-side client-certificate validation; not the CLI's trusted CA | "net.http.client_ca_tls" |
| `AIS_CLIENT_AUTH_TLS`    | Client authentication during TLS handshake: a range from 0 (no authentication) to 4 (request and validate client's certificate) | "net.http.client_auth_tls" |
| `AIS_SKIP_VERIFY_CRT`    | when true: skip certificate verification for AIS-initiated TLS clients and, when set for the CLI, the CLI's server check; use only for isolated tests | "net.http.skip_verify" (server-side setting) / "cluster.skip_verify_crt" (CLI setting) |

> More info on [`AIS_CLIENT_AUTH_TLS`](https://pkg.go.dev/crypto/tls#ClientAuthType).

For a test deployment that requires mutual TLS, `ais/test/tls-env/server.conf` illustrates the server certificate, key, client CA, and `AIS_CLIENT_AUTH_TLS=4`; `ais/test/tls-env/client.conf` illustrates the CLI client certificate, key, and trusted CA. Replace the test file paths with your actual files. Do not source test settings into an existing deployment or delete Smap files as a generic TLS setup step.

`AIS_SKIP_VERIFY_CRT=true` on the CLI bypasses server certificate verification; reserve it for isolated tests. For verified TLS, provide the CA as shown above.

> See also: [Client-side TLS environment](/docs/cli.md#environment-variables)

## Observability: TLS related alerts

HTTPS deployment implies (and requires) that each AIS node has a valid TLS (a.k.a. [X.509](https://www.ssl.com/faqs/what-is-an-x-509-certificate/)) certificate.

The latter has a number of interesting properties ultimately intended to authenticate clients to the server (AIS node, in this case), and vice versa.

TLS certificates have expiration dates. Their lifetime depends on the issuer and deployment policy, so check and renew them before they expire. AIS monitors certificate validity and reloads updated certificates; check node alerts and logs for renewal failures.

AIStore (AIS) always:

* tracks certificate expiration times;
* automatically - upon update - reloads updated certificates;
* raises associated alerts.

### Associated alerts

```console
$ ais show cluster

PROXY            MEM AVAIL  LOAD AVERAGE    UPTIME      STATUS  ALERT
p[KKFpNjqo][P]   127.77GiB  [5.2 7.2 3.1]   108h30m40s  online  **tls-cert-will-soon-expire**
...

TARGET           MEM AVAIL  CAP USED(%)     CAP AVAIL   LOAD AVERAGE    UPTIME      STATUS  ALERT
t[pDztYhhb]      98.02GiB   16%             960.824GiB  [9.1 13.4 8.3]  108h30m1s  online   **tls-cert-will-soon-expire**
...
...
```

Overall, there are currently 3 (three) alerts:

| alert | comment |
| -- | -- |
| `tls-cert-will-soon-expire` | a warning that X.509 cert will expire in less than 3 days |
| `tls-cert-expired` | red alert (as the name implies) |
| `tls-cert-invalid` | ditto |

## Updating and reloading X.509 certificates

Quoting WWW:

> "The validity periods for digital certificates are determined by their accepting organizations ...", at [https://www.sectigo.com/resource-library/how-long-are-digital-certificates-valid](https://www.sectigo.com/resource-library/how-long-are-digital-certificates-valid)

Long story short, there are many reasons to update or replace the X.509 certificates AIS cluster uses to authenticate API calls.

But maybe the most common one is - expiration time. TLS certificates, also often referred to as [X.509](https://www.itu.int/rec/T-REC-X.509) certificates, do periodically expire. And the immediate implication is that the system must be able to (periodically) reload updated ones.

In AIS, related functionality consists of two pieces:

1. AIS nodes automatically reload updated certs while simultaneously adjusting the interval to check for the update.
2. Separately, there's an administrative [API](https://github.com/NVIDIA/aistore/blob/main/api/cluster.go) and CLI (shown below) to reload certificate.

The scope of this latter operation may be either a selected node or entire cluster.

As far as automatic adjustment of the polling interval, the resulting value depends on the remaining time (until expired) and works [approximately](https://github.com/NVIDIA/aistore/blob/main/cmn/certloader/certloader.go) as follows:

| time to expire | period to check for renewal |
| -- | -- |
| more than 24h | 6 hours |
| more than 6h | 1 hour |
| more than 1h | 10m |
| more than 1s | 1m |
| `expired` | 1h |

Upon initial loading, or every time when reloading, an AIS node logs a record that also shows the validity bounds, e.g.:

```log
I 11:05:45.753438 certloader:151 server.crt[26 Aug 24 18:18 UTC, 26 Aug 25 18:18 UTC]
```

In addition, if certificate fails to load or expires, AIS node raises the namesake alert that - as usual - will show up in Grafana dashboard or via CLI `show cluster`, or both.

```console
$ ais show cluster

PROXY            MEM USED(%)    MEM AVAIL   LOAD AVERAGE    UPTIME  STATUS  ALERT
p[atipJhgn][P]   0.17%          27.51GiB    [0.3 0.1 0.0]   -       online  **tls-cert-expired**

TARGET           MEM USED(%)    MEM AVAIL   CAP USED(%)     CAP AVAIL       LOAD AVERAGE    STATUS  ALERT
t[NlLtPtrm]      0.16%          27.51GiB    16%             367.538GiB      [0.3 0.1 0.0]   online  **tls-cert-expired**
```

Overall, supported alerts include:

| alert | comment |
| -- | -- |
| `tls-cert-will-soon-expire` | warning: less than 3 days remains until X.509 cert expires |
| `tls-cert-expired` | X.509 expired (red alert, as the name implies) |
| `tls-cert-invalid` | e.g., invalid PEM format; further details at [OpenSSL: X.509 errors](https://x509errors.org/)  |

To view existing or reload new TLS certificates, see the following CLI reference:

* [Managing TLS Certificates](/docs/cli/x509.md)

When AIS is deployed with authentication enabled, the API (and CLI) to reload certificates will require administrative permissions.

> See `ais config cluster` command and related `auth.client_auth_required` knob.

## Public and intra-cluster TLS configuration

AIS uses one cluster-wide HTTP/HTTPS mode (`net.http.use_https`) but may use different TLS configurations on its public and intra-cluster listeners.

| Listener | TLS configuration |
|---|---|
| Intra-control | `net.http` |
| Intra-data | `net.http` |
| Public and additional public interfaces | `net.http.pub`, when configured |
| Public without `net.http.pub` | Inherits the complete `net.http` TLS configuration |

The optional `net.http.pub` section is an independent TLS configuration, not a field-by-field overlay.

When `net.http.pub` is absent, public listeners reuse the default `net.http` TLS configuration, including its certificate and client-authentication policy.

When `net.http.pub` is configured:

- `server_crt` and `server_key` must both be specified;
- public listeners use the public certificate and key;
- `client_auth_tls` and `client_ca_tls` are independent of their `net.http` counterparts;
- when the public TLS section is first configured, an omitted `client_auth_tls`
  defaults to `NoClientCert`; it is not inherited from `net.http`;
- the default and public certificates are loaded, reloaded, monitored, and reported independently.

> **All public listeners share one TLS configuration.** The primary public address and all additional
> [multi-home interfaces](/docs/networking.md) use the same certificate: `net.http.pub` when configured,
> or the default `net.http` certificate otherwise. AIS currently provides no per-address or
> SNI-based certificate selection.
>
> The certificate's Subject Alternative Name (**SAN**) extension must cover every DNS name or IP address
> clients use to reach these listeners. Otherwise, client-side certificate verification fails with a
> name mismatch when the cluster is accessed through an uncovered name or address.

`client_auth_tls` uses Go standard library `tls.ClientAuthType` values:

| Value | Mode | Client certificate | Verification | `client_ca_tls` required |
|---:|---|---|---|---|
| 0 | `NoClientCert` | Not requested | No | No |
| 1 | `RequestClientCert` | Requested, optional | No | No |
| 2 | `RequireAnyClientCert` | Required | No | No |
| 3 | `VerifyClientCertIfGiven` | Requested, optional | Yes, when provided | Yes |
| 4 | `RequireAndVerifyClientCert` | Required | Yes | Yes |

In particular, `RequireAnyClientCert` requires a client certificate but does not verify its chain. AIS therefore loads `client_ca_tls` only for `VerifyClientCertIfGiven` and `RequireAndVerifyClientCert`.

Certificate status and reload operations are described in [TLS certificate management](/docs/cli/x509.md).

## kTLS transmit offload

On Linux, HTTPS targets use best-effort kernel TLS transmit (`kTLS_TX`) on their public listeners. Once a connection is armed, AIS gives plaintext response bytes to the TCP socket and the kernel emits encrypted TLS records. Eligible warm, monolithic object GETs can then use `sendfile(2)` without copying the object through userspace.

Starting v5.2, this capability is on by default. To disable it, set the `Disable-KTLS` feature flag (reserved for internal use) and restart the targets:

```console
$ ais config cluster features Disable-KTLS
```

Feature configuration is a complete list: include any other enabled feature names in the same command. The flag's name and meaning may change in a later release; do not depend on it in deployment automation.

> In v5.1, the same bit was named `System-Reserved-KTLS` and _enabled_ kTLS. A cluster that had it set runs with kTLS disabled after upgrading to v5.2; to re-enable, remove `Disable-KTLS` from the feature list and restart the targets.

The current scope and compatibility constraints are:

- Linux target public listeners only; proxies and intra-cluster listeners are unchanged.
- TLS 1.2 and TLS 1.3 AES-GCM cipher suites are supported. Unsupported kernels, TLS versions, and cipher suites continue with Go userspace TLS.
- Public target connections use HTTP/1.1 and have TLS session tickets disabled unless kTLS is disabled. These constraints apply even when a particular connection cannot arm kTLS and falls back to userspace TLS.
- kTLS applies to the entire transmit side of an armed connection. `sendfile(2)` remains limited to eligible local-file object responses; transformed, chunked, and other generated responses keep their normal userspace read path.
- AIS retires long-lived connections near the per-key transmit limit; clients must tolerate normal HTTP connection closure and reconnection.
- Fixed-size socket buffers limit transmit throughput. Any `net.l4.sndrcv_buf_size` other than `-1` (kernel auto-tuning) pins the buffers, including `0` (the 128KiB default). See [Socket Buffers](/docs/relnotes/5.1.md#socket-buffers).

As a quick host check, run [scripts/check-ktls.sh](https://github.com/NVIDIA/aistore/blob/main/scripts/check-ktls.sh). This is necessary but not sufficient: the kernel must also support the negotiated TLS version and AES-GCM cipher. At AIS verbosity level 5, target logs report whether connections were armed, unsupported, skipped, not established, or failed.

## Further references

- [HTTPS-related environment variables](/docs/environment-vars.md#https)
- [Managing TLS certificates](/docs/cli/x509.md)
- [Switching cluster between HTTP and HTTPS](/docs/switch_https.md)
