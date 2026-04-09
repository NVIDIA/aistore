---
layout: post
title: "Eliminating Cluster Authentication Risks: AIStore with RSA and OIDC Issuer Discovery"
date: Apr 9, 2026
author: Aaron Wilson
categories: 
  - aistore
  - authn 
  - security 
  - authentication
---

Back in February 1997, [RFC 2104](https://datatracker.ietf.org/doc/html/rfc2104) introduced HMAC as a mechanism for authenticating messages based on a shared secret key.

Symmetric signing algorithms like HMAC __can__ be used to securely sign access tokens, but with two extremely important caveats:
1. The secret key must be strong enough to avoid simple brute-force attacks (high entropy, sufficiently long, and randomly generated)
2. The secret key must NEVER be leaked

Unfortunately for the first point, [hashcat](https://hashcat.net/hashcat/) received its public release in 2009.
Since then, advances in GPU hardware and frameworks like [NVIDIA CUDA](https://developer.nvidia.com/cuda) have turned tools such as hashcat into [increasingly effective](https://chiomaibeakanma.hashnode.dev/exploiting-weak-jwt-hmac-secrets-from-account-takeover-to-admin-privilege-escalation) brute-force engines, capable of breaking secrets that were once considered safe.
Still, given a sufficiently long and random key, this is [not a concern](https://specopssoft.com/blog/sha256-hashing-password-cracking/) with `HMAC-SHA256`.

The second issue is a much larger problem for symmetric signing approaches like HMAC.
Since the signing key is also used for validation, it must be provided to the server, not just the token issuer. 
And this secret key must never be accidentally exposed in deployment pipelines, configuration files, or logs.
This increased attack surface is a massive risk!

What's worse, a compromised signing key gives no indication to server owners. 
With no key rotation, a stolen key can be used to sign tokens with ANY level of access indefinitely. 
Attackers can use this key to quietly read or corrupt sensitive data without revealing their access.
For any AIStore deployments that are not carefully gated in a protected environment, this could spell disaster.

With the 4.3 and subsequent 4.4 releases, AIStore AuthN now supports RSA signing keys and OIDC Issuer Discovery -- two essential features to mitigate the risk of this total security collapse.

---

## Table of Contents

- [RSA JWT Signing](#rsa-jwt-signing)
- [OIDC Issuer Discovery](#oidc-issuer-discovery)
  - [Static Key Distribution](#static-key-distribution)
  - [Trusted Issuers](#trusted-issuers)
  - [OIDC in AuthN](#oidc-in-authn)
  - [Drawbacks and Limitations](#drawbacks-and-limitations)
- [Complete Kubernetes Deployment](#complete-kubernetes-deployment)
  - [Running the Deployment](#running-the-deployment)
  - [AuthN Config](#authn-config)
  - [AIS Config](#ais-config)
- [Conclusion and Future Work](#conclusion-and-future-work)
  - [Signing Key Rotation](#signing-key-rotation)
  - [Multi-replica Support](#multi-replica-support)
  - [Service Account Authentication](#service-account-authentication)
- [References](#references)

---

## RSA JWT Signing

Previously, AIStore AuthN relied on HS256, which uses HMAC-SHA256 with a shared secret key.
This is a symmetric algorithm, where the same secret is used for both signing [JWTs](https://datatracker.ietf.org/doc/html/rfc7519) and validating them. 

This meant the signing key was distributed and could potentially exist in files, K8s secrets, K8s Pod specs, or environment variables in the actual AIS deployment. 

We needed to be able to distribute a key publicly without exposing the ability to sign new tokens.
That's where __asymmetric__ RSA signing key pairs come into the picture. 
With RSA, the private key never leaves the AuthN service. 
JWT signatures are validated only by a public key that cannot be used to sign new tokens.

AuthN also now supports encrypting the private key locally with a passphrase so it's never unprotected on disk even within the service.

See [RSA Signing](https://github.com/NVIDIA/aistore/blob/main/docs/authn.md#rsa-signing) in the AuthN docs for more details.

---

## OIDC Issuer Discovery

### Static Key Distribution

Even with the improved security of RSA keys, relying on static key distribution presents challenges.

First, this still doesn't fully address the issue of compromised keys. 
Private key leaks are less likely, as they are never distributed, but we still risk silent exposure.
Without key rotation, a compromised private key can be used to mint fraudulent JWTs indefinitely.
And by using a static public key in AIS config, we can't simply rotate the validation key in AIS without invalidating all existing tokens. 

The static config also adds friction to deployment, since AuthN generates the key pair. 
Any AIS cluster deployment would need to inject the generated public key into its config. 

### Trusted Issuers

OIDC issuer lookup solves all of this by validating JWTs with a cached set of keys from trusted issuers. 
Instead of checking a JWT signature with a static public key, AIS uses the `iss` and `kid` claims from the JWT to look up the associated public key.

AIS itself has supported the concept of [OIDC issuer discovery](https://github.com/NVIDIA/aistore/blob/main/docs/auth_validation.md#oidc-lookup) since version 4.1, but this was restricted to 3rd-party JWT issuers, which needed additional configuration to support the custom JWT format for AIS access.

This update brings that functionality to the native AIStore AuthN service, offering much better security and simplified deployment compared to the previous approach of symmetric, static signing keys.

### OIDC in AuthN

AuthN does NOT fully implement the [OIDC spec](https://openid.net/specs/openid-connect-core-1_0.html). 
It simply exposes the path `/.well-known/openid-configuration`, which responds with a "discovery document" containing `jwks_uri`. 
That `jwks_uri` path then returns the complete set of valid public [JSON Web Keys (JWK)](https://datatracker.ietf.org/doc/html/rfc7517). 
A JWK is a generic JSON container for different key types.
In the case of AuthN, it represents an encoded RSA public key with some extra metadata. 

This JWK set (JWKS) is then cached on the AIStore proxies, where the keys are used to validate JWT signatures.

Below is a diagram showing the full flow; see the [AuthN docs](https://github.com/NVIDIA/aistore/blob/main/docs/authn.md#oidc-issuer) for more implementation details.

![OIDC Issuer flow](/assets/rsa_and_oidc/OIDC_issuer.png)

### Drawbacks and Limitations

One disadvantage is that previously, AIS had no dependency on the availability of the AuthN service. 
Now, AIS expects AuthN to be reachable for updating the local cache of key sets on a regular basis, increasing the requirement for AuthN reliability.
[Deploying in K8s](https://github.com/NVIDIA/ais-k8s/tree/main/helm/authn) simplifies this, but multi-replica support for AuthN requires ongoing work (see [future work](#conclusion-and-future-work)).

However, AIS will not need to query AuthN on every request, and in fact caches the key sets intelligently thanks to the [JWX library](https://github.com/lestrrat-go/jwx). 

> Note: AIS currently only refreshes its cached key sets for a specific issuer on proxy restart. 
> This is a known deficiency that limits the usability of key rotation and will be fixed in a future release.
> See the [signing key rotation](#signing-key-rotation) section below. 

---

## Complete Kubernetes Deployment

With RSA signing and OIDC discovery, the signing key is no longer shared, keys can be rotated without touching AIS config, and AIStore and AuthN can be deployed in any order without pre-distributing keys.

To demonstrate, we'll show a local AIS cluster deployed in K8s alongside AuthN, runnable in K8s KinD via a single script.

See the full deployment [scripts on the ais-k8s repo](https://github.com/NVIDIA/ais-k8s/tree/main/local).

### Running the Deployment

See the [guide in ais-k8s](https://github.com/NVIDIA/ais-k8s/blob/main/local/README.md) for full details.
First, you'll need a few prerequisites:

- [Docker](https://www.docker.com/) or [Podman](https://podman.io/)
- [Kubernetes in Docker](https://kind.sigs.k8s.io/)
- [kubectl](https://kubernetes.io/docs/reference/kubectl/)
- [Helm](https://helm.sh/docs/intro/install/)
- [Helmfile](https://github.com/helmfile/helmfile#installation)

Next, to create the local deployment, clone [ais-k8s](https://github.com/NVIDIA/ais-k8s) and navigate to `local`.

Then run `./test-cluster.sh --auth`. 

That's it! 
The script will bootstrap a local K8s cluster with all dependencies and an entire stack for AIS: K8s operator, AIS cluster, AIS AuthN, and an admin client deployment.

Once deployed, run the following to drop into a shell on the admin client pod inside the cluster: 

```bash
kubectl exec -it -n ais deploy/ais-client -- /bin/bash
```

Initially, the AIS CLI won't have access because AIS is enforcing authentication:

```bash
root@ais-client-7d869f99bf-dp76b:/# ais ls
Error: token required
```

This pod is pre-configured with environment variables for accessing the AuthN service. 
Run `ais auth login $AIS_AUTHN_USERNAME -p $AIS_AUTHN_PASSWORD` to fetch a token. 
Now the client in this pod has full admin access to the local cluster. 

```bash
root@ais-client-7d869f99bf-dp76b:/# ais auth login $AIS_AUTHN_USERNAME -p $AIS_AUTHN_PASSWORD
Logged in (/root/.config/ais/cli/auth.token)
# Successful request
root@ais-client-7d869f99bf-dp76b:/# ais ls
No buckets in the cluster.
```

Below is a simplified diagram showing the entire setup:

![K8s AuthN Deployment](/assets/rsa_and_oidc/k8s_authn.png)

### AuthN Config

In recent versions of AuthN, RSA is the default signing method and will auto-generate a key pair on initial startup. 

The relevant configuration for enabling OIDC lookup in the [AuthN local helm environment](https://github.com/NVIDIA/ais-k8s/blob/main/helm/authn/config/authn/local.yaml.gotmpl) is `net.ExternalURL`:

```yaml
net:
  externalURL: "https://ais-authn.ais.svc.cluster.local:52001"
```

This tells the AuthN service what to use when building the `jwks_uri` in the `openid-configuration` response.
The URL that clients can use to access the service depends on the deployment, so it must be configured in advance.

### AIS Config

Because the AIS cluster runs in the same local deployment, we can use the K8s service DNS to access AuthN directly. 

In the [local-auth helm values](https://github.com/NVIDIA/ais-k8s/blob/main/helm/ais/config/ais/local-auth.yaml) for AIS, we set `configToUpdate` to update the AIS internal configuration to trust JWTs signed by the given allowed issuer. 

The `auth` section configures how the operator and admin clients connect and provision an admin token by using credentials from a K8s secret. 

```yaml
# Configure AIS to trust JWTs issued by the local AuthN issuer
configToUpdate:
  auth:
    enabled: true
    # Instead of signature.key, we configure a list of issuers that we trust
    oidc:
      allowed_iss: ["https://ais-authn.ais.svc.cluster.local:52001"]

# Client AuthN API config used by operator and admin client
auth:
  serviceURL: "https://ais-authn.ais.svc.cluster.local:52001"
  # Currently, AuthN only supports username and password login to fetch tokens
  usernamePassword:
    secretName: ais-authn-su-creds
```
---

## Conclusion and Future Work

Moving towards RSA signing and OIDC issuer lookup is important for AuthN, but there's more we want to build: 

### Signing Key Rotation
    
Issuer lookup supports multiple active signing keys, which in theory allows for seamless rotation. 
Currently, AuthN keys can be rotated manually via `ais auth rotate-key`.
However, AIStore version 4.4 won't accept tokens signed by the new keys until the cached keyset is refreshed. 
This refresh is only triggered by the previous JWK's expiry date (currently unset by AuthN) or by a proxy restart.

Once live rotation is fully supported on the AIStore side, automated signing key rotation for AuthN is a natural progression.
Configurable intervals for automated rotation would eliminate the manual step and reduce the window of exposure if a key is compromised.

### Multi-replica Support

Since AIS now actively queries AuthN for key sets, a single-replica AuthN becomes a potential availability bottleneck.
Supporting multiple replicas would bring AuthN to production-grade availability.

This is a non-trivial development that requires more than a simple scale-up. 
AuthN currently uses [BuntDB](https://github.com/tidwall/buntdb) as its underlying storage. 
Support for distributed DBs will be required for multi-replica access. 
Signing keys must also be distributed and synchronized between instances to support consistency between multiple signers.

### Service Account Authentication

A current limitation is that the AIS operator requires K8s secrets for admin credentials to manage AuthN-enabled clusters.
One proposed alternative is to support AuthN token provisioning via a [K8s projected service account token](https://dev.to/piyushjajoo/understanding-kubernetes-projected-service-account-tokens-205f).
This moves the access control used for the operator and admin client deployments to K8s RBAC and away from static credentials.

Follow our progress on the [main AIStore repo](https://github.com/NVIDIA/aistore) or try out the [local deployment](https://github.com/NVIDIA/ais-k8s/tree/main/local) yourself!

---

## References

__AIStore Authentication__
- [AuthN Documentation](https://github.com/NVIDIA/aistore/blob/main/docs/authn.md)
- [AIS Auth Validation](https://github.com/NVIDIA/aistore/blob/main/docs/auth_validation.md)
- [AIS Auth CLI](https://github.com/NVIDIA/aistore/blob/main/docs/cli/auth.md)
- [ais-k8s Local Deployment](https://github.com/NVIDIA/ais-k8s/tree/main/local)
- [AuthN in K8s docs](https://github.com/NVIDIA/ais-k8s/blob/main/docs/authn.md)
- [AuthN in K8s Helm](https://github.com/NVIDIA/ais-k8s/tree/main/helm/authn)

__Standards and Specs__
- [OpenID Connect Core 1.0](https://openid.net/specs/openid-connect-core-1_0.html)
- [OpenID Connect Discovery 1.0](https://openid.net/specs/openid-connect-discovery-1_0.html)
- [HMAC (RFC 2104)](https://datatracker.ietf.org/doc/html/rfc2104)
- [JSON Web Token (RFC 7519)](https://datatracker.ietf.org/doc/html/rfc7519)
- [JSON Web Key (RFC 7517)](https://datatracker.ietf.org/doc/html/rfc7517)
- [JSON Web Algorithms (RFC 7518) -- RSA](https://datatracker.ietf.org/doc/html/rfc7518#section-3.3)

__Libraries and Tools__
- AIStore JWK caching library: [lestrrat-go/jwx](https://github.com/lestrrat-go/jwx)
- [Kubernetes in Docker (KinD)](https://kind.sigs.k8s.io/)
- AuthN storage: [BuntDB](https://github.com/tidwall/buntdb)

__General__
- [AIStore GitHub](https://github.com/NVIDIA/aistore)
- [AIStore Blog](https://aistore.nvidia.com/blog)

