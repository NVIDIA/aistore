## Table of Contents

- [Overview](#overview)
- [Getting started](#getting-started)
	- [Notation](#notation)
	- [AuthN configuration and log](#authn-configuration-and-log)
	- [How to enable AuthN server after deployment](#how-to-enable-authn-server-after-deployment)
	- [Using Kubernetes secrets](#using-kubernetes-secrets)
- [User management](#user-management)
	- [Superuser](#superuser)
	- [REST operations](#rest-operations)
- [Token management](#token-management)
	- [REST operations](#rest-operations-1)
- [Interaction with AIStore proxy/gateway](#interaction-with-aistore-proxygateway)
	- [Calling AIStore proxy API](#calling-aistore-proxy-api)
	- [AuthN server typical workflow](#authn-server-typical-workflow)
- [Known limitations](#known-limitations)

## Overview

AIStore Authentication Server (AuthN) provides a token-based secure access to AIStore. It employs [JSON Web Tokens](https://github.com/dgrijalva/jwt-go) framework to grant access to resources: buckets and objects. Please read a short [introduction to JWT](https://jwt.io/introduction/) for details. Currently, we only support hash-based message authentication (HMAC) using SHA256 hash.

AuthN is a standalone process that manages users and their tokens. It reports to registered clusters all the changes on the fly.
It results in that the clusters always have up-to-date information, e.g., list of revoked tokens.

For a client, a typical workflow looks as follows:

<img src="images/authn_flow.gif" alt="Authn workflow">

AuthN supports both HTTP and HTTPS protocols. By default, AuthN starts as HTTP server listening on port 52001. If you enable HTTPS access make sure that the configuration file options `server_cert` and `server_key` point to the correct SSL certificate and key.


## Getting started

Environment variables used by the deployment script to setup AuthN server:

| Variable | Default value | Description |
|---|---|---|
| AIS_ENDPOINT | `""` | Primary proxy URL for AuthN to notify the cluster about events like a token is revoked |
| AIS_SECRET_KEY | `aBitLongSecretKey` | A secret key to encrypt and decrypt tokens |
| AUTH_ENABLED | `false` | Set it to `true` to enable AuthN server and token-based access in AIStore proxy |
| AUTHN_PORT | `52001` | Port on which AuthN listens to requests |
| AUTHN_TTL | `24h` | A token expiration time. Can be set to 0 that means "no expiration time" |

All variables can be set at AIStore launch. Example of starting AuthN with the default configuration:

```console
$ AUTH_ENABLED=true make deploy
```

Note: don't forget to change the default secret key used to encrypt and decrypt tokens before starting the deployment process.

To change AuthN settings after deployment, modify the server's configuration file and restart the server. If you change the server's secret key make sure to modify AIStore proxy configuration as well.

At start AuthN checks user list. If it is empty, AuthN creates a default user that can access to everything: user ID is `admin` and password is `admin`. Do not forget to change the user's password for security reasons.

### Notation

In this README:

> `AUTHSRV` - denotes a (hostname:port) address of a deployed AuthN server

> `PROXY` - (hostname:port) of a **gateway**(any gateway in a given AIS cluster)

### AuthN configuration and log

| File                 | Location                     |
|----------------------|------------------------------|
| Server configuration | `$AUTHN_CONF_DIR/authn.json` |
| User database        | `$AUTHN_CONF_DIR/authn.db`   |
| Log directory        | `$AIS_LOG_DIR/authn/log/`    |

### How to enable AuthN server after deployment

By default, AIStore deployment currently won't launch the AuthN server. To start AuthN, perform the following steps:

- Change AIStore proxy configuration to enable token-based access: look for `{"auth": { "enabled": false } }` in proxy configuration file and replace `false` with `true`. Restart the proxy to apply changes
- Start authn server: <path_to_ais_binaries>/authn -config=<path_to_config_dir>/authn.json. Path to config directory is set at the time of cluster deployment and it is the same as the directory for AIStore proxies and targets

### Using Kubernetes secrets

To increase security, secret key for token generation can be
put to [Kubernetes secrets](https://kubernetes.io/docs/concepts/configuration/secret/)
instead of keeping them in an AuthN configuration file. When secrets are used, AuthN
overrides configuration values with environment variables set by Kubernetes.

Add secrets to AuthN pod description:

```
apiVersion: v1
kind: Pod
metadata:
  name: secret-env-pod
spec:
  containers:
  - name: container-name
        image: image-name
        env:
          - name: SECRETKEY
            valueFrom:
              secretKeyRef:
                name: mysecret
                key: secret-key
```

In the example above the values in all-capitals are the names of the environment
variables that AuthN looks for. All other values are arbitrary.

When AuthN pod starts, it loads its configuration from local file, and then
overrides secret values with ones from the pod's description.

## User management

### Superuser

After deploying the cluster, a superuser account is created automatically - a special account that cannot be deleted and only this account can manage users and their credentials.

Adding and deleting usernames requires superuser authentication. Super user credentials are sent in the request header via `Authorization` field (for curl it is `curl -u<username>:<password ...`, for HTTP requests it is header option `Authorization: Basic <base64-encoded-username:password>`).

### REST operations

| Operation | HTTP Action | Example |
|---|---|---|
| Add a user| POST {"name": "username", "password": "pass"} /v1/users | curl -X POST http://AUTHSRV/v1/users -d '{"name": "username","password":"pass"}' -H 'Content-Type: application/json' -uadmin:admin |
| Delete a user | DELETE /v1/users/username | curl -X DELETE http://AUTHSRV/v1/users/username -uadmin:admin |

## Token management

Generating a token for data access does not require superuser credentials. Users must provide correct their username and password to get their tokens. Token has expiration time that is 24 hours by default. After that, the token must be reissued. To change default expiration time, look for `expiration_time` in the configuration file.

Call revoke token API to forcefully invalidate a token before it expires.

### REST operations

| Operation | HTTP Action | Example |
|---|---|---|
| Generate a token for a user (Log in) | POST {"password": "pass"} /v1/users/username | curl -X POST http://AUTHSRV/v1/users/username -d '{"password":"pass"}' -H 'Content-Type: application/json' |
| Revoke a token (Log out) | DEL { "token": "issued_token" } /v1/tokens | curl -X DEL http://AUTHSRV/v1/tokens -d '{"token":"issued_token"}' -H 'Content-Type: application/json' |

A generated token is returned as a JSON formatted message. Example: `{"token": "issued_token"}`.

## Interaction with AIStore proxy/gateway

AIStore proxies and targets require a valid token in a request header - but only if AuthN is enabled. Every token includes all the information needed by the target:

- UserID (username)
- User ACL
- Time when the token expires

A token is validated by a target. The token must not be expired and it must not be in the blacklist. The blacklist is a list of revoked tokens: tokens that are revoked with REST API or belong to deleted users. The list of revoked tokens is broadcast over the cluster on change. Periodically the list is cleaned up by removing expired tokens.

### Calling AIStore proxy API

If authentication is enabled a REST API call to the AIStore proxy must include a valid token issued by the AuthN. The token is passed in the request header in the format: `Authorization: Bearer <token>`. Curl example: `curl -L  http://PROXY/v1/buckets/* -X GET -H 'Content-Type: application/json' -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIs'`.

At this moment, only requests to buckets and objects API require a token.

### AuthN server typical workflow

If the AuthN server is enabled then all requests to buckets and objects should contain a valid token issued by AuthN. Requests without a token are rejected unless the AIS cluster is deployed with guest support enabled. In this case GET, HEAD, and list objects requests do not require authorization - anyone can read the data but modifications are allowed only for registered users.

Steps to generate and use a token:

1. Superuser creates a user account

```console
$ curl -X POST http://AUTHSRV/v1/users \
  -d '{"name": "username", "password": "pass"}' \
  -H 'Content-Type: application/json' -uadmin:admin
```

2. The user requests a token

```console
$ curl -X POST http://AUTHSRV/v1/users/username \
  -d '{"password": "pass"}' -H 'Content-Type: application/json'

{"token": "eyJhbGciOiJI.eyJjcmVkcyI.T6r6790"}
```
3. The user adds the token for every AIStore request (list buckets names example)

```console
$ curl -L  http://PROXY/v1/buckets/* -X GET \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer eyJhbGciOiJI.eyJjcmVkcyI.T6r6790"

{
  "cloud": [ "image-net-set-1" ],
  "ais": [ "train-set-001", "train-set-002" ]
}
```

## Known limitations

- **Per-bucket authentication**. It is currently not possible to limit user access to only a certain bucket, or buckets. Once users log in, they have full access to all the data;
