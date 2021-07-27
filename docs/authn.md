## Table of Contents

- [Overview](#overview)
- [Getting started](#getting-started)
	- [Notation](#notation)
	- [AuthN configuration and log](#authn-configuration-and-log)
	- [How to enable AuthN server after deployment](#how-to-enable-authn-server-after-deployment)
	- [Using Kubernetes secrets](#using-kubernetes-secrets)
- [REST API](#rest-api)
	- [Authorization](#authorization)
	- [Tokens](#tokens)
	- [Clusters](#clusters)
	- [Roles](#roles)
	- [Users](#users)
	- [Configuration](#configuration)
- [AuthN server typical workflow](#authn-server-typical-workflow)
- [Known limitations](#known-limitations)

## Overview

AIStore Authentication Server (AuthN) provides token-based secure access to AIStore.
It employs the [JSON Web Tokens](https://github.com/form3tech-oss/jwt-go) framework to grant access to resources: buckets and objects.
Please read a short [introduction to JWT](https://jwt.io/introduction/) for details.
Currently, we only support hash-based message authentication (HMAC) using SHA256 hash.

AuthN is a standalone server that manages users and tokens. If AuthN is enabled on a cluster,
a client must request a token from AuthN and put it into HTTP headers of every request to the cluster.
Requests without tokens are rejected.

A typical workflow looks as follows:

<img src="images/authn_flow.png" alt="AuthN workflow">

AuthN generates self-sufficient tokens: a proxy does not need access to AuthN to check permissions.
Though, for security reasons, clusters should be registered at AuthN server.
AuthN broadcasts revoked tokens to all registered clusters, so they updated their blacklists.

A workflow for the case when a token is revoked and only one cluster is registered.
"AIS Cluster 2" is unregistered and allows requests with revoked token:

<img src="images/token_revoke.png" alt="Revoke token workflow">

AuthN supports both HTTP and HTTPS protocols. By default, AuthN starts as an HTTP server listening on port 52001.
If you enable HTTPS access, make sure that the configuration file options `server_crt` and `server_key` point to the correct SSL certificate and key.


## Getting started

Environment variables used by the deployment script to setup AuthN server:

| Variable | Default value | Description |
|---|---|---|
| AIS_SECRET_KEY | `aBitLongSecretKey` | A secret key to sign tokens |
| AUTH_ENABLED | `false` | Set it to `true` to enable AuthN server and token-based access in AIStore proxy |
| AUTHN_PORT | `52001` | Port on which AuthN listens to requests |
| AUTHN_TTL | `24h` | A token expiration time. Can be set to 0 which means "no expiration time" |

All variables can be set at AIStore cluster deployment.
Example of starting a cluster with AuthN enabled:

```console
$ AUTH_ENABLED=true make deploy
```

Note: don't forget to change the default secret key used to sign tokens before starting the deployment process.

To change AuthN settings after deployment, modify the server's configuration file and restart the server.
If you change the server's secret key, make sure to modify AIStore proxy configuration as well.

At start, AuthN checks the user list. If it is empty, AuthN creates a default user that can access everything:
user ID is `admin` and password is `admin`. Do not forget to change the user's password for security reasons.

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

By default, AIStore deployment currently does not launch the AuthN server.
To start AuthN manually, perform the following steps:

- Start authn server: <path_to_ais_binaries>/authn -config=<path_to_config_dir>/authn.json. Path to config directory is set at the time of cluster deployment and it is the same as the directory for AIStore proxies and targets
- Update AIS CLI configuration file: change AuthN URL. Alternatively, prepend AuthN URL to every CLI command that uses `auth` subcommand: `AUTHN_URL=http://10.10.1.190:52001 ais auth COMMAND`
- Change AIStore cluster configuration to enable token-based access and use the same secret as AuthN uses:

```console
$ # Change the secret
$ ais config cluster auth.secret SECRET

$ # Enable cluster-wide authorization
$ ais config cluster auth.enabled true

$ # Register the cluster at AuthN to receive AuthN messages (e.g, revoked token list)
$ # ais auth add cluster CLUSTER_ALIAS CLUSTER-URL-LIST
$ ais auth add cluster mainCluster http://10.10.1.70:50001 http://10.10.1.71:50001

$ # Calling AuthN without modifying CLI configuration
$ # Assuming AuthN listens at http://10.10.1.190:52001
$ AUTHN_URL=http://10.10.1.190:52001 ais auth add cluster mainCluster http://10.10.1.70:50001 http://10.10.1.71:50001
```

### Using Kubernetes secrets

To increase security, a secret key for token generation can be
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

When AuthN pod starts, it loads its configuration from the local file, and then
overrides secret values with ones from the pod's description.

## REST API

### Authorization

After deploying the cluster, a superuser role `Admin` and `admin` account are created automatically.
Only users with `Admin` role can manage AuthN. Every request to AuthN(except login one) must
contain authentication token in the header:

```
Authorization: Bearer <token-issued-by-AuthN-after-login>
```

For curl, it is an argument `-H 'Authorization: Bearer token'`.

### Tokens

AIStore proxies and targets require a valid token in a request header - but only if AuthN is enabled.
Every token includes all the information needed by a node:

- User name
- User ACL
- Time when the token expires

A proxy validates the token. The token must not be expired and it must not be on the blacklist (the list of revoked tokens).
AuthN broadcasts the revoked token when it is added to AuthN.
When AuthN registers a new cluster, it sends to the cluster the entire list of revoked tokens.
Periodically the list is cleaned up: expired and invalid tokens are removed.

Generating a token for data access requires correct user name and password.
Token expiration time is 24 hours by default.
Modify `expiration_time` in the configuration file to change default expiration time.

To issue single token with custom expiration time, pass optional expiration duration in the request.
Example: generate a token that expires in 5 hours. API:

```
POST {"password": "password", "expires_in": 18000000000000} /v1/users/usename
````

CLI:

```console
$ ais auth login -p password usename -e 5h
```

Pass a zero value `"expires_in": 0` to generate a never-expired token.

AuthN return the generated token in as a JSON formatted message. Example: `{"token": "issued_token"}`.

Call revoke token API to forcefully invalidate a token before it expires.

| Operation | HTTP Action | Example |
|---|---|---|
| Generate a token for a user (Log in) | POST {"password": "pass"} /v1/users/username | curl -X POST AUTHSRV/v1/users/username -d '{"password":"pass"}' -H 'Content-Type: application/json' |
| Revoke a token | DEL { "token": "issued_token" } /v1/tokens | curl -X DEL AUTHSRV/v1/tokens -d '{"token":"issued_token"}' -H 'Content-Type: application/json' |

### Clusters

When a cluster is registered, an arbitrary alias can be assigned for the cluster.
CLI supports both cluster's ID and cluster's alias in commands.
The alias is used to create default roles for a just registered cluster.
If a cluster does not have an alias, the role names contain cluster ID.

| Operation | HTTP Action | Example |
|---|---|---|
| Get a list of registered clusters | GET /v1/clusters | curl -X GET AUTHSRV/v1/clusters |
| Get a registered cluster info | GET /v1/clusters/cluster-id | curl -X GET AUTHSRV/v1/clusters/cluster-id |
| Register a cluster | POST /v1/clusters {"id": "cluster-id", "alias": "cluster-alias", "urls": ["http://CLUSTERIP:PORT"]}| curl -X POST AUTHSRV/v1/clusters -d '{"id": "cluster-id", "alias": "cluster-alias", "urls": ["http://CLUSTERIP:PORT"]}' -H 'Content-Type: application/json' |
| Update a registered cluster | PUT /v1/clusters/id {"alias": "cluster-alias", "urls": ["http://CLUSTERIP:PORT"]}| curl -X PUT AUTHSRV/v1/clusters/id -d '{"alias": "cluster-alias", "urls": ["http://CLUSTERIP:PORT"]}' -H 'Content-Type: application/json' |
| Delete a registered cluster | DELETE /v1/clusters/cluster-id | curl -X DELETE AUTHSRV/v1/clusters/cluster-id |

### Roles

| Operation | HTTP Action | Example |
|---|---|---|
| Get a list of roles | GET /v1/roles | curl -X GET AUTHSRV/v1/roles |
| Get a role | GET /v1/roles/ROLE_ID | curl -X GET AUTHSRV/v1/roles/ROLE_ID |
| Create a new role | POST /v1/roles {"name": "rolename", "desc": "description", "clusters": ["clusterid": permissions]} | curl -X AUTHSRV/v1/roles '{"name": "rolename", "desc": "description", "clusters": ["clusterid": permissions]}' |
| Update an existing role | PUT /v1/roles/role-name {"desc": "description", "clusters": ["clusterid": permissions]} | curl -X PUT AUTHSRV/v1/roles '{"desc": "description", "clusters": ["clusterid": permissions]}' |
| Delete a role | DELETE /v1/roles/role-name | curl -X DELETE AUTHSRV/v1/roles/role-name |

### Users

| Operation | HTTP Action | Example |
|---|---|---|
| Get a list of users | GET /v1/users | curl -X GET AUTHSRV/v1/users |
| Get a users | GET /v1/users/USER_ID | curl -X GET AUTHSRV/v1/users/USER_ID |
| Add a user | POST {"id": "username", "password": "pass", "roles": ["CluOne-owner", "CluTwo-readonly"]} /v1/users | curl -X POST AUTHSRV/v1/users -d '{"id": "username", "password":"pass", "roles": ["CluOne-owner", "CluTwo-readonly"]}' -H 'Content-Type: application/json' |
| Update an existing user| PUT {"password": "pass", "roles": ["CluOne-owner", "CluTwo-readonly"]} /v1/users/user-id | curl -X PUT AUTHSRV/v1/users/user-id -d '{"password":"pass", "roles": ["CluOne-owner", "CluTwo-readonly"]}' -H 'Content-Type: application/json' |
| Delete a user | DELETE /v1/users/username | curl -X DELETE AUTHSRV/v1/users/username |

### Configuration

| Operation | HTTP Action | Example |
|---|---|---|
| Get AuthN configuration | GET /v1/daemon | curl -X GET AUTHSRV/v1/daemon |
| Update AuthN configuration | PUT /v1/daemon { "auth": { "secret": "new_secret", "expiration_time": "24h"}}  | curl -X PUT AUTHSRV/v1/daemon -d '{"auth": {"secret": "new_secret"}}' -H 'Content-Type: application/json' |

## AuthN server typical workflow

If the AuthN server is enabled, all requests to buckets and objects must contain a valid token issued by AuthN.
Requests without a token are rejected.

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
3. The user adds the token to every AIStore request (list buckets names example)

```console
$ curl -L  http://PROXY/v1/buckets/* -X GET \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer eyJhbGciOiJI.eyJjcmVkcyI.T6r6790"

{
  "ais": [ "train-set-001", "train-set-002" ]
  "gcp": [ "image-net-set-1" ],
}
```

## Known limitations

- **Per-bucket authentication**. It is currently impossible to limit user access to only a certain bucket, or buckets. Once users with read-write access log in, they have full access to all cluster's buckets.
