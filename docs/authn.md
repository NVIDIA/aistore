---
layout: post
title: AUTHN
permalink: /docs/authn
redirect_from:
 - /authn.md/
 - /docs/authn.md/
---

AIStore Authentication Server (**AuthN**) provides OAuth 2.0 compliant [JSON Web Tokens](https://datatracker.ietf.org/doc/html/rfc7519) based secure access to AIStore.

## AuthN Config

Note:

* AuthN configuration directory: `$HOME/.config/ais/authn`

The directory usually contains plain-text `authn.json` configuration and tokens DB `authn.db`

> For the most updated system filenames and configuration directories, please see [`fname/fname.go`](https://github.com/NVIDIA/aistore/blob/main/cmn/fname/fname.go) source.

Examples below use AuthN specific environment variables. Note that all of them are enumerated in:

* [`api/env/authn.go`](https://github.com/NVIDIA/aistore/blob/main/api/env/authn.go)

## Getting started with AuthN: local-playground session

The following brief and commented sequence assumes that [AIS local playground](getting_started.md#local-playground) is up and running.

```console
# 1. Login as administrator (and note that admin user and password can be only
#    provisioned at AuthN deployment time and can never change)
$ ais auth login admin -p admin
Token(/root/.config/ais/cli/auth.token):
...

# 2. Connect AIS cluster to *this* authentication server (note that a single AuthN can be shared my multiple AIS clusters)
#    List existing pre-defined roles
$ ais auth show role
ROLE    DESCRIPTION
Admin   AuthN administrator
$ ais auth add cluster myclu http://localhost:8080
$ ais auth show role
ROLE                    DESCRIPTION
Admin                   AuthN administrator
BucketOwner-myclu       Full access to buckets in WayZWN_f4[myclu]
ClusterOwner-myclu      Admin access to WayZWN_f4[myclu]
Guest-myclu             Read-only access to buckets in WayZWN_f4[myclu]

# 3. Create a bucket (to further demonstrate access permissions in action)
$ ais create ais://nnn
"ais://nnn" created (see https://github.com/NVIDIA/aistore/blob/main/docs/bucket.md#default-bucket-properties)
$ ais put README.md ais://nnn
PUT "README.md" to ais://nnn

4. Create a new role. A named role is, ultimately, a combination of access permissions
#  and a (user-friendly) description. A given role can be assigned to multiple users.
$ ais auth add role new-role myclu <TAB-TAB>
ADMIN                  DESTROY-BUCKET         HEAD-OBJECT            MOVE-OBJECT            ro
APPEND                 UPDATE-OBJECT          LIST-BUCKETS           PATCH                  rw
CREATE-BUCKET          GET                    LIST-OBJECTS           PROMOTE                SET-BUCKET-ACL
DELETE-OBJECT          HEAD-BUCKET            MOVE-BUCKET            PUT                    su

# Notice that <TAB-TAB> can be used to both list existing access permissions
# and
# complete those that you started to type.
$ ais auth add role new-role myclu --desc "this is description" LIST-BUCKETS LIST-OBJECTS GET HEAD-BUCKET HEAD-OBJECT ... <TAB-TAB>

# 5. Show users. Add a new user.
#    We can always utilize one of the prebuilt roles, e.g. `Guest-myclu` for read-only access.
#    But in this example we will use the newly added `new-role`:
$ ais auth show user
NAME    ROLES
admin   Admin

$ ais auth add user new-user -p 12345 new-role
$ ais auth show user
NAME    ROLES
admin   Admin
new-user Guest-myclu

# Not showing here is how to add a new role
# (that can be further conveniently used to grant subsets of permissions)

# 5. Login as `new-user` (added above) and save the token separately as `/tmp/new-user.token`
#    Note that by default the token for a logged-in user will be saved in the
#    $HOME/.config/ais/cli directory
#    (which is always checked if `AIS_AUTHN_TOKEN_FILE` environment is not specified)

$ ais auth login new-user -p 12345 -f /tmp/new-user.token
Token(/tmp/new-user.token):
...

# 6. Perform operations. Note that the `new-user` has a limited `Guest-myclu` access.
$ AIS_AUTHN_TOKEN_FILE=/tmp/new-user.token ais ls ais:
AIS Buckets (1)
  ais://nnn
$ AIS_AUTHN_TOKEN_FILE=/tmp/new-user.token ais ls ais://nnn
NAME             SIZE
README.md        8.96KiB

# However:
$ AIS_AUTHN_TOKEN_FILE=/tmp/new-user.token ais create ais://mmm
Failed to create "ais://mmm": insufficient permissions

$ AIS_AUTHN_TOKEN_FILE=/tmp/new-user.token ais put LICENSE ais://nnn
Insufficient permissions
```

Further references:

* See [CLI auth subcommand](/docs/cli/auth.md) for all supported command line options and usage examples.

---------------------

## Table of Contents

- [Overview](#overview)
- [Environment and configuration](#environment-and-configuration)
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
- [Typical workflow](#typical-workflow)
- [Known limitations](#known-limitations)

## Overview

AIStore Authentication Server (AuthN) provides OAuth 2.0 compliant [JSON Web Tokens](https://datatracker.ietf.org/doc/html/rfc7519) based secure access to AIStore.

* [Brief introduction to JWT](https://jwt.io/introduction/)
* [Go (language) implementation of JSON Web Tokens](https://github.com/golang-jwt/jwt) that we utilize for AuthN.

Currently, we only support hash-based message authentication (HMAC) using SHA256 hash.

AuthN is a standalone server that manages users and tokens. If AuthN is enabled on a cluster,
a client must request a token from AuthN and put it into HTTP headers of every request to the cluster.
Requests without tokens are rejected.

A typical workflow looks as follows:

![AuthN workflow](images/authn_flow.png)

AuthN generates self-sufficient tokens: a proxy does not need access to AuthN to check permissions.
Though, for security reasons, clusters should be registered at AuthN server.
AuthN broadcasts revoked tokens to all registered clusters, so they updated their blacklists.

A workflow for the case when a token is revoked and only one cluster is registered.
"AIS Cluster 2" is unregistered and allows requests with revoked token:

![Revoke token workflow](images/token_revoke.png)

AuthN supports both HTTP and HTTPS protocols. By default, AuthN starts as an HTTP server listening on port 52001.
If you enable HTTPS access, make sure that the configuration file options `server_crt` and `server_key` point to the correct SSL certificate and key.

## Environment and configuration

Environment variables used by the deployment script to setup AuthN server:

| Variable | Default value | Description |
|---|---|---|
| AIS_SECRET_KEY | `aBitLongSecretKey` | A secret key to sign tokens |
| AIS_AUTHN_ENABLED | `false` | Set it to `true` to enable AuthN server and token-based access in AIStore proxy |
| AIS_AUTHN_PORT | `52001` | Port on which AuthN listens to requests |
| AIS_AUTHN_TTL | `24h` | A token expiration time. Can be set to 0 which means "no expiration time" |
| AIS_AUTHN_USE_HTTPS | `false` | Enable HTTPS for AuthN server. If `true`, AuthN server requires also `AIS_SERVER_CRT` and `AIS_SERVER_KEY` to be set |
| AIS_SERVER_CRT | ` ` | OpenSSL certificate. Optional: set it only when secure HTTP is enabled |
| AIS_SERVER_KEY | ` ` | OpenSSL key. Optional: set it only when secure HTTP is enabled |

All variables can be set at AIStore cluster deployment.
Example of starting a cluster with AuthN enabled:

```console
$ AIS_AUTHN_ENABLED=true make deploy
```

Note: don't forget to change the default secret key used to sign tokens before starting the deployment process.

To change AuthN settings after deployment, modify the server's configuration file and restart the server.
If you change the server's secret key, make sure to modify AIStore proxy configuration as well.

Upon startup, AuthN checks the user list. If it is empty, AuthN creates a default user that can access everything:
user ID is `admin` and password is `admin`. Do not forget to change the user's password for security reasons.

### Notation

In this README:

> `AUTHSRV` - denotes a (hostname:port) address of a deployed AuthN server

> `PROXY` - (hostname:port) of a **gateway**(any gateway in a given AIS cluster)

### AuthN configuration and log

| File                 | Location                     |
|----------------------|------------------------------|
| Server configuration | `$AIS_AUTHN_CONF_DIR/authn.json` |
| User database        | `$AIS_AUTHN_CONF_DIR/authn.db`   |
| Log directory        | `$AIS_LOG_DIR/authn/log/`    |

Note: when AuthN is running, execute `ais auth show config` to find out the current location of all AuthN files.

### How to enable AuthN server after deployment

By default, AIStore deployment currently does not launch the AuthN server.
To start AuthN manually, perform the following steps:

- Start authn server: <path_to_ais_binaries>/authn -config=<path_to_config_dir>/authn.json. Path to config directory is set at the time of cluster deployment and it is the same as the directory for AIStore proxies and targets
- Update AIS CLI configuration file: change AuthN URL. Alternatively, prepend AuthN URL to every CLI command that uses `auth` subcommand: `AIS_AUTHN_URL=http://10.10.1.190:52001 ais auth COMMAND`
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
$ AIS_AUTHN_URL=http://10.10.1.190:52001 ais auth add cluster mainCluster http://10.10.1.70:50001 http://10.10.1.71:50001
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

AIStore gateways and targets require a valid token in a request header - but only if AuthN is enabled.

Every token includes the following information (needed to enforce access permissions):

- User name
- User ACL
- Time when the token expires

To pass all the checks, the token must not be expired or blacklisted (revoked).

#### Revoked tokens

AuthN makes sure that all AIS gateways are timely updated with each revoked token.
When AuthN registers a new cluster, it sends to the cluster the entire list of revoked tokens.
Periodically the list is cleaned up whereby expired and invalid tokens get removed.

#### Expired tokens
Generating a token for data access requires user name and user password.
By default, token expiration time is set to 24 hours.
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

## Typical workflow

When AuthN is enabled all requests to buckets and objects must contain a valid token (issued by the AuthN).
Requests without a token will be rejected.

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
