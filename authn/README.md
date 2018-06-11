DFC Authentication Server (AuthN)
-----------------------------------------

## Overview

DFC Authentication Server provides a token-based security access to DFC REST API. It employs [JSON Web Tokens](https://github.com/dgrijalva/jwt-go) framework to grant access to resources: buckets and objects. Please read a short [introduction to JWT](https://jwt.io/introduction/) for details.

The server is a standalone application that manages users and their tokens. It reports to the primary DFC proxy all changes on the fly and immediately after each user login or logout. The result is that the DFC primary proxy (aka DFC gateway) always has updated set of valid tokens that grant access to the DFC and Cloud resources.

For a client application, a typical workflow looks as follows:

<img src="images/authn_flow.gif" alt="Authn workflow">

AuthN supports both HTTP and HTTPS protocols. By default, the server starts as HTTP server listening on port 8203. If you enable HTTPS access make sure that the configuration file options `server_cert` and `server_key` point to the correct SSL certificate and key.


## Getting started

Environment variables used by the deployment script to setup AuthN server:

| Variable | Default value | Description |
|---|---|---|
| AUTHENABLED | false | Set it to `true` to enable authn server and token-based access in DFC proxy |
| AUTH_SU_NAME | admin | Super user name (see `A super user` section for details) |
| AUTH_SU_PASS | admin | Super user password (see `A super user` section for details) |
| SECRETKEY| aBitLongSecretKey | A secret key to encrypt and decrypt tokens |
| CREDDIR | empty value | A path to directory to keep Google Storage user credentials |

All variables can be set at DFC launch. Example of starting AuthN with default configuration:

```
$ CREDDIR=/tmp/creddir AUTHENABLED=true make deploy
```

Note: before starting deployment process don't forget to change the default secret key used to encrypt and decrypt tokens.

To change AuthN settings after deployment, modify the server's configuration file and restart the server. If you change the server's secret key make sure to modify DFC proxy configuration as well.

### AuthN configuration and log

| File | Location |
|---|---|
| Server configuration | $CONFDIR/authn.json |
| User list | $CONFDIR/users.json |
| Log directory | $LOGDIR/authn/log/ |

### How to enable AuthN server after deployment

By default, DFC deployment currently won't launch AuthN server. To start AuthN, perform the following steps:

- Change DFC proxy configuration to enable token-based access: look for `{"auth": { "enabled": false } }` in proxy configration file and replace `false` with `true`. Restart the proxy to apply changes
- Start authn server: <path_to_dfc_binaries>/authn -config=<path_to_config_dir>/authn.json. Path to config directory is set at the time of cluster deployment and it is the same as the directory for DFC proxies and DFC targets

## User management

### Superuser

After deploying the cluster, there will be an automatically generated superuser account - a special account that cannot be deleted and that will be used exclusively to manage users and their credentials. This superuser account does not have access to buckets and objects.

Superuser's credentials can be set at cluster deployment time(please, see [Getting started](#getting-started)), or changed later by modifying AuthN configuration file `authn.json`. It is located in $CONFDIR, default value is $HOME/.dfc.

Adding and deleting usernames requires superuser authentication. Super user credentials are sent in the request header via `Authorization` field (for curl it is `curl -u<username>:<password ...`, for HTTP requesest it is header option `Authorization: Basic <base64-encoded-username:password>`).

### REST operations

| Operation | HTTP Action | Example |
|---|---|---|
| Add a user| POST {"name": "username", "password": "pass"} /v1/users | curl -X POST http://localhost:8203/v1/users -d '{"name": "username","password":"pass"}' -H 'Content-Type: application/json' -uadmin:admin |
| Delete a user | DELETE /v1/users/username | curl -X DELETE http://localhost:8203/v1/users/username -uadmin:admin |
| Add a cloud credentials for a user | PUT /v1/users/username/cloud-provider {data} | curl -X PUT -L -H 'Content-Type: application/json' http://localhost:8203/v1/users/username/aws -uadmin:admin -T ~/.aws/credentials |
| Remove user's cloud credentials | DELETE /v1/users/username/cloud-provider | curl -X DELETE -L http://localhost:8203/v1/users/username/aws -uadmin:admin |

## Token management

Generating a token for data access does not require superuser credentials. Users must provide correct their username and password to get their tokens. Token expires in 30 minutes. After that the token must be reissued. To change default expiration time, look for `expiration_time` in configuration file.

Call revoke token API to forcefully invalidate a token before it expires.

### REST operations

| Operation | HTTP Action | Example |
|---|---|---|
| Generate a token for a user (Log in) | POST {"password": "pass"} /v1/users/username | curl -X POST http://localhost:8203/v1/users/username -d '{"password":"pass"}' -H 'Content-Type: application/json' |
| Revoke a token (Log out) | DEL { "token": "issued_token" } /v1/tokens | curl -X DEL http://localhost:8203/v1/tokens -d '{"token":"issued_token"}' -H 'Content-Type: application/json' |

A generated token is returned as a JSON formatted message. Example: `{"token": "issued_token"}`.

## Interaction with DFC proxy/gateway

DFC proxies and targets require a valid token in a request header - but only if AuthN is enabled. Every token includes all the information needed by the target:

- UserID (username)
- Time when the the token was generated
- Time when the token expires
- User's AWS/GCP credentials

A token is validated by target. The token must not be expired and it must not be in black list. Black list is a list of revoked tokens: tokens that are revoked with REST API or belong to deleted users. List of revoked tokens is broadcast over the cluster on change. Periodically the list is cleaned up by removing expired tokens.

### Calling DFC proxy API

If authentication is enabled a REST API call to the DFC proxy must include a valid token issued by the AuthN. The token is passed in the request header in the format: `Authorization: Bearer <token>`. Curl example: `curl -L  http://localhost:8080/v1/buckets/* -X GET -H 'Content-Type: application/json' -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIs'`.

At this moment, only requests to buckets and objects API require a token.

### AuthN server typical workflow

If AuthN server is enabled then all requests to buckets and objects must contain a valid token issued by AuthN. Steps to generate and use a token:

1. Superuser creates a user account

```
$ curl -X POST http://localhost:8203/v1/users \
  -d '{"name": "username", "password": "pass"}' \
  -H 'Content-Type: application/json' -uadmin:admin
```
2. If the user needs access to AWS or GCP, the superuser add user's credentials (example of adding AWS credentials from file)

```
$ curl -X PUT -L -H 'Content-Type: application/json' \
  http://localhost:8203/v1/users/username/aws \
  -uadmin:admin -T ~/.aws/credentials
```
3. The user requests a token

```
$ curl -X POST http://localhost:8203/v1/users/username \
  -d '{"password": "pass"}' -H 'Content-Type: application/json'

{"token": "eyJhbGciOiJI.eyJjcmVkcyI.T6r6790"}

```
4. The user adds the token for every DFC request to a proxy or a target (list bucket example)

```
$ curl -L  http://localhost:8080/v1/buckets/bucketname -X GET \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer eyJhbGciOiJI.eyJjcmVkcyI.T6r6790" \
  -d '{"props": "size"}'

{"entries":
 [{"name": "obj1", "size": 1048576, "ctime": "", "checksum": "", "type": "",
   "atime":"", "bucket": "", "version": "", "iscached": false}
 ],
"pagemarker": ""}
```

## Known limitations

- **Per-bucket authentication**. It is currently not possible to limit user access to only a certain bucket, or buckets. Once users login, they have full access to all the data;
- **Token refresh**. There is no automatic token refreshing. By default a token expires in 30 minutes. So, if you are going to run something for longer time you should either add manual token refresh on getting 'No authorized' error or increase expiration time in settings.
