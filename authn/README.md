DFC Authentication Server(authn)
-----------------------------------------

## Overview

An authentication server provides a token-based security access to DFC REST API. It employs [JSON Web Tokens](https://github.com/dgrijalva/jwt-go) framework to grant access for users to resources like buckets or objects. Please read a short [introduction to JWT](https://jwt.io/introduction/) for details.

The server is a standalone application that manages users and their tokens granting access to the cluster. It reports to a primary DFC proxy all changes on the fly immediately after each user login or logout. It results in that the proxy always has up-to-dated information about users who have access to the data. A typical workflow for a client application to get access to cluster resources:

<img src="images/authn_flow.gif" alt="Authn workflow">

Authn supports both HTTP and HTTPS protocols. By default the server starts as HTTP server listening on port 8203. If you enable HTTPS access make sure that the configuration file options `server_cert` and `server_key` point to correct SSL certificate and key.


## Getting started

Authn server is installed as a part of DFC cluster but it is disabled by default. It requires to set a few environment variables before depoying a cluster. Environment variables used by deployment script to set up the authn server:

| Variable | Default value | Description |
|---|---|---|
| AUTHENABLED | false | Set it to `true` to enable authn server and token-based access in DFC proxy |
| AUTH_SU_NAME | admin | Super user name (see `A super user` section for details) |
| AUTH_SU_PASS | admin | Super user password (see `A super user` section for details) |
| SECRETKEY| aBitLongSecretKey | A secret key to encrypt and decrypt tokens |

Note: do not forget to change before starting deployment process the default secret key used to crypt and decrypt tokens.

To change authn settings after deployment, modify its configuration file and restart authn server. If you change authn server secret key make sure to modify DFC proxy configuration as well.

### Authn configuration files

| File | Location |
|---|---|
| Server configuration | $CONFDIR/authn.json |
| User list | $CONFDIR/users.json |
| Log directory | $LOGDIR/authn/log/ |

The configuration file is located at $CONFDIR/authn.json.

### How to enable authn server after deployment

If you deploy a cluster with default configuration the deployment process does not launch authn server but generates configuration for it. Manual start requires the following steps:

- Change DFC proxy configuration to enable token-based access: look for `{"auth": { "enabled": false } }` in proxy configration file and replace `false` with `true`. Restart the proxy to apply changes
- Start authn server: <path_to_dfc_binaries>/authn -config=<path_to_config_dir>/authn.json. Path to config directory is set at the time of cluster deployment and it is the same as the directory for DFC proxies and DFC targets

## User management

### A super user

After deploying the cluster, the super user account is the only existing account. It is a special account that cannot be deleted and has no access to data. It is used to manage users who are able to access the data.

Adding and deleting a regular user requires super user authentication. Super user credentials are sent in the request header in standard way via `Authorization` field (for curl it is `curl -u<username>:<password ...`, for HTTP requesest it is header option `Authorization: Basic <base64-encoded-username:password>`).

### REST operations

| Operation | HTTP Action | Example |
|---|---|---|
| Add a user| POST {"name": "username", "password": "pass"} /v1/users | curl -X POST http://localhost:8203/v1/users -d '{"name": "username","password":"pass"}' -H 'Content-Type: application/json' -uadmin:admin |
| Delete a user | DEL /v1/users/username | curl -X DEL http://localhost:8203/v1/users/username -uadmin:admin |

## Token management

Generating a token for data access does not require super user credentials. Users must provide correct their username and password to get their tokens. Token expires in 30 minutes. After that the token must be reissued. To change default expiration time, look for `expiration_time` in configuration file.

Call revoke token API to forcefully invalidate a token before it expires.

### REST operations

| Operation | HTTP Action | Example |
|---|---|---|
| Generate a token for a user (Log in) | POST {"password": "pass"} /v1/users/username | curl -X POST http://localhost:8203/v1/users/username -d '{"password":"pass"}' -H 'Content-Type: application/json' |
| Revoke a token (Log out) | DEL { "token": "issued_token" } /v1/tokens | curl -X DEL http://localhost:8203/v1/tokens -d '{"token":"issued_token"}' -H 'Content-Type: application/json' |

A generated token is returned as JSON. Example: `{"token": "issued_token"}`.

## Interaction With DFC proxy

DFC proxy requires a valid token in a request header only if authn server is enabled.

After each successful token generation and revoking authn server sends a new list of valid tokens to DFC proxy. Every token includes all information needed by proxy to avoid extra HTTP calls between applications. At this moment a token includes:

- UserID for whom the token was issued (username)
- Time when the token was generated and when it expires
- Credentials to access AWS/GCP (not used at this moment)

### Calling DFC proxy API

If authentication is enabled, a call to DFC proxy REST API must include valid token issued by authn. Token is passed in request header in format: `Authorization: Bearer <token>`. Curl example: `curl -L  http://localhost:8080/v1/buckets/* -X GET -H 'Content-Type: application/json' -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIs'`.

At this moment only requests to bucket and objects API requires a token.

## Known limitations

- It is not possible to limit a user to access only a certain bucket yet. If a user gets a token, the user has the full access to all the data
- The server keeps issued tokens in memory, so after each authn server restart every user must request for a new token
- DFC proxy token list is not persistent. After restarting a proxy a list of valid token has to be refreshed by authn server. Authn sends the current list of valid token after every revoking a token or generating a new one
- AWS/GCP credentials are not supported yet
