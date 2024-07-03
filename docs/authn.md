---
layout: post
title: AUTHN
permalink: /docs/authn
redirect_from:
 - /authn.md/
 - /docs/authn.md/
---
The AIStore Authentication Server (AuthN) provides secure access to AIStore by leveraging [OAuth 2.0](https://oauth.net/2/) compliant [JSON Web Tokens (JWT)](https://datatracker.ietf.org/doc/html/rfc7519).

For more details:
- [Introduction to JWT](https://jwt.io/introduction/)
- [Go implementation of JSON Web Tokens](https://github.com/golang-jwt/jwt) used for AuthN.

### Key Features
- **Standalone Server**: AuthN operates *independently*, managing users and tokens separately from the AIStore servers (aisnodes) in the cluster.
- **Secure Tokens**: Currently, AuthN supports HMAC (hash-based message authentication) using the SHA256 hash.
- **Client Workflow**: If AuthN is enabled on a cluster, users must log in to receive a token from AuthN. This token must be included in subsequent HTTP requests to the AIS cluster. Requests without a valid token are rejected by AuthN-enabled AIS clusters.

### Typical Workflow

![AuthN workflow](images/authn_flow.png)

1. **User logs in**: The client sends login credentials to AuthN.
2. **Token Issuance**: AuthN issues a token to the client.
3. **Authenticated Requests**: The client includes the token in the request headers (`Authorization: Bearer <token>`) for subsequent API requests to the AIStore cluster.
4. **API Response**: The cluster processes the request and responds.

### Protocols
AuthN supports both HTTP and HTTPS protocols. By default, AuthN starts as an HTTP server listening on port `52001`. For HTTPS, ensure the configuration file options `server_crt` and `server_key` are correctly set to the SSL certificate and key paths.

AuthN generates tokens that are *self-sufficient*, meaning a proxy does not need to contact AuthN to check permissions. AIStore clusters should be registered with the AuthN server. This allows AuthN to broadcast revoked tokens to all registered clusters, ensuring they update their blacklists accordingly.

## Table of Contents

- [Getting Started](#getting-started)
- [Environment and Configuration](#environment-and-configuration)
  - [Notation](#notation)
  - [AuthN Configuration and Log](#authn-configuration-and-log)
  - [How to Enable AuthN Server After Deployment](#how-to-enable-authn-server-after-deployment)
- [REST API](#rest-api)
  - [Authorization](#authorization)
  - [Tokens](#tokens)
  - [Clusters](#clusters)
  - [Roles](#roles)
  - [Users](#users)
  - [Configuration](#configuration)

## Getting Started

To deploy an AIS cluster with AuthN enabled, follow these steps:
> **Note:** If you already have an AIS cluster deployed and want to add AuthN to it, follow these [instructions](#how-to-enable-authn-server-after-deployment).

1. Ensure there is no previous cluster running:
    ```sh
    make kill clean
    ```
    
> **Note:** When deploying AIStore with AuthN, an admin user is created by default with admin privileges. The default password for the admin user is `admin`. Be sure to [change this password](../cmd/authn/const.go#L22) before starting the server, as it cannot be updated after deployment.

2. Deploy the cluster with AuthN enabled:
    ```sh
    AIS_AUTHN_ENABLED=true make deploy
    ```

This will start up an AIStore cluster with the AuthN server.

### Initial Setup and Authentication

After deploying the cluster, you won't be able to access it without authentication:

```sh
ais cluster show
E 12:48:06.491664 token required: GET /v1/daemon (p[gUFp8080]: htrun.go:1307 <- prxauth.go:234 <- proxy.go:2580 <- proxy.go:2535])
Error: token required
```

1. **Login as Admin**:
    ```sh
    ais auth login admin -p admin
    Logged in (/root/.config/ais/cli/auth.token)
    ```

2. **View Registered Clusters**:
    ```sh
    ais auth show cluster
    CLUSTER ID  ALIAS   URLs
    ```

3. **Add Your Cluster**:
    ```sh
    ais auth add cluster mycluster http://localhost:8080
    ```

4. **Confirm Cluster Registration**:
    ```sh
    ais auth show cluster
    CLUSTER ID  ALIAS       URLs
    eTdL4YGHN   mycluster   http://localhost:8080
    ```

### Default Roles and Permissions

When a cluster is registered, a set of default roles are created:

```sh
ais auth show role
ROLE                          DESCRIPTION
Admin                         AuthN administrator
BucketOwner-mycluster         Full access to buckets in eTdL4YGHN[mycluster]
ClusterOwner-mycluster        Admin access to eTdL4YGHN[mycluster]
Guest-mycluster               Read-only access to buckets in eTdL4YGHN[mycluster]
```

Admins have superuser permissions and can perform all roles in the cluster.

### Enable CLI Auto-Completion

Ensure that you have auto-completions enabled for the AIS CLI:
```sh
make cli-autocompletions
```

You can use `<TAB-TAB>` to view a list of possible options:
```sh
$ ais auth add role role_name <TAB-TAB>
ro               GET              DESTROY-BUCKET   PATCH            PROMOTE          APPEND           SET-BUCKET-ACL 
rw               HEAD-OBJECT      MOVE-BUCKET      PUT              HEAD-BUCKET      MOVE-OBJECT      LIST-BUCKETS 
su               LIST-OBJECTS     ADMIN            DELETE-OBJECT    CREATE-BUCKET    UPDATE-OBJECT    SHOW-CLUSTER 
```

### Example Workflow

1. **Create a Bucket and Add an Object**:
    ```sh
    ais bucket create ais://nnn
    "ais://nnn" created
    ais put README.md ais://nnn
    PUT "README.md" => ais://nnn/README.md
    ```

2. **Create a Role and User**:
    - Create a role that can only list buckets and objects:
        ```sh
        ais auth add role list-perm --cluster mycluster --desc "Users with this role can only list buckets and objects" LIST-OBJECTS LIST-BUCKETS
        ```
        > For a comprehensive list of permissions, see the [Permissions section](#permissions) below.
    - Add a user named `alice` with this role:
        ```sh
        ais auth add user alice -p 12345 list-perm
        ```

3. **Login as the New User and Save the Token**:
    ```sh
    ais auth login alice -p 12345 -f /tmp/alice.token
    Logged in (/tmp/alice.token)
    ```

4. **Perform Operations with Limited Permissions**:
    - List buckets and objects:
        ```sh
        AIS_AUTHN_TOKEN_FILE=/tmp/alice.token ais ls
        AIS_AUTHN_TOKEN_FILE=/tmp/alice.token ais ls ais://nnn
        ```
    - Attempt restricted actions (which will fail due to permissions):
        ```sh
        AIS_AUTHN_TOKEN_FILE=/tmp/alice.token ais get ais://nnn/README.md -
        Error: http error code 'Forbidden', bucket "ais://nnn"
        ```

Further references:

* See [CLI auth subcommand](/docs/cli/auth.md) for all supported command line options and usage examples.

## Environment and Configuration

Environment variables used by the deployment script to set up the AuthN server:

| Variable             | Default Value       | Description                                                   |
|----------------------|---------------------|---------------------------------------------------------------|
| AIS_SECRET_KEY       | `aBitLongSecretKey` | A secret key to sign tokens                                   |
| AIS_AUTHN_ENABLED    | `false`             | Set it to `true` to enable AuthN server and token-based access in AIStore proxy |
| AIS_AUTHN_PORT       | `52001`             | Port on which AuthN listens to requests                       |
| AIS_AUTHN_TTL        | `24h`               | A token expiration time. Can be set to 0 which means "no expiration time" |
| AIS_AUTHN_USE_HTTPS  | `false`             | Enable HTTPS for AuthN server. If `true`, AuthN server requires also `AIS_SERVER_CRT` and `AIS_SERVER_KEY` to be set |
| AIS_SERVER_CRT       | ``                  | OpenSSL certificate. Optional: set it only when secure HTTP is enabled |
| AIS_SERVER_KEY       | ``                  | OpenSSL key. Optional: set it only when secure HTTP is enabled |

All variables can be set at AIStore cluster deployment.
Example of starting a cluster with AuthN enabled:

```sh
AIS_AUTHN_ENABLED=true make deploy
```

> **Note:** Don't forget to change the _default secret key_ used to sign tokens and the _admin password_ before starting the deployment process. If you don't, you will have to restart the cluster.
* More info on env vars: [`api/env/authn.go`](https://github.com/NVIDIA/aistore/blob/main/api/env/authn.go)

## Notation

In this README:

> `AUTHSRV` - denotes a (hostname:port) address of a deployed AuthN server (default: http://localhost:52001)

## AuthN Configuration and Log

| File                 | Location                     |
|----------------------|------------------------------|
| Server configuration | `$AIS_AUTHN_CONF_DIR/authn.json` |
| User database        | `$AIS_AUTHN_CONF_DIR/authn.db`   |
| Log directory        | `$AIS_LOG_DIR/authn/log/`    |

> **Note:** When AuthN is running, execute `ais auth show config` to find out the current location of all AuthN files.

## Permissions

In AIStore, roles define the level of access and the permissions available to users. Here is a detailed explanation of the roles and their associated permissions:

| Permission        | Description                                                 |
|-------------------|-------------------------------------------------------------|
| GET               | Allows reading objects.                                     |
| LIST-OBJECTS      | Allows listing objects within a bucket.                     |
| LIST-BUCKETS      | Allows listing buckets.                                     |
| PUT               | Allows writing or uploading objects.                        |
| DELETE-OBJECT     | Allows deleting objects.                                    |
| HEAD-OBJECT       | Allows retrieving object metadata.                          |
| MOVE-OBJECT       | Allows moving objects within or between buckets.            |
| CREATE-BUCKET     | Allows creating new buckets.                                |
| DESTROY-BUCKET    | Allows deleting buckets.                                    |
| HEAD-BUCKET       | Allows retrieving bucket metadata without listing contents. |
| MOVE-BUCKET       | Allows moving buckets.                                      |
| UPDATE-OBJECT     | Allows updating object metadata.                            |
| APPEND            | Allows appending data to an existing object.                |
| PATCH             | Allows applying patches to objects.                         |
| SET-BUCKET-ACL    | Allows setting access control lists for buckets.            |
| SHOW-CLUSTER      | Allows viewing cluster information.                         |
| PROMOTE           | Allows promoting local files to objects in the cluster.     |
| ADMIN             | Grants full administrative access to the system.            |
| ro                | Grants Read Only permissions. (GET, LIST-OBJECTS and LIST-BUCKETS)                 |
| rw                | Grants Write Only permissions. (GET, PUT, DELETE-OBJECT, HEAD-OBJECT, LIST-OBJECTS, LIST-BUCKETS, MOVE-OBJECT) |
| su                | Grants Super-User permissions. Can perform all of the above.                  |


## How to Enable AuthN Server After Deployment

By default, the AIStore deployment does not launch the AuthN server. To start the AuthN server manually, follow these steps:

1. **Build AuthN:**
    ```sh
    make authn
    ```

2. **Create a Configuration File:**
    You will need a configuration file similar to the following:
    ```json
    $ cat $HOME/.config/ais/authn/authn.json
    {
        "log": {
            "dir": "/tmp/ais/authn/log",
            "level": "3"
        },
        "net": {
            "http": {
                "port": 52001,
                "use_https": false,
                "server_crt": "",
                "server_key": ""
            }
        },
        "auth": {
            "secret": "aBitLongSecretKey",
            "expiration_time": "24h"
        },
        "timeout": {
            "default_timeout": "30s"
        }
    }
    ```

3. **Start the AuthN Server:**
    Start the AuthN server in a new terminal or screen session. Ensure that `authn.json` exists in the configuration directory.
    ```sh
    authn -config=<authn_config_dir>
    ```

4. **Configure AIStore Cluster to Enable Token-Based Access:**
    Change the AIStore cluster configuration to enable token-based access.
    ```sh
    ais config cluster auth.enabled true
    ```

5. **Verify Access:**
    Now, you cannot access the cluster without tokens:
    ```sh
    ais ls
    Error: token required
    ```

6. **Login as Admin:**
    To continue operations, log in as the admin user:
    ```sh
    ais auth login admin -p admin
    Logged in (/root/.config/ais/cli/auth.token)
    ```

7. **Register the Cluster with AuthN:**
    Register the AIStore cluster at AuthN to receive AuthN messages (e.g., revoked token list):
    ```sh
    ais auth add cluster mycluster http://localhost:8080
    ```

8. **Proceed with Cluster Operations:**
    After registering, you can proceed using your cluster with admin privileges. You can add users, set up roles, etc.

> **Note:** This example assumes that AuthN is running on the same host as the AIS cluster. If AuthN is running on a different host, you will need to specify the `AIS_AUTHN_URL` variable. For example, use `AIS_AUTHN_URL=http://10.10.1.190:52001 ais auth COMMAND`.

## REST API

### Authorization

After deploying the cluster, a superuser role `admin` and `admin` account are created automatically.
Only users with the `admin` role can manage AuthN. Every request to AuthN (except login) must contain an authentication token in the header:

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

#### Revoked Tokens

AuthN ensures that all AIS gateways (proxies) are updated with each revoked token.
When AuthN registers a new cluster, it sends the cluster the entire list of revoked tokens.
Periodically, AuthN will clean up the list and remove expired and invalid tokens.

See the following example workflow below, where a token is revoked and only one cluster is registered.
"AIS Cluster 2" is unregistered and allows requests with revoked token:

![Revoke token workflow](images/token_revoke.png)

#### Expired Tokens

Generating a token for data access requires a user name and user password.
By default, the token expiration time is set to 24 hours.
Modify `expiration_time` in the configuration file to change the default expiration time.

To issue a single token with a custom expiration time, pass an optional expiration duration in the request.
Example: generate a token that expires in 5 hours (in nanoseconds). API:

```json
POST {"password": "password", "expires_in": 18000000000000} /v1/users/username
```

CLI:

```sh
ais auth login -p password username -e 5h
```

Pass a zero value `"expires_in": 0` to generate a token with no expiration.

AuthN returns the generated token as a JSON formatted message. Example: `{"token": "issued_token"}`.
The revoke token API shown below will forcefully invalidate a token before it expires.

Call revoke token API to forcefully invalidate a token before it expires.

| Operation                      | HTTP Action | Example                                                                                                                      |
|--------------------------------|-------------|------------------------------------------------------------------------------------------------------------------------------|
| Generate a token for a user (Log in)   | POST /v1/users/\<user-name\> | `curl -X POST $AUTHSRV/v1/users/<user-name> -d '{"password":"<password>"}'`|
| Revoke a token                 | DELETE /v1/tokens| `curl -X DELETE $AUTHSRV/v1/tokens -d '{"token":"<issued_token>"}' -H 'Content-Type: application/json'`

### Clusters

When a cluster is registered, an arbitrary alias can be assigned to the cluster. The CLI supports both the cluster's ID and the cluster's alias in commands. The alias is used to create default roles for a newly registered cluster. If a cluster does not have an alias, the role names contain the cluster ID.

| Operation                       | HTTP Action | Example                                                                                              |
|---------------------------------|-------------|------------------------------------------------------------------------------------------------------|
| Get a list of registered clusters | GET /v1/clusters | `curl -X GET $AUTHSRV/v1/clusters`                                                                       |
| Get a registered cluster info    | GET /v1/clusters/cluster-id | `curl -X GET $AUTHSRV/v1/clusters/cluster-id`                                                           |
| Register a cluster               | POST /v1/clusters| `curl -X POST $AUTHSRV/v1/clusters -d '{"id": "<cluster-id>", "alias": "<cluster-alias>", "urls": ["<http://host:port>"]}' -H 'Content-Type: application/json' -H 'Authorization: Bearer <token>'`                     |
| Update a registered cluster      | PUT /v1/clusters/\<cluster-id\>| `curl -X PUT $AUTHSRV/v1/clusters/<cluster-id> -d '{"id": "<cluster-id>", "alias": "<cluster-alias>", "urls": ["http://host:port"]}' -H 'Content-Type: application/json' -H 'Authorization: Bearer <token>'`                  |
| Delete a registered cluster      | DELETE /v1/clusters/\<cluster-id\> | `curl -X DELETE $AUTHSRV/v1/clusters/<cluster-id> -H 'Authorization: Bearer <token>'` |

### Roles

| Operation                    | HTTP Action | Example                                                                                                               |
|------------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------|
| Get a list of roles          | GET  /v1/roles | `curl -X GET $AUTHSRV/v1/roles`                                                                                          |
| Get a role                   | GET  /v1/roles/\<role-name\> | `curl -X GET $AUTHSRV/v1/roles/<role-name>`                                                                                  |
| Create a new role            | POST /v1/roles/| `curl -X POST $AUTHSRV/v1/roles/ -d '{"name":"<role-name>","desc":"<role-desc>","clusters":[{"id":"<cluster-id>","perm":"<permission-number>"}],"buckets":[{"bck":{"name":"<bck-name>","provider":"<bck-provider>","namespace":{"uuid":"<namespace-id>","name":""}},"perm":"<permission-number>"}],"admin":false}' -H 'Content-Type: application/json' -H 'Authorization: Bearer <token>'` |
| Update an existing role      | PUT /v1/roles/\<role-name\> | `curl -X PUT $AUTHSRV/v1/roles/<role-name> -d '{"name":"<role-name>","desc":"<role-desc>","clusters":[{"id":"<cluster-id>","perm":"<permission-number>"}],"buckets":[{"bck":{"name":"<bck-name>","provider":"<bck-provider>","namespace":{"uuid":"<namespace-id>","name":""}},"perm":"<permission-number>"}],"admin":false}' -H 'Content-Type: application/json' -H 'Authorization: Bearer <token>'`|
| Delete a role                | DELETE /v1/roles/\<role-name\> | `curl -X DELETE $AUTHSRV/v1/roles/<role-name> -H 'Content-Type: application/json' -H 'Authorization: Bearer <token>'` |

### Users

| Operation               | HTTP Action | Example                                                                                                               |
|-------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------|
| Get a list of users     | GET /v1/users | `curl -X GET $AUTHSRV/v1/users`                                                                                          |
| Get a user              | GET /v1/users/\<user-id\> | `curl -X GET $AUTHSRV/v1/users/<user-id>`                                                                                  |
| Add a user              | POST /v1/users | `curl -X POST $AUTHSRV/v1/users -d '{"id": "<user-id>", "password": "<password>", "roles": "[{<role-json>}]"' -H 'Authorization: Bearer <token>'` |
| Update an existing user | PUT /v1/users/\<user-id\> | `curl -X PUT $AUTHSRV/v1/users/<user-id> -d '{"id": "<user-id>", "password": "<password>", "roles": "[{<role-json>}]"' -H 'Authorization: Bearer <token>'`                    |
| Delete a user           | DELETE /v1/users/\<user-id\> | `curl -X DELETE $AUTHSRV/v1/users/<user-id>  -H 'Authorization: Bearer <token>'`                                                      |

### Configuration

| Operation                    | HTTP Action | Example                                                                                       |
|------------------------------|-------------|-----------------------------------------------------------------------------------------------|
| Get AuthN configuration      | GET /v1/daemon | `curl -X GET $AUTHSRV/v1/daemon -H 'Authorization: Bearer <token>'` |
| Update AuthN configuration   | PUT /v1/daemon | `curl -X PUT $AUTHSRV/v1/daemon -d '{"log":{"dir":"<log-dir>","level":"<log-level>"},"net":{"http":{"port":<port>,"use_https":false,"server_crt":"","server_key":""}},"auth":{"secret":"aBitLongSecretKey","expiration_time":"24h0m"},"timeout":{"default_timeout":"30s"}}' -H 'Authorization: Bearer <token>'` |