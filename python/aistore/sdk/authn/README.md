## AIS Python SDK AuthN Sub-Package

The AIStore Authentication Server (AuthN) is a standalone service that provides secure, user- and role-based access to AIStore by leveraging [OAuth 2.0](https://oauth.net/2/) compliant [JSON Web Tokens (JWTs)](https://datatracker.ietf.org/doc/html/rfc7519). The `aistore.sdk.authn` sub-package in the Python SDK allows developers to interact with the AuthN server to manage authentication, users, roles, clusters, and tokens seamlessly.

> For more details, please refer to the [AuthN documentation](https://github.com/NVIDIA/aistore/blob/main/docs/authn.md).

### Quick Start

#### Client Initialization

To interact with a running AuthN instance, create an `AuthNClient` object by providing the endpoint of the AuthN server:

```python
from aistore.sdk.authn import AuthNClient

authn_client = AuthNClient("http://localhost:52001")
```

This `authn_client` enables the management of roles, users, clusters, and tokens.

#### Logging In

Log in to the AuthN server to get an authorization token for secure interactions:

```python
from aistore.sdk.authn import AuthNClient
from aistore.sdk import Client

# Initialize AuthN client and login as admin
authn_client = AuthNClient("http://localhost:52001")
admin_auth_token = authn_client.login("admin", "admin")

# Initialize AIStore client with the token
aistore_client = Client("http://localhost:8080", admin_auth_token)
```

> Note: 
> - You can either pass the token explicitly, as shown above, or set it as an environment variable (`AIS_AUTHN_TOKEN`). If no token is provided directly, the client will automatically use the token from the environment variable. Ensure the environment variable contains only the token itself (not the full JSON object that the CLI generates).
> - `http://localhost:8080` address (above and elsewhere) is used for purely demonstration purposes and must be understood as a placeholder for an _arbitrary_ AIStore endpoint (`AIS_ENDPOINT`).

#### Registering a Cluster

Register a cluster with the AuthN server for security management. This allows AuthN to ensure that security policies, such as revoked tokens, are properly managed across all clusters. During registration, the AuthN server contacts the AIStore server to verify that `authn.enabled` is set and that the secret signing key hashes match:

```python
cluster_alias = "my-cluster"
cluster_manager = authn_client.cluster_manager()
cluster_info = cluster_manager.register(cluster_alias, ["http://localhost:8080"])
```

#### Creating Custom Roles & Users

Define custom roles and manage users to control access to AIStore resources at the bucket or cluster level, and use the `AccessAttr` class to specify permissions like `GET`, `PUT`, `CREATE-BUCKET`, etc., tailored to your needs.

> For more information on the specific access permissions, refer to the please refer to the permissions section of the [AuthN documentation](https://github.com/NVIDIA/aistore/blob/main/docs/authn.md#permissions) and the section [`AccessAttr`](https://github.com/NVIDIA/aistore/blob/main/docs/python_sdk.md#authn.access_attr.AccessAttr) under the [Python SDK Documentation](https://github.com/NVIDIA/aistore/blob/main/docs/python_sdk.md).

##### Creating a Custom Role

Use the `RoleManager` class to create roles that define access permissions:

```python
role_manager = authn_client.role_manager()

# Custom Role w/ Object GET & PUT Access for `ais://my-bucket`
custom_role = role_manager.create(
    name="Custom-Role",
    desc="Role to GET and PUT objects in specified bucket",
    cluster_alias=cluster_alias,
    perms=[AccessAttr.GET, AccessAttr.PUT],
    bucket_name="my-bucket"
)
```

The `perms` parameter uses the `AccessAttr` class to specify allowed actions for the role.

##### Creating a Custom User

Use the `UserManager` class to create users and assign roles:

```python
user_manager = authn_client.user_manager()

custom_user = user_manager.create(
    username="myusername",
    roles=["Custom-Role"],
    password="mypassword"
)

custom_user_token = authn_client.login("myusername", "mypassword")
aistore_client = Client("http://localhost:8080", custom_user_token)
```

#### Managing Tokens

The TokenManager class provides methods to manage tokens, including revoking them to maintain secure access control.

##### Revoking Tokens

Revoke a token to prevent unauthorized access:

```python
token_manager = authn_client.token_manager()
token_manager.revoke(custom_user_token)
```

When a token is revoked, the AuthN server updates all registered clusters to ensure the token is no longer valid.

### API Documentation

| Module | Summary |
| --- | --- |
| [authn_client.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/authn_client.py) | Manages AuthN server interactions, including login, logout, and managing clusters, roles, users, and tokens. |
| [cluster_manager.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/cluster_manager.py) | Handles cluster management, including registration and updates. |
| [role_manager.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/role_manager.py) | Manages roles and their permissions within AIStore. |
| [token_manager.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/token_manager.py) | Manages tokens, including revocation and secure access control. |
| [user_manager.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/user_manager.py) | Manages users and their roles within AIStore. |
| [access_attr.py](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/authn/access_attr.py) | Defines permissions for access control, including predefined sets like read-only and admin access. |

For more detailed information on available methods, roles, and permissions, as well as best practices for using the AuthN module as well as the AIStore SDK, please refer to the [Python SDK documentation](https://aistore.nvidia.com/docs/python_sdk.md).

### References

* [AIStore GitHub](https://github.com/NVIDIA/aistore)
* [Documentation](https://aistore.nvidia.com/docs)
* [AIStore `pip` Package](https://pypi.org/project/aistore/)
