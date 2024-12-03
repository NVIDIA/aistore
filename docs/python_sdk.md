---
layout: post
title: PYTHON SDK
permalink: /docs/python-sdk
redirect_from:
 - /python_sdk.md/
 - /docs/python_sdk.md/
---

AIStore Python SDK is a growing set of client-side objects and methods to access and utilize AIS clusters. This document contains API documentation
for the AIStore Python SDK.

> For our PyTorch integration, please refer to the [PyTorch Docs](https://github.com/NVIDIA/aistore/tree/main/docs/pytorch.md).
For more information, please refer to [AIS Python SDK](https://pypi.org/project/aistore) available via Python Package Index (PyPI)
or see [https://github.com/NVIDIA/aistore/tree/main/python/aistore](https://github.com/NVIDIA/aistore/tree/main/python/aistore).
* [authn.authn\_client](#authn.authn_client)
  * [AuthNClient](#authn.authn_client.AuthNClient)
    * [client](#authn.authn_client.AuthNClient.client)
    * [login](#authn.authn_client.AuthNClient.login)
    * [logout](#authn.authn_client.AuthNClient.logout)
    * [cluster\_manager](#authn.authn_client.AuthNClient.cluster_manager)
    * [role\_manager](#authn.authn_client.AuthNClient.role_manager)
    * [user\_manager](#authn.authn_client.AuthNClient.user_manager)
    * [token\_manager](#authn.authn_client.AuthNClient.token_manager)
* [authn.cluster\_manager](#authn.cluster_manager)
  * [ClusterManager](#authn.cluster_manager.ClusterManager)
    * [client](#authn.cluster_manager.ClusterManager.client)
    * [list](#authn.cluster_manager.ClusterManager.list)
    * [get](#authn.cluster_manager.ClusterManager.get)
    * [register](#authn.cluster_manager.ClusterManager.register)
    * [update](#authn.cluster_manager.ClusterManager.update)
    * [delete](#authn.cluster_manager.ClusterManager.delete)
* [authn.role\_manager](#authn.role_manager)
  * [RoleManager](#authn.role_manager.RoleManager)
    * [client](#authn.role_manager.RoleManager.client)
    * [list](#authn.role_manager.RoleManager.list)
    * [get](#authn.role_manager.RoleManager.get)
    * [create](#authn.role_manager.RoleManager.create)
    * [update](#authn.role_manager.RoleManager.update)
    * [delete](#authn.role_manager.RoleManager.delete)
* [authn.token\_manager](#authn.token_manager)
  * [TokenManager](#authn.token_manager.TokenManager)
    * [client](#authn.token_manager.TokenManager.client)
    * [revoke](#authn.token_manager.TokenManager.revoke)
* [authn.user\_manager](#authn.user_manager)
  * [UserManager](#authn.user_manager.UserManager)
    * [client](#authn.user_manager.UserManager.client)
    * [get](#authn.user_manager.UserManager.get)
    * [delete](#authn.user_manager.UserManager.delete)
    * [create](#authn.user_manager.UserManager.create)
    * [list](#authn.user_manager.UserManager.list)
    * [update](#authn.user_manager.UserManager.update)
* [authn.access\_attr](#authn.access_attr)
  * [AccessAttr](#authn.access_attr.AccessAttr)
    * [describe](#authn.access_attr.AccessAttr.describe)
* [bucket](#bucket)
  * [Bucket](#bucket.Bucket)
    * [client](#bucket.Bucket.client)
    * [client](#bucket.Bucket.client)
    * [qparam](#bucket.Bucket.qparam)
    * [provider](#bucket.Bucket.provider)
    * [name](#bucket.Bucket.name)
    * [namespace](#bucket.Bucket.namespace)
    * [list\_urls](#bucket.Bucket.list_urls)
    * [list\_all\_objects\_iter](#bucket.Bucket.list_all_objects_iter)
    * [create](#bucket.Bucket.create)
    * [delete](#bucket.Bucket.delete)
    * [rename](#bucket.Bucket.rename)
    * [evict](#bucket.Bucket.evict)
    * [head](#bucket.Bucket.head)
    * [summary](#bucket.Bucket.summary)
    * [info](#bucket.Bucket.info)
    * [copy](#bucket.Bucket.copy)
    * [list\_objects](#bucket.Bucket.list_objects)
    * [list\_objects\_iter](#bucket.Bucket.list_objects_iter)
    * [list\_all\_objects](#bucket.Bucket.list_all_objects)
    * [transform](#bucket.Bucket.transform)
    * [put\_files](#bucket.Bucket.put_files)
    * [object](#bucket.Bucket.object)
    * [objects](#bucket.Bucket.objects)
    * [make\_request](#bucket.Bucket.make_request)
    * [verify\_cloud\_bucket](#bucket.Bucket.verify_cloud_bucket)
    * [get\_path](#bucket.Bucket.get_path)
    * [as\_model](#bucket.Bucket.as_model)
    * [write\_dataset](#bucket.Bucket.write_dataset)
* [client](#client)
  * [Client](#client.Client)
    * [bucket](#client.Client.bucket)
    * [cluster](#client.Client.cluster)
    * [job](#client.Client.job)
    * [etl](#client.Client.etl)
    * [dsort](#client.Client.dsort)
    * [fetch\_object\_by\_url](#client.Client.fetch_object_by_url)
* [cluster](#cluster)
  * [Cluster](#cluster.Cluster)
    * [client](#cluster.Cluster.client)
    * [get\_info](#cluster.Cluster.get_info)
    * [get\_primary\_url](#cluster.Cluster.get_primary_url)
    * [list\_buckets](#cluster.Cluster.list_buckets)
    * [list\_jobs\_status](#cluster.Cluster.list_jobs_status)
    * [list\_running\_jobs](#cluster.Cluster.list_running_jobs)
    * [list\_running\_etls](#cluster.Cluster.list_running_etls)
    * [is\_ready](#cluster.Cluster.is_ready)
    * [get\_performance](#cluster.Cluster.get_performance)
    * [get\_uuid](#cluster.Cluster.get_uuid)
* [etl](#etl)
* [job](#job)
  * [Job](#job.Job)
    * [job\_id](#job.Job.job_id)
    * [job\_kind](#job.Job.job_kind)
    * [status](#job.Job.status)
    * [wait](#job.Job.wait)
    * [wait\_for\_idle](#job.Job.wait_for_idle)
    * [wait\_single\_node](#job.Job.wait_single_node)
    * [start](#job.Job.start)
    * [get\_within\_timeframe](#job.Job.get_within_timeframe)
* [multiobj.object\_group](#multiobj.object_group)
  * [ObjectGroup](#multiobj.object_group.ObjectGroup)
    * [client](#multiobj.object_group.ObjectGroup.client)
    * [client](#multiobj.object_group.ObjectGroup.client)
    * [list\_urls](#multiobj.object_group.ObjectGroup.list_urls)
    * [list\_all\_objects\_iter](#multiobj.object_group.ObjectGroup.list_all_objects_iter)
    * [delete](#multiobj.object_group.ObjectGroup.delete)
    * [evict](#multiobj.object_group.ObjectGroup.evict)
    * [prefetch](#multiobj.object_group.ObjectGroup.prefetch)
    * [copy](#multiobj.object_group.ObjectGroup.copy)
    * [transform](#multiobj.object_group.ObjectGroup.transform)
    * [archive](#multiobj.object_group.ObjectGroup.archive)
    * [list\_names](#multiobj.object_group.ObjectGroup.list_names)
* [multiobj.object\_names](#multiobj.object_names)
  * [ObjectNames](#multiobj.object_names.ObjectNames)
* [multiobj.object\_range](#multiobj.object_range)
  * [ObjectRange](#multiobj.object_range.ObjectRange)
    * [from\_string](#multiobj.object_range.ObjectRange.from_string)
* [multiobj.object\_template](#multiobj.object_template)
  * [ObjectTemplate](#multiobj.object_template.ObjectTemplate)
* [obj.object](#obj.object)
  * [BucketDetails](#obj.object.BucketDetails)
  * [Object](#obj.object.Object)
    * [bucket\_name](#obj.object.Object.bucket_name)
    * [bucket\_provider](#obj.object.Object.bucket_provider)
    * [query\_params](#obj.object.Object.query_params)
    * [name](#obj.object.Object.name)
    * [props](#obj.object.Object.props)
    * [head](#obj.object.Object.head)
    * [get\_reader](#obj.object.Object.get_reader)
    * [get](#obj.object.Object.get)
    * [get\_semantic\_url](#obj.object.Object.get_semantic_url)
    * [get\_url](#obj.object.Object.get_url)
    * [put\_content](#obj.object.Object.put_content)
    * [put\_file](#obj.object.Object.put_file)
    * [get\_writer](#obj.object.Object.get_writer)
    * [promote](#obj.object.Object.promote)
    * [delete](#obj.object.Object.delete)
    * [blob\_download](#obj.object.Object.blob_download)
    * [append\_content](#obj.object.Object.append_content)
    * [set\_custom\_props](#obj.object.Object.set_custom_props)
* [obj.object\_reader](#obj.object_reader)
  * [ObjectReader](#obj.object_reader.ObjectReader)
    * [head](#obj.object_reader.ObjectReader.head)
    * [attributes](#obj.object_reader.ObjectReader.attributes)
    * [read\_all](#obj.object_reader.ObjectReader.read_all)
    * [raw](#obj.object_reader.ObjectReader.raw)
    * [as\_file](#obj.object_reader.ObjectReader.as_file)
    * [iter\_from\_position](#obj.object_reader.ObjectReader.iter_from_position)
    * [\_\_iter\_\_](#obj.object_reader.ObjectReader.__iter__)
* [obj.obj\_file.object\_file](#obj.obj_file.object_file)
  * [ObjectFile](#obj.obj_file.object_file.ObjectFile)
    * [readable](#obj.obj_file.object_file.ObjectFile.readable)
    * [read](#obj.obj_file.object_file.ObjectFile.read)
    * [close](#obj.obj_file.object_file.ObjectFile.close)
  * [ObjectFileWriter](#obj.obj_file.object_file.ObjectFileWriter)
    * [write](#obj.obj_file.object_file.ObjectFileWriter.write)
    * [flush](#obj.obj_file.object_file.ObjectFileWriter.flush)
    * [close](#obj.obj_file.object_file.ObjectFileWriter.close)
* [obj.object\_props](#obj.object_props)
  * [ObjectProps](#obj.object_props.ObjectProps)
    * [bucket\_name](#obj.object_props.ObjectProps.bucket_name)
    * [bucket\_provider](#obj.object_props.ObjectProps.bucket_provider)
    * [name](#obj.object_props.ObjectProps.name)
    * [location](#obj.object_props.ObjectProps.location)
    * [mirror\_paths](#obj.object_props.ObjectProps.mirror_paths)
    * [mirror\_copies](#obj.object_props.ObjectProps.mirror_copies)
    * [present](#obj.object_props.ObjectProps.present)
* [obj.object\_attributes](#obj.object_attributes)
  * [ObjectAttributes](#obj.object_attributes.ObjectAttributes)
    * [size](#obj.object_attributes.ObjectAttributes.size)
    * [checksum\_type](#obj.object_attributes.ObjectAttributes.checksum_type)
    * [checksum\_value](#obj.object_attributes.ObjectAttributes.checksum_value)
    * [access\_time](#obj.object_attributes.ObjectAttributes.access_time)
    * [obj\_version](#obj.object_attributes.ObjectAttributes.obj_version)
    * [custom\_metadata](#obj.object_attributes.ObjectAttributes.custom_metadata)

<a id="authn.authn_client.AuthNClient"></a>

## Class: AuthNClient

```python
class AuthNClient()
```

AuthN client for managing authentication.

This client provides methods to interact with AuthN Server.
For more info on AuthN Server, see https://github.com/NVIDIA/aistore/blob/main/docs/authn.md

**Arguments**:

- `endpoint` _str_ - AuthN service endpoint URL.
- `skip_verify` _bool, optional_ - If True, skip SSL certificate verification. Defaults to False.
- `ca_cert` _str, optional_ - Path to a CA certificate file for SSL verification.
- `timeout` _Union[float, Tuple[float, float], None], optional_ - Request timeout in seconds; a single float
  for both connect/read timeouts (e.g., 5.0), a tuple for separate connect/read timeouts (e.g., (3.0, 10.0)),
  or None to disable timeout.
- `retry` _urllib3.Retry, optional_ - Retry configuration object from the urllib3 library.
- `token` _str, optional_ - Authorization token.

<a id="authn.authn_client.AuthNClient.client"></a>

### client

```python
@property
def client() -> RequestClient
```

Get the request client.

**Returns**:

- `RequestClient` - The client this AuthN client uses to make requests.

<a id="authn.authn_client.AuthNClient.login"></a>

### login

```python
def login(username: str,
          password: str,
          expires_in: Optional[Union[int, float]] = None) -> str
```

Logs in to the AuthN Server and returns an authorization token.

**Arguments**:

- `username` _str_ - The username to log in with.
- `password` _str_ - The password to log in with.
- `expires_in` _Optional[Union[int, float]]_ - The expiration duration of the token in seconds.
  

**Returns**:

- `str` - An authorization token to use for future requests.
  

**Raises**:

- `ValueError` - If the password is empty or consists only of spaces.
- `AISError` - If the login request fails.

<a id="authn.authn_client.AuthNClient.logout"></a>

### logout

```python
def logout() -> None
```

Logs out and revokes current token from the AuthN Server.

**Raises**:

- `AISError` - If the logout request fails.

<a id="authn.authn_client.AuthNClient.cluster_manager"></a>

### cluster\_manager

```python
def cluster_manager() -> ClusterManager
```

Factory method to create a ClusterManager instance.

**Returns**:

- `ClusterManager` - An instance to manage cluster operations.

<a id="authn.authn_client.AuthNClient.role_manager"></a>

### role\_manager

```python
def role_manager() -> RoleManager
```

Factory method to create a RoleManager instance.

**Returns**:

- `RoleManager` - An instance to manage role operations.

<a id="authn.authn_client.AuthNClient.user_manager"></a>

### user\_manager

```python
def user_manager() -> UserManager
```

Factory method to create a UserManager instance.

**Returns**:

- `UserManager` - An instance to manage user operations.

<a id="authn.authn_client.AuthNClient.token_manager"></a>

### token\_manager

```python
def token_manager() -> TokenManager
```

Factory method to create a TokenManager instance.

**Returns**:

- `TokenManager` - An instance to manage token operations.

<a id="authn.cluster_manager.ClusterManager"></a>

## Class: ClusterManager

```python
class ClusterManager()
```

ClusterManager class for handling operations on clusters within the context of authentication.

This class provides methods to list, get, register, update, and delete clusters on AuthN server.

**Arguments**:

- `client` _RequestClient_ - The request client to make HTTP requests.

<a id="authn.cluster_manager.ClusterManager.client"></a>

### client

```python
@property
def client() -> RequestClient
```

RequestClient: The client this cluster manager uses to make requests.

<a id="authn.cluster_manager.ClusterManager.list"></a>

### list

```python
def list() -> ClusterList
```

Retrieve all clusters.

**Returns**:

- `ClusterList` - A list of all clusters.
  

**Raises**:

- `AISError` - If an error occurs while listing clusters.

<a id="authn.cluster_manager.ClusterManager.get"></a>

### get

```python
def get(cluster_id: Optional[str] = None,
        cluster_alias: Optional[str] = None) -> ClusterInfo
```

Retrieve a specific cluster by ID or alias.

**Arguments**:

- `cluster_id` _Optional[str]_ - The ID of the cluster. Defaults to None.
- `cluster_alias` _Optional[str]_ - The alias of the cluster. Defaults to None.
  

**Returns**:

- `ClusterInfo` - Information about the specified cluster.
  

**Raises**:

- `ValueError` - If neither cluster_id nor cluster_alias is provided.
- `RuntimeError` - If no cluster matches the provided ID or alias.
- `AISError` - If an error occurs while getting the cluster.

<a id="authn.cluster_manager.ClusterManager.register"></a>

### register

```python
def register(cluster_alias: str, urls: List[str]) -> ClusterInfo
```

Register a new cluster.

**Arguments**:

- `cluster_alias` _str_ - The alias for the new cluster.
- `urls` _List[str]_ - A list of URLs for the new cluster.
  

**Returns**:

- `ClusterInfo` - Information about the registered cluster.
  

**Raises**:

- `ValueError` - If no URLs are provided or an invalid URL is provided.
- `AISError` - If an error occurs while registering the cluster.

<a id="authn.cluster_manager.ClusterManager.update"></a>

### update

```python
def update(cluster_id: str,
           cluster_alias: Optional[str] = None,
           urls: Optional[List[str]] = None) -> ClusterInfo
```

Update an existing cluster.

**Arguments**:

- `cluster_id` _str_ - The ID of the cluster to update.
- `cluster_alias` _Optional[str]_ - The new alias for the cluster. Defaults to None.
- `urls` _Optional[List[str]]_ - The new list of URLs for the cluster. Defaults to None.
  

**Returns**:

- `ClusterInfo` - Information about the updated cluster.
  

**Raises**:

- `ValueError` - If neither cluster_alias nor urls are provided.
- `AISError` - If an error occurs while updating the cluster

<a id="authn.cluster_manager.ClusterManager.delete"></a>

### delete

```python
def delete(cluster_id: Optional[str] = None,
           cluster_alias: Optional[str] = None)
```

Delete a specific cluster by ID or alias.

**Arguments**:

- `cluster_id` _Optional[str]_ - The ID of the cluster to delete. Defaults to None.
- `cluster_alias` _Optional[str]_ - The alias of the cluster to delete. Defaults to None.
  

**Raises**:

- `ValueError` - If neither cluster_id nor cluster_alias is provided.
- `AISError` - If an error occurs while deleting the cluster

<a id="authn.role_manager.RoleManager"></a>

## Class: RoleManager

```python
class RoleManager()
```

Manages role-related operations.

This class provides methods to interact with roles, including
retrieving, creating, updating, and deleting role information.

**Arguments**:

- `client` _RequestClient_ - The RequestClient used to make HTTP requests.

<a id="authn.role_manager.RoleManager.client"></a>

### client

```python
@property
def client() -> RequestClient
```

Returns the RequestClient instance used by this RoleManager.

<a id="authn.role_manager.RoleManager.list"></a>

### list

```python
def list() -> RolesList
```

Retrieves information about all roles.

**Returns**:

- `RoleList` - A list containing information about all roles.
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore.
- `requests.RequestException` - If the HTTP request fails.

<a id="authn.role_manager.RoleManager.get"></a>

### get

```python
def get(role_name: str) -> RoleInfo
```

Retrieves information about a specific role.

**Arguments**:

- `role_name` _str_ - The name of the role to retrieve.
  

**Returns**:

- `RoleInfo` - Information about the specified role.
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore.
- `requests.RequestException` - If the HTTP request fails.

<a id="authn.role_manager.RoleManager.create"></a>

### create

```python
def create(name: str,
           desc: str,
           cluster_alias: str,
           perms: List[AccessAttr],
           bucket_name: str = None) -> RoleInfo
```

Creates a new role.

**Arguments**:

- `name` _str_ - The name of the role.
- `desc` _str_ - A description of the role.
- `cluster_alias` _str_ - The alias of the cluster this role will have access to.
- `perms` _List[AccessAttr]_ - A list of permissions to be granted for this role.
- `bucket_name` _str, optional_ - The name of the bucket this role will have access to.
  

**Returns**:

- `RoleInfo` - Information about the newly created role.
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore.
- `requests.RequestException` - If the HTTP request fails.

<a id="authn.role_manager.RoleManager.update"></a>

### update

```python
def update(name: str,
           desc: str = None,
           cluster_alias: str = None,
           perms: List[AccessAttr] = None,
           bucket_name: str = None) -> RoleInfo
```

Updates an existing role.

**Arguments**:

- `name` _str_ - The name of the role.
- `desc` _str, optional_ - An updated description of the role.
- `cluster_alias` _str, optional_ - The alias of the cluster this role will have access to.
- `perms` _List[AccessAttr], optional_ - A list of updated permissions to be granted for this role.
- `bucket_name` _str, optional_ - The name of the bucket this role will have access to.
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore.
- `requests.RequestException` - If the HTTP request fails.
- `ValueError` - If the role does not exist or if invalid parameters are provided.

<a id="authn.role_manager.RoleManager.delete"></a>

### delete

```python
def delete(name: str, missing_ok: bool = False) -> None
```

Deletes a role.

**Arguments**:

- `name` _str_ - The name of the role to delete.
- `missing_ok` _bool_ - Ignore error if role does not exist. Defaults to False
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore.
- `requests.RequestException` - If the HTTP request fails.
- `ValueError` - If the role does not exist.

<a id="authn.token_manager.TokenManager"></a>

## Class: TokenManager

```python
class TokenManager()
```

Manages token-related operations.

This class provides methods to interact with tokens in the AuthN server.
.

**Arguments**:

- `client` _RequestClient_ - The RequestClient used to make HTTP requests.

<a id="authn.token_manager.TokenManager.client"></a>

### client

```python
@property
def client() -> RequestClient
```

Returns the RequestClient instance used by this TokenManager.

<a id="authn.token_manager.TokenManager.revoke"></a>

### revoke

```python
def revoke(token: str) -> None
```

Revokes the specified authentication token.

**Arguments**:

- `token` _str_ - The token to be revoked.
  

**Raises**:

- `ValueError` - If the token is not provided.
- `AISError` - If the revoke token request fails.

<a id="authn.user_manager.UserManager"></a>

## Class: UserManager

```python
class UserManager()
```

UserManager provides methods to manage users in the AuthN service.

**Arguments**:

- `client` _RequestClient_ - The RequestClient used to make HTTP requests.

<a id="authn.user_manager.UserManager.client"></a>

### client

```python
@property
def client() -> RequestClient
```

Returns the RequestClient instance used by this UserManager.

<a id="authn.user_manager.UserManager.get"></a>

### get

```python
def get(username: str) -> UserInfo
```

Retrieve user information from the AuthN Server.

**Arguments**:

- `username` _str_ - The username to retrieve.
  

**Returns**:

- `UserInfo` - The user's information.
  

**Raises**:

- `AISError` - If the user retrieval request fails.

<a id="authn.user_manager.UserManager.delete"></a>

### delete

```python
def delete(username: str, missing_ok: bool = False) -> None
```

Delete an existing user from the AuthN Server.

**Arguments**:

- `username` _str_ - The username of the user to delete.
- `missing_ok` _bool_ - Ignore error if user does not exist. Defaults to False.
  

**Raises**:

- `AISError` - If the user deletion request fails.

<a id="authn.user_manager.UserManager.create"></a>

### create

```python
def create(username: str, roles: List[str], password: str) -> UserInfo
```

Create a new user in the AuthN Server.

**Arguments**:

- `username` _str_ - The name or ID of the user to create.
- `password` _str_ - The password for the user.
- `roles` _List[str]_ - The list of names of roles to assign to the user.
  

**Returns**:

- `UserInfo` - The created user's information.
  

**Raises**:

- `AISError` - If the user creation request fails.

<a id="authn.user_manager.UserManager.list"></a>

### list

```python
def list()
```

List all users in the AuthN Server.

**Returns**:

- `str` - The list of users in the AuthN Server.
  

**Raises**:

- `AISError` - If the user list request fails.

<a id="authn.user_manager.UserManager.update"></a>

### update

```python
def update(username: str,
           password: Optional[str] = None,
           roles: Optional[List[str]] = None) -> UserInfo
```

Update an existing user's information in the AuthN Server.

**Arguments**:

- `username` _str_ - The ID of the user to update.
- `password` _str, optional_ - The new password for the user.
- `roles` _List[str], optional_ - The list of names of roles to assign to the user.
  

**Returns**:

- `UserInfo` - The updated user's information.
  

**Raises**:

- `AISError` - If the user update request fails.

<a id="authn.access_attr.AccessAttr"></a>

## Class: AccessAttr

```python
class AccessAttr(IntFlag)
```

AccessAttr defines permissions as bitwise flags for access control (for more details, refer to the Go API).

<a id="authn.access_attr.AccessAttr.describe"></a>

### describe

```python
@staticmethod
def describe(perms: int) -> str
```

Returns a comma-separated string describing the permissions based on the provided bitwise flags.

<a id="bucket.Bucket"></a>

## Class: Bucket

```python
class Bucket(AISSource)
```

A class representing a bucket that contains user data.

**Arguments**:

- `client` _RequestClient_ - Client for interfacing with AIS cluster
- `name` _str_ - name of bucket
- `provider` _str or Provider, optional_ - Provider of bucket (one of "ais", "aws", "gcp", ...), defaults to "ais"
- `namespace` _Namespace, optional_ - Namespace of bucket, defaults to None

<a id="bucket.Bucket.client"></a>

### client

```python
@property
def client() -> RequestClient
```

The client used by this bucket.

<a id="bucket.Bucket.client"></a>

### client

```python
@client.setter
def client(client)
```

Update the client used by this bucket.

<a id="bucket.Bucket.qparam"></a>

### qparam

```python
@property
def qparam() -> Dict
```

Default query parameters to use with API calls from this bucket.

<a id="bucket.Bucket.provider"></a>

### provider

```python
@property
def provider() -> Provider
```

The provider for this bucket.

<a id="bucket.Bucket.name"></a>

### name

```python
@property
def name() -> str
```

The name of this bucket.

<a id="bucket.Bucket.namespace"></a>

### namespace

```python
@property
def namespace() -> Namespace
```

The namespace for this bucket.

<a id="bucket.Bucket.list_urls"></a>

### list\_urls

```python
def list_urls(prefix: str = "", etl_name: str = None) -> Iterable[str]
```

Implementation of the abstract method from AISSource that provides an iterator
of full URLs to every object in this bucket matching the specified prefix

**Arguments**:

- `prefix` _str, optional_ - Limit objects selected by a given string prefix
- `etl_name` _str, optional_ - ETL to include in URLs
  

**Returns**:

  Iterator of full URLs of all objects matching the prefix

<a id="bucket.Bucket.list_all_objects_iter"></a>

### list\_all\_objects\_iter

```python
def list_all_objects_iter(prefix: str = "",
                          props: str = "name,size") -> Iterable[Object]
```

Implementation of the abstract method from AISSource that provides an iterator
of all the objects in this bucket matching the specified prefix.

**Arguments**:

- `prefix` _str, optional_ - Limit objects selected by a given string prefix
- `props` _str, optional_ - Comma-separated list of object properties to return. Default value is "name,size".
- `Properties` - "name", "size", "atime", "version", "checksum", "target_url", "copies".
  

**Returns**:

  Iterator of all object URLs matching the prefix

<a id="bucket.Bucket.create"></a>

### create

```python
def create(exist_ok=False)
```

Creates a bucket in AIStore cluster.
Can only create a bucket for AIS provider on localized cluster. Remote cloud buckets do not support creation.

**Arguments**:

- `exist_ok` _bool, optional_ - Ignore error if the cluster already contains this bucket
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `aistore.sdk.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.delete"></a>

### delete

```python
def delete(missing_ok=False)
```

Destroys bucket in AIStore cluster.
In all cases removes both the bucket's content _and_ the bucket's metadata from the cluster.
Note: AIS will _not_ call the remote backend provider to delete the corresponding Cloud bucket
(iff the bucket in question is, in fact, a Cloud bucket).

**Arguments**:

- `missing_ok` _bool, optional_ - Ignore error if bucket does not exist
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `aistore.sdk.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.rename"></a>

### rename

```python
def rename(to_bck_name: str) -> str
```

Renames bucket in AIStore cluster.
Only works on AIS buckets. Returns job ID that can be used later to check the status of the asynchronous
operation.

**Arguments**:

- `to_bck_name` _str_ - New bucket name for bucket to be renamed as
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `aistore.sdk.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.evict"></a>

### evict

```python
def evict(keep_md: bool = False)
```

Evicts bucket in AIStore cluster.
NOTE: only Cloud buckets can be evicted.

**Arguments**:

- `keep_md` _bool, optional_ - If true, evicts objects but keeps the bucket's metadata (i.e., the bucket's name
  and its properties)
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `aistore.sdk.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.head"></a>

### head

```python
def head() -> Header
```

Requests bucket properties.

**Returns**:

  Response header with the bucket properties
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.summary"></a>

### summary

```python
def summary(uuid: str = "",
            prefix: str = "",
            cached: bool = True,
            present: bool = True)
```

Returns bucket summary (starts xaction job and polls for results).

**Arguments**:

- `uuid` _str_ - Identifier for the bucket summary. Defaults to an empty string.
- `prefix` _str_ - Prefix for objects to be included in the bucket summary.
  Defaults to an empty string (all objects).
- `cached` _bool_ - If True, summary entails cached entities. Defaults to True.
- `present` _bool_ - If True, summary entails present entities. Defaults to True.
  

**Raises**:

- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
- `aistore.sdk.errors.AISError` - All other types of errors with AIStore

<a id="bucket.Bucket.info"></a>

### info

```python
def info(flt_presence: int = FLTPresence.FLT_EXISTS,
         bsumm_remote: bool = True,
         prefix: str = "")
```

Returns bucket summary and information/properties.

**Arguments**:

- `flt_presence` _FLTPresence_ - Describes the presence of buckets and objects with respect to their existence
  or non-existence in the AIS cluster using the enum FLTPresence. Defaults to
  value FLT_EXISTS and values are:
  FLT_EXISTS - object or bucket exists inside and/or outside cluster
  FLT_EXISTS_NO_PROPS - same as FLT_EXISTS but no need to return summary
  FLT_PRESENT - bucket is present or object is present and properly
  located
  FLT_PRESENT_NO_PROPS - same as FLT_PRESENT but no need to return summary
  FLT_PRESENT_CLUSTER - objects present anywhere/how in
  the cluster as replica, ec-slices, misplaced
  FLT_EXISTS_OUTSIDE - not present; exists outside cluster
- `bsumm_remote` _bool_ - If True, returned bucket info will include remote objects as well
- `prefix` _str_ - Only include objects with the given prefix in the bucket
  

**Raises**:

- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
- `ValueError` - `flt_presence` is not one of the expected values
- `aistore.sdk.errors.AISError` - All other types of errors with AIStore

<a id="bucket.Bucket.copy"></a>

### copy

```python
def copy(to_bck: Bucket,
         prefix_filter: str = "",
         prepend: str = "",
         dry_run: bool = False,
         force: bool = False,
         latest: bool = False,
         sync: bool = False) -> str
```

Returns job ID that can be used later to check the status of the asynchronous operation.

**Arguments**:

- `to_bck` _Bucket_ - Destination bucket
- `prefix_filter` _str, optional_ - Only copy objects with names starting with this prefix
- `prepend` _str, optional_ - Value to prepend to the name of copied objects
- `dry_run` _bool, optional_ - Determines if the copy should actually
  happen or not
- `force` _bool, optional_ - Override existing destination bucket
- `latest` _bool, optional_ - GET the latest object version from the associated remote bucket
- `sync` _bool, optional_ - synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.list_objects"></a>

### list\_objects

```python
def list_objects(prefix: str = "",
                 props: str = "",
                 page_size: int = 0,
                 uuid: str = "",
                 continuation_token: str = "",
                 flags: List[ListObjectFlag] = None,
                 target: str = "") -> BucketList
```

Returns a structure that contains a page of objects, job ID, and continuation token (to read the next page, if
available).

**Arguments**:

- `prefix` _str, optional_ - Return only objects that start with the prefix
- `props` _str, optional_ - Comma-separated list of object properties to return. Default value is "name,size".
- `Properties` - "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies",
  "ec", "custom", "node".
- `page_size` _int, optional_ - Return at most "page_size" objects.
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return
  more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number of objects.
- `uuid` _str, optional_ - Job ID, required to get the next page of objects
- `continuation_token` _str, optional_ - Marks the object to start reading the next page
- `flags` _List[ListObjectFlag], optional_ - Optional list of ListObjectFlag enums to include as flags in the
  request
  target(str, optional): Only list objects on this specific target node
  

**Returns**:

- `BucketList` - the page of objects in the bucket and the continuation token to get the next page
  Empty continuation token marks the final page of the object list
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.list_objects_iter"></a>

### list\_objects\_iter

```python
def list_objects_iter(prefix: str = "",
                      props: str = "",
                      page_size: int = 0,
                      flags: List[ListObjectFlag] = None,
                      target: str = "") -> ObjectIterator
```

Returns an iterator for all objects in bucket

**Arguments**:

- `prefix` _str, optional_ - Return only objects that start with the prefix
- `props` _str, optional_ - Comma-separated list of object properties to return. Default value is "name,size".
- `Properties` - "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies",
  "ec", "custom", "node".
- `page_size` _int, optional_ - return at most "page_size" objects
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return
  more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects
- `flags` _List[ListObjectFlag], optional_ - Optional list of ListObjectFlag enums to include as flags in the
  request
  target(str, optional): Only list objects on this specific target node
  

**Returns**:

- `ObjectIterator` - object iterator
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.list_all_objects"></a>

### list\_all\_objects

```python
def list_all_objects(prefix: str = "",
                     props: str = "",
                     page_size: int = 0,
                     flags: List[ListObjectFlag] = None,
                     target: str = "") -> List[BucketEntry]
```

Returns a list of all objects in bucket

**Arguments**:

- `prefix` _str, optional_ - return only objects that start with the prefix
- `props` _str, optional_ - comma-separated list of object properties to return. Default value is "name,size".
- `Properties` - "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies",
  "ec", "custom", "node".
- `page_size` _int, optional_ - return at most "page_size" objects
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return
  more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects
- `flags` _List[ListObjectFlag], optional_ - Optional list of ListObjectFlag enums to include as flags in the
  request
  target(str, optional): Only list objects on this specific target node
  

**Returns**:

- `List[BucketEntry]` - list of objects in bucket
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.transform"></a>

### transform

```python
def transform(etl_name: str,
              to_bck: Bucket,
              timeout: str = DEFAULT_ETL_TIMEOUT,
              prefix_filter: str = "",
              prepend: str = "",
              ext: Dict[str, str] = None,
              force: bool = False,
              dry_run: bool = False,
              latest: bool = False,
              sync: bool = False) -> str
```

Visits all selected objects in the source bucket and for each object, puts the transformed
result to the destination bucket

**Arguments**:

- `etl_name` _str_ - name of etl to be used for transformations
- `to_bck` _str_ - destination bucket for transformations
- `timeout` _str, optional_ - Timeout of the ETL job (e.g. 5m for 5 minutes)
- `prefix_filter` _str, optional_ - Only transform objects with names starting with this prefix
- `prepend` _str, optional_ - Value to prepend to the name of resulting transformed objects
- `ext` _Dict[str, str], optional_ - dict of new extension followed by extension to be replaced
  (i.e. {"jpg": "txt"})
- `dry_run` _bool, optional_ - determines if the copy should actually happen or not
- `force` _bool, optional_ - override existing destination bucket
- `latest` _bool, optional_ - GET the latest object version from the associated remote bucket
- `sync` _bool, optional_ - synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="bucket.Bucket.put_files"></a>

### put\_files

```python
def put_files(path: str,
              prefix_filter: str = "",
              pattern: str = "*",
              basename: bool = False,
              prepend: str = None,
              recursive: bool = False,
              dry_run: bool = False,
              verbose: bool = True) -> List[str]
```

Puts files found in a given filepath as objects to a bucket in AIS storage.

**Arguments**:

- `path` _str_ - Local filepath, can be relative or absolute
- `prefix_filter` _str, optional_ - Only put files with names starting with this prefix
- `pattern` _str, optional_ - Shell-style wildcard pattern to filter files
- `basename` _bool, optional_ - Whether to use the file names only as object names and omit the path information
- `prepend` _str, optional_ - Optional string to use as a prefix in the object name for all objects uploaded
  No delimiter ("/", "-", etc.) is automatically applied between the prepend value and the object name
- `recursive` _bool, optional_ - Whether to recurse through the provided path directories
- `dry_run` _bool, optional_ - Option to only show expected behavior without an actual put operation
- `verbose` _bool, optional_ - Whether to print upload info to standard output
  

**Returns**:

  List of object names put to a bucket in AIS
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `ValueError` - The path provided is not a valid directory

<a id="bucket.Bucket.object"></a>

### object

```python
def object(obj_name: str, props: ObjectProps = None) -> Object
```

Factory constructor for an object in this bucket.
Does not make any HTTP request, only instantiates an object in a bucket owned by the client.

**Arguments**:

- `obj_name` _str_ - Name of object
- `props` _ObjectProps, optional_ - Properties of the object, as updated by head(), optionally pre-initialized.
  

**Returns**:

  The object created.

<a id="bucket.Bucket.objects"></a>

### objects

```python
def objects(obj_names: List = None,
            obj_range: ObjectRange = None,
            obj_template: str = None) -> ObjectGroup
```

Factory constructor for multiple objects belonging to this bucket.

**Arguments**:

- `obj_names` _list_ - Names of objects to include in the group
- `obj_range` _ObjectRange_ - Range of objects to include in the group
- `obj_template` _str_ - String template defining objects to include in the group
  

**Returns**:

  The ObjectGroup created

<a id="bucket.Bucket.make_request"></a>

### make\_request

```python
def make_request(method: str,
                 action: str,
                 value: Dict = None,
                 params: Dict = None) -> requests.Response
```

Use the bucket's client to make a request to the bucket endpoint on the AIS server

**Arguments**:

- `method` _str_ - HTTP method to use, e.g. POST/GET/DELETE
- `action` _str_ - Action string used to create an ActionMsg to pass to the server
- `value` _dict_ - Additional value parameter to pass in the ActionMsg
- `params` _dict, optional_ - Optional parameters to pass in the request
  

**Returns**:

  Response from the server

<a id="bucket.Bucket.verify_cloud_bucket"></a>

### verify\_cloud\_bucket

```python
def verify_cloud_bucket()
```

Verify the bucket provider is a cloud provider

<a id="bucket.Bucket.get_path"></a>

### get\_path

```python
def get_path() -> str
```

Get the path representation of this bucket

<a id="bucket.Bucket.as_model"></a>

### as\_model

```python
def as_model() -> BucketModel
```

Return a data-model of the bucket

**Returns**:

  BucketModel representation

<a id="bucket.Bucket.write_dataset"></a>

### write\_dataset

```python
def write_dataset(config: DatasetConfig, skip_missing: bool = True, **kwargs)
```

Write a dataset to a bucket in AIS in webdataset format using wds.ShardWriter. Logs the missing attributes

**Arguments**:

- `config` _DatasetConfig_ - Configuration dict specifying how to process
  and store each part of the dataset item
- `skip_missing` _bool, optional_ - Skip samples that are missing one or more attributes, defaults to True
- `**kwargs` _optional_ - Optional keyword arguments to pass to the ShardWriter

<a id="client.Client"></a>

## Class: Client

```python
class Client()
```

AIStore client for managing buckets, objects, ETL jobs

**Arguments**:

- `endpoint` _str_ - AIStore endpoint
- `skip_verify` _bool, optional_ - If True, skip SSL certificate verification. Defaults to False.
- `ca_cert` _str, optional_ - Path to a CA certificate file for SSL verification. If not provided, the
  'AIS_CLIENT_CA' environment variable will be used. Defaults to None.
- `client_cert` _Union[str, Tuple[str, str], None], optional_ - Path to a client certificate PEM file
  or a path pair (cert, key) for mTLS. If not provided, 'AIS_CRT' and 'AIS_CRT_KEY'
  environment variables will be used. Defaults to None.
- `timeout` _Union[float, Tuple[float, float], None], optional_ - Request timeout in seconds; a single float
  for both connect/read timeouts (e.g., 5.0), a tuple for separate connect/read timeouts (e.g., (3.0, 10.0)),
  or None to disable timeout.
- `retry` _urllib3.Retry, optional_ - Retry configuration object from the urllib3 library.
- `token` _str, optional_ - Authorization token. If not provided, the 'AIS_AUTHN_TOKEN' environment variable
  will be used. Defaults to None.

<a id="client.Client.bucket"></a>

### bucket

```python
def bucket(bck_name: str,
           provider: Union[Provider, str] = Provider.AIS,
           namespace: Namespace = None)
```

Factory constructor for bucket object.
Does not make any HTTP request, only instantiates a bucket object.

**Arguments**:

- `bck_name` _str_ - Name of bucket
- `provider` _str or Provider_ - Provider of bucket, one of "ais", "aws", "gcp", ...
  (optional, defaults to ais)
- `namespace` _Namespace_ - Namespace of bucket (optional, defaults to None)
  

**Returns**:

  The bucket object created.

<a id="client.Client.cluster"></a>

### cluster

```python
def cluster()
```

Factory constructor for cluster object.
Does not make any HTTP request, only instantiates a cluster object.

**Returns**:

  The cluster object created.

<a id="client.Client.job"></a>

### job

```python
def job(job_id: str = "", job_kind: str = "")
```

Factory constructor for job object, which contains job-related functions.
Does not make any HTTP request, only instantiates a job object.

**Arguments**:

- `job_id` _str, optional_ - Optional ID for interacting with a specific job
- `job_kind` _str, optional_ - Optional specific type of job empty for all kinds
  

**Returns**:

  The job object created.

<a id="client.Client.etl"></a>

### etl

```python
def etl(etl_name: str)
```

Factory constructor for ETL object.
Contains APIs related to AIStore ETL operations.
Does not make any HTTP request, only instantiates an ETL object.

**Arguments**:

- `etl_name` _str_ - Name of the ETL
  

**Returns**:

  The ETL object created.

<a id="client.Client.dsort"></a>

### dsort

```python
def dsort(dsort_id: str = "")
```

Factory constructor for dSort object.
Contains APIs related to AIStore dSort operations.
Does not make any HTTP request, only instantiates a dSort object.

**Arguments**:

- `dsort_id` - ID of the dSort job
  

**Returns**:

  dSort object created

<a id="client.Client.fetch_object_by_url"></a>

### fetch\_object\_by\_url

```python
def fetch_object_by_url(url: str) -> Object
```

Retrieve an object based on its URL.

**Arguments**:

- `url` _str_ - Full URL of the object (e.g., "ais://bucket1/file.txt")
  

**Returns**:

- `Object` - The object retrieved from the specified URL

<a id="cluster.Cluster"></a>

## Class: Cluster

```python
class Cluster()
```

A class representing a cluster bound to an AIS client.

<a id="cluster.Cluster.client"></a>

### client

```python
@property
def client()
```

Client this cluster uses to make requests

<a id="cluster.Cluster.get_info"></a>

### get\_info

```python
def get_info() -> Smap
```

Returns state of AIS cluster, including the detailed information about its nodes.

**Returns**:

- `aistore.sdk.types.Smap` - Smap containing cluster information
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="cluster.Cluster.get_primary_url"></a>

### get\_primary\_url

```python
def get_primary_url() -> str
```

Returns: URL of primary proxy

<a id="cluster.Cluster.list_buckets"></a>

### list\_buckets

```python
def list_buckets(provider: Union[str, Provider] = Provider.AIS)
```

Returns list of buckets in AIStore cluster.

**Arguments**:

- `provider` _str or Provider, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az" or "ht".
  Defaults to "ais". Empty provider returns buckets of all providers.
  

**Returns**:

- `List[BucketModel]` - A list of buckets
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="cluster.Cluster.list_jobs_status"></a>

### list\_jobs\_status

```python
def list_jobs_status(job_kind="", target_id="") -> List[JobStatus]
```

List the status of jobs on the cluster

**Arguments**:

- `job_kind` _str, optional_ - Only show jobs of a particular type
- `target_id` _str, optional_ - Limit to jobs on a specific target node
  

**Returns**:

  List of JobStatus objects

<a id="cluster.Cluster.list_running_jobs"></a>

### list\_running\_jobs

```python
def list_running_jobs(job_kind="", target_id="") -> List[str]
```

List the currently running jobs on the cluster

**Arguments**:

- `job_kind` _str, optional_ - Only show jobs of a particular type
- `target_id` _str, optional_ - Limit to jobs on a specific target node
  

**Returns**:

  List of jobs in the format job_kind[job_id]

<a id="cluster.Cluster.list_running_etls"></a>

### list\_running\_etls

```python
def list_running_etls() -> List[ETLInfo]
```

Lists all running ETLs.

Note: Does not list ETLs that have been stopped or deleted.

**Returns**:

- `List[ETLInfo]` - A list of details on running ETLs

<a id="cluster.Cluster.is_ready"></a>

### is\_ready

```python
def is_ready() -> bool
```

Checks if cluster is ready or still setting up.

**Returns**:

- `bool` - True if cluster is ready, or false if cluster is still setting up

<a id="cluster.Cluster.get_performance"></a>

### get\_performance

```python
def get_performance(get_throughput: bool = True,
                    get_latency: bool = True,
                    get_counters: bool = True) -> ClusterPerformance
```

Retrieves and calculates the performance metrics for each target node in the AIStore cluster.
It compiles throughput, latency, and various operational counters from each target node,
providing a comprehensive view of the cluster's overall performance

**Arguments**:

- `get_throughput` _bool, optional_ - get cluster throughput
- `get_latency` _bool, optional_ - get cluster latency
- `get_counters` _bool, optional_ - get cluster counters
  

**Returns**:

- `ClusterPerformance` - An object encapsulating the detailed performance metrics of the cluster,
  including throughput, latency, and counters for each node
  

**Raises**:

- `requests.RequestException` - If there's an ambiguous exception while processing the request
- `requests.ConnectionError` - If there's a connection error with the cluster
- `requests.ConnectionTimeout` - If the connection to the cluster times out
- `requests.ReadTimeout` - If the timeout is reached while awaiting a response from the cluster

<a id="cluster.Cluster.get_uuid"></a>

### get\_uuid

```python
def get_uuid() -> str
```

Returns: UUID of AIStore Cluster

<a id="job.Job"></a>

## Class: Job

```python
class Job()
```

A class containing job-related functions.

**Arguments**:

- `client` _RequestClient_ - Client for interfacing with AIS cluster
- `job_id` _str, optional_ - ID of a specific job, empty for all jobs
- `job_kind` _str, optional_ - Specific kind of job, empty for all kinds

<a id="job.Job.job_id"></a>

### job\_id

```python
@property
def job_id()
```

Return job id

<a id="job.Job.job_kind"></a>

### job\_kind

```python
@property
def job_kind()
```

Return job kind

<a id="job.Job.status"></a>

### status

```python
def status() -> JobStatus
```

Return status of a job

**Returns**:

  The job status including id, finish time, and error info.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="job.Job.wait"></a>

### wait

```python
def wait(timeout: int = DEFAULT_JOB_WAIT_TIMEOUT, verbose: bool = True)
```

Wait for a job to finish

**Arguments**:

- `timeout` _int, optional_ - The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
- `verbose` _bool, optional_ - Whether to log wait status to standard output
  

**Returns**:

  None
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `errors.Timeout` - Timeout while waiting for the job to finish

<a id="job.Job.wait_for_idle"></a>

### wait\_for\_idle

```python
def wait_for_idle(timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
                  verbose: bool = True)
```

Wait for a job to reach an idle state

**Arguments**:

- `timeout` _int, optional_ - The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
- `verbose` _bool, optional_ - Whether to log wait status to standard output
  

**Returns**:

  None
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `errors.Timeout` - Timeout while waiting for the job to finish
- `errors.JobInfoNotFound` - Raised when information on a job's status could not be found on the AIS cluster

<a id="job.Job.wait_single_node"></a>

### wait\_single\_node

```python
def wait_single_node(timeout: int = DEFAULT_JOB_WAIT_TIMEOUT,
                     verbose: bool = True)
```

Wait for a job running on a single node

**Arguments**:

- `timeout` _int, optional_ - The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
- `verbose` _bool, optional_ - Whether to log wait status to standard output
  

**Returns**:

  None
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `errors.Timeout` - Timeout while waiting for the job to finish
- `errors.JobInfoNotFound` - Raised when information on a job's status could not be found on the AIS cluster

<a id="job.Job.start"></a>

### start

```python
def start(daemon_id: str = "",
          force: bool = False,
          buckets: List[Bucket] = None) -> str
```

Start a job and return its ID.

**Arguments**:

- `daemon_id` _str, optional_ - For running a job that must run on a specific target node (e.g. resilvering).
- `force` _bool, optional_ - Override existing restrictions for a bucket (e.g., run LRU eviction even if the
  bucket has LRU disabled).
- `buckets` _List[Bucket], optional_ - List of one or more buckets; applicable only for jobs that have bucket
  scope (for details on job types, see `Table` in xact/api.go).
  

**Returns**:

  The running job ID.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="job.Job.get_within_timeframe"></a>

### get\_within\_timeframe

```python
def get_within_timeframe(
        start_time: datetime,
        end_time: Optional[datetime] = None) -> List[JobSnapshot]
```

Retrieves jobs that started after a specified start_time and optionally ended before a specified end_time.

**Arguments**:

- `start_time` _datetime_ - The start of the timeframe for monitoring jobs.
- `end_time` _datetime, optional_ - The end of the timeframe for monitoring jobs.
  

**Returns**:

- `List[JobSnapshot]` - A list of jobs that meet the specified timeframe criteria.
  

**Raises**:

- `JobInfoNotFound` - Raised when no relevant job info is found.

<a id="multiobj.object_group.ObjectGroup"></a>

## Class: ObjectGroup

```python
class ObjectGroup(AISSource)
```

A class representing multiple objects within the same bucket. Only one of obj_names, obj_range, or obj_template
should be provided.

**Arguments**:

- `bck` _Bucket_ - Bucket the objects belong to
- `obj_names` _list[str], optional_ - List of object names to include in this collection
- `obj_range` _ObjectRange, optional_ - Range defining which object names in the bucket should be included
- `obj_template` _str, optional_ - String argument to pass as template value directly to api

<a id="multiobj.object_group.ObjectGroup.client"></a>

### client

```python
@property
def client() -> RequestClient
```

The client bound to the bucket used by the ObjectGroup.

<a id="multiobj.object_group.ObjectGroup.client"></a>

### client

```python
@client.setter
def client(client) -> RequestClient
```

Update the client bound to the bucket used by the ObjectGroup.

<a id="multiobj.object_group.ObjectGroup.list_urls"></a>

### list\_urls

```python
def list_urls(prefix: str = "", etl_name: str = None) -> Iterable[str]
```

Implementation of the abstract method from AISSource that provides an iterator
of full URLs to every object in this bucket matching the specified prefix

**Arguments**:

- `prefix` _str, optional_ - Limit objects selected by a given string prefix
- `etl_name` _str, optional_ - ETL to include in URLs
  

**Returns**:

  Iterator of all object URLs in the group

<a id="multiobj.object_group.ObjectGroup.list_all_objects_iter"></a>

### list\_all\_objects\_iter

```python
def list_all_objects_iter(prefix: str = "",
                          props: str = "name,size") -> Iterable[Object]
```

Implementation of the abstract method from AISSource that provides an iterator
of all the objects in this bucket matching the specified prefix.

**Arguments**:

- `prefix` _str, optional_ - Limit objects selected by a given string prefix
- `props` _str, optional_ - By default, will include all object properties.
  Pass in None to skip and avoid the extra API call.
  

**Returns**:

  Iterator of all the objects in the group

<a id="multiobj.object_group.ObjectGroup.delete"></a>

### delete

```python
def delete()
```

Deletes a list or range of objects in a bucket

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="multiobj.object_group.ObjectGroup.evict"></a>

### evict

```python
def evict()
```

Evicts a list or range of objects in a bucket so that they are no longer cached in AIS
NOTE: only Cloud buckets can be evicted.

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="multiobj.object_group.ObjectGroup.prefetch"></a>

### prefetch

```python
def prefetch(blob_threshold: int = None,
             num_workers: int = None,
             latest: bool = False,
             continue_on_error: bool = False)
```

Prefetches a list or range of objects in a bucket so that they are cached in AIS
NOTE: only Cloud buckets can be prefetched.

**Arguments**:

- `latest` _bool, optional_ - GET the latest object version from the associated remote bucket
- `continue_on_error` _bool, optional_ - Whether to continue if there is an error prefetching a single object
- `blob_threshold` _int, optional_ - Utilize built-in blob-downloader for remote objects
  greater than the specified (threshold) size in bytes
- `num_workers` _int, optional_ - Number of concurrent workers (readers). Defaults to the number of target
  mountpaths if omitted or zero. A value of -1 indicates no workers at all (i.e., single-threaded
  execution). Any positive value will be adjusted not to exceed the number of target CPUs.
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="multiobj.object_group.ObjectGroup.copy"></a>

### copy

```python
def copy(to_bck: "Bucket",
         prepend: str = "",
         continue_on_error: bool = False,
         dry_run: bool = False,
         force: bool = False,
         latest: bool = False,
         sync: bool = False,
         num_workers: int = None)
```

Copies a list or range of objects in a bucket

**Arguments**:

- `to_bck` _Bucket_ - Destination bucket
- `prepend` _str, optional_ - Value to prepend to the name of copied objects
- `continue_on_error` _bool, optional_ - Whether to continue if there is an error copying a single object
- `dry_run` _bool, optional_ - Skip performing the copy and just log the intended actions
- `force` _bool, optional_ - Force this job to run over others in case it conflicts
  (see "limited coexistence" and xact/xreg/xreg.go)
- `latest` _bool, optional_ - GET the latest object version from the associated remote bucket
- `sync` _bool, optional_ - synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source
- `num_workers` _int, optional_ - Number of concurrent workers (readers). Defaults to the number of target
  mountpaths if omitted or zero. A value of -1 indicates no workers at all (i.e., single-threaded
  execution). Any positive value will be adjusted not to exceed the number of target CPUs.
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="multiobj.object_group.ObjectGroup.transform"></a>

### transform

```python
def transform(to_bck: "Bucket",
              etl_name: str,
              timeout: str = DEFAULT_ETL_TIMEOUT,
              prepend: str = "",
              continue_on_error: bool = False,
              dry_run: bool = False,
              force: bool = False,
              latest: bool = False,
              sync: bool = False,
              num_workers: int = None)
```

Performs ETL operation on a list or range of objects in a bucket, placing the results in the destination bucket

**Arguments**:

- `to_bck` _Bucket_ - Destination bucket
- `etl_name` _str_ - Name of existing ETL to apply
- `timeout` _str_ - Timeout of the ETL job (e.g. 5m for 5 minutes)
- `prepend` _str, optional_ - Value to prepend to the name of resulting transformed objects
- `continue_on_error` _bool, optional_ - Whether to continue if there is an error transforming a single object
- `dry_run` _bool, optional_ - Skip performing the transform and just log the intended actions
- `force` _bool, optional_ - Force this job to run over others in case it conflicts
  (see "limited coexistence" and xact/xreg/xreg.go)
- `latest` _bool, optional_ - GET the latest object version from the associated remote bucket
- `sync` _bool, optional_ - synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source
- `num_workers` _int, optional_ - Number of concurrent workers (readers). Defaults to the number of target
  mountpaths if omitted or zero. A value of -1 indicates no workers at all (i.e., single-threaded
  execution). Any positive value will be adjusted not to exceed the number of target CPUs.
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="multiobj.object_group.ObjectGroup.archive"></a>

### archive

```python
def archive(archive_name: str,
            mime: str = "",
            to_bck: "Bucket" = None,
            include_source_name: bool = False,
            allow_append: bool = False,
            continue_on_err: bool = False)
```

Create or append to an archive

**Arguments**:

- `archive_name` _str_ - Name of archive to create or append
- `mime` _str, optional_ - MIME type of the content
- `to_bck` _Bucket, optional_ - Destination bucket, defaults to current bucket
- `include_source_name` _bool, optional_ - Include the source bucket name in the archived objects' names
- `allow_append` _bool, optional_ - Allow appending to an existing archive
- `continue_on_err` _bool, optional_ - Whether to continue if there is an error archiving a single object
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="multiobj.object_group.ObjectGroup.list_names"></a>

### list\_names

```python
def list_names() -> List[str]
```

List all the object names included in this group of objects

**Returns**:

  List of object names

<a id="multiobj.object_names.ObjectNames"></a>

## Class: ObjectNames

```python
class ObjectNames(ObjectCollection)
```

A collection of object names, provided as a list of strings

**Arguments**:

- `names` _List[str]_ - A list of object names

<a id="multiobj.object_range.ObjectRange"></a>

## Class: ObjectRange

```python
class ObjectRange(ObjectCollection)
```

Class representing a range of object names

**Arguments**:

- `prefix` _str_ - Prefix contained in all names of objects
- `min_index` _int_ - Starting index in the name of objects
- `max_index` _int_ - Last index in the name of all objects
- `pad_width` _int, optional_ - Left-pad indices with zeros up to the width provided, e.g. pad_width = 3 will
  transform 1 to 001
- `step` _int, optional_ - Size of iterator steps between each item
- `suffix` _str, optional_ - Suffix at the end of all object names

<a id="multiobj.object_range.ObjectRange.from_string"></a>

### from\_string

```python
@classmethod
def from_string(cls, range_string: str)
```

Construct an ObjectRange instance from a valid range string like 'input-{00..99..1}.txt'

**Arguments**:

- `range_string` _str_ - The range string to parse
  

**Returns**:

- `ObjectRange` - An instance of the ObjectRange class

<a id="multiobj.object_template.ObjectTemplate"></a>

## Class: ObjectTemplate

```python
class ObjectTemplate(ObjectCollection)
```

A collection of object names specified by a template in the bash brace expansion format

**Arguments**:

- `template` _str_ - A string template that defines the names of objects to include in the collection

<a id="obj.object.BucketDetails"></a>

## Class: BucketDetails

```python
@dataclass
class BucketDetails()
```

Metadata about a bucket, used by objects within that bucket.

<a id="obj.object.Object"></a>

## Class: Object

```python
class Object()
```

Provides methods for interacting with an object in AIS.

**Arguments**:

- `client` _RequestClient_ - Client used for all http requests.
- `bck_details` _BucketDetails_ - Metadata about the bucket to which this object belongs.
- `name` _str_ - Name of the object.
- `props` _ObjectProps, optional_ - Properties of the object, as updated by head(), optionally pre-initialized.

<a id="obj.object.Object.bucket_name"></a>

### bucket\_name

```python
@property
def bucket_name() -> str
```

Name of the bucket where this object resides.

<a id="obj.object.Object.bucket_provider"></a>

### bucket\_provider

```python
@property
def bucket_provider() -> Provider
```

Provider of the bucket where this object resides (e.g. ais, s3, gcp).

<a id="obj.object.Object.query_params"></a>

### query\_params

```python
@property
def query_params() -> Dict[str, str]
```

Query params used as a base for constructing all requests for this object.

<a id="obj.object.Object.name"></a>

### name

```python
@property
def name() -> str
```

Name of this object.

<a id="obj.object.Object.props"></a>

### props

```python
@property
def props() -> ObjectProps
```

Properties of this object.

<a id="obj.object.Object.head"></a>

### head

```python
def head() -> CaseInsensitiveDict
```

Requests object properties and returns headers. Updates props.

**Returns**:

  Response header with the object properties.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exceptions.HTTPError(404)` - The object does not exist

<a id="obj.object.Object.get_reader"></a>

### get\_reader

```python
def get_reader(archive_config: ArchiveConfig = None,
               blob_download_config: BlobDownloadConfig = None,
               chunk_size: int = DEFAULT_CHUNK_SIZE,
               etl_name: str = None,
               writer: BufferedWriter = None,
               latest: bool = False,
               byte_range: str = None) -> ObjectReader
```

Creates and returns an ObjectReader with access to object contents and optionally writes to a provided writer.

**Arguments**:

- `archive_config` _ArchiveConfig, optional_ - Settings for archive extraction
- `blob_download_config` _BlobDownloadConfig, optional_ - Settings for using blob download
- `chunk_size` _int, optional_ - chunk_size to use while reading from stream
- `etl_name` _str, optional_ - Transforms an object based on ETL with etl_name
- `writer` _BufferedWriter, optional_ - User-provided writer for writing content output
  User is responsible for closing the writer
- `latest` _bool, optional_ - GET the latest object version from the associated remote bucket
- `byte_range` _str, optional_ - Specify a specific data segment of the object for transfer, including
  both the start and end of the range (e.g. "bytes=0-499" to request the first 500 bytes)
  

**Returns**:

  An ObjectReader which can be iterated over to stream chunks of object content or used to read all content
  directly.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="obj.object.Object.get"></a>

### get

```python
def get(archive_config: ArchiveConfig = None,
        blob_download_config: BlobDownloadConfig = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_name: str = None,
        writer: BufferedWriter = None,
        latest: bool = False,
        byte_range: str = None) -> ObjectReader
```

Deprecated: Use 'get_reader' instead.

Creates and returns an ObjectReader with access to object contents and optionally writes to a provided writer.

**Arguments**:

- `archive_config` _ArchiveConfig, optional_ - Settings for archive extraction
- `blob_download_config` _BlobDownloadConfig, optional_ - Settings for using blob download
- `chunk_size` _int, optional_ - chunk_size to use while reading from stream
- `etl_name` _str, optional_ - Transforms an object based on ETL with etl_name
- `writer` _BufferedWriter, optional_ - User-provided writer for writing content output
  User is responsible for closing the writer
- `latest` _bool, optional_ - GET the latest object version from the associated remote bucket
- `byte_range` _str, optional_ - Specify a specific data segment of the object for transfer, including
  both the start and end of the range (e.g. "bytes=0-499" to request the first 500 bytes)
  

**Returns**:

  An ObjectReader which can be iterated over to stream chunks of object content or used to read all content
  directly.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="obj.object.Object.get_semantic_url"></a>

### get\_semantic\_url

```python
def get_semantic_url() -> str
```

Get the semantic URL to the object

**Returns**:

  Semantic URL to get object

<a id="obj.object.Object.get_url"></a>

### get\_url

```python
def get_url(archpath: str = "", etl_name: str = None) -> str
```

Get the full url to the object including base url and any query parameters

**Arguments**:

- `archpath` _str, optional_ - If the object is an archive, use `archpath` to extract a single file
  from the archive
- `etl_name` _str, optional_ - Transforms an object based on ETL with etl_name
  

**Returns**:

  Full URL to get object

<a id="obj.object.Object.put_content"></a>

### put\_content

```python
def put_content(content: bytes) -> Response
```

Deprecated: Use 'ObjectWriter.put_content' instead.

Puts bytes as an object to a bucket in AIS storage.

**Arguments**:

- `content` _bytes_ - Bytes to put as an object.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="obj.object.Object.put_file"></a>

### put\_file

```python
def put_file(path: str or Path) -> Response
```

Deprecated: Use 'ObjectWriter.put_file' instead.

Puts a local file as an object to a bucket in AIS storage.

**Arguments**:

- `path` _str or Path_ - Path to local file
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `ValueError` - The path provided is not a valid file

<a id="obj.object.Object.get_writer"></a>

### get\_writer

```python
def get_writer() -> ObjectWriter
```

Create an ObjectWriter to write to object contents and attributes.

**Returns**:

  An ObjectWriter which can be used to write to an object's contents and attributes.

<a id="obj.object.Object.promote"></a>

### promote

```python
def promote(path: str,
            target_id: str = "",
            recursive: bool = False,
            overwrite_dest: bool = False,
            delete_source: bool = False,
            src_not_file_share: bool = False) -> str
```

Promotes a file or folder an AIS target can access to a bucket in AIS storage.
These files can be either on the physical disk of an AIS target itself or on a network file system
the cluster can access.
See more info here: https://aiatscale.org/blog/2022/03/17/promote

**Arguments**:

- `path` _str_ - Path to file or folder the AIS cluster can reach
- `target_id` _str, optional_ - Promote files from a specific target node
- `recursive` _bool, optional_ - Recursively promote objects from files in directories inside the path
- `overwrite_dest` _bool, optional_ - Overwrite objects already on AIS
- `delete_source` _bool, optional_ - Delete the source files when done promoting
- `src_not_file_share` _bool, optional_ - Optimize if the source is guaranteed to not be on a file share
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation, or empty if job is done synchronously
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `AISError` - Path does not exist on the AIS cluster storage

<a id="obj.object.Object.delete"></a>

### delete

```python
def delete() -> Response
```

Delete an object from a bucket.

**Returns**:

  None
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exceptions.HTTPError(404)` - The object does not exist

<a id="obj.object.Object.blob_download"></a>

### blob\_download

```python
def blob_download(chunk_size: int = None,
                  num_workers: int = None,
                  latest: bool = False) -> str
```

A special facility to download very large remote objects a.k.a. BLOBs
Returns job ID that for the blob download operation.

**Arguments**:

- `chunk_size` _int_ - chunk size in bytes
- `num_workers` _int_ - number of concurrent blob-downloading workers (readers)
- `latest` _bool_ - GET the latest object version from the associated remote bucket
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."

<a id="obj.object.Object.append_content"></a>

### append\_content

```python
def append_content(content: bytes,
                   handle: str = "",
                   flush: bool = False) -> str
```

Deprecated: Use 'ObjectWriter.append_content' instead.

Append bytes as an object to a bucket in AIS storage.

**Arguments**:

- `content` _bytes_ - Bytes to append to the object.
- `handle` _str_ - Handle string to use for subsequent appends or flush (empty for the first append).
- `flush` _bool_ - Whether to flush and finalize the append operation, making the object accessible.
  

**Returns**:

- `handle` _str_ - Handle string to pass for subsequent appends or flush.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exceptions.HTTPError(404)` - The object does not exist

<a id="obj.object.Object.set_custom_props"></a>

### set\_custom\_props

```python
def set_custom_props(custom_metadata: Dict[str, str],
                     replace_existing: bool = False) -> Response
```

Deprecated: Use 'ObjectWriter.set_custom_props' instead.

Set custom properties for the object.

**Arguments**:

- `custom_metadata` _Dict[str, str]_ - Custom metadata key-value pairs.
- `replace_existing` _bool, optional_ - Whether to replace existing metadata. Defaults to False.

<a id="obj.object_reader.ObjectReader"></a>

## Class: ObjectReader

```python
class ObjectReader()
```

Provide a way to read an object's contents and attributes, optionally iterating over a stream of content.

**Arguments**:

- `object_client` _ObjectClient_ - Client for making requests to a specific object in AIS
- `chunk_size` _int, optional_ - Size of each data chunk to be fetched from the stream.
  Defaults to DEFAULT_CHUNK_SIZE.

<a id="obj.object_reader.ObjectReader.head"></a>

### head

```python
def head() -> ObjectAttributes
```

Make a head request to AIS to update and return only object attributes.

**Returns**:

  `ObjectAttributes` containing metadata for this object.

<a id="obj.object_reader.ObjectReader.attributes"></a>

### attributes

```python
@property
def attributes() -> ObjectAttributes
```

Object metadata attributes.

**Returns**:

- `ObjectAttributes` - Parsed object attributes from the headers returned by AIS.

<a id="obj.object_reader.ObjectReader.read_all"></a>

### read\_all

```python
def read_all() -> bytes
```

Read all byte data directly from the object response without using a stream.

This requires all object content to fit in memory at once and downloads all content before returning.

**Returns**:

- `bytes` - Object content as bytes.

<a id="obj.object_reader.ObjectReader.raw"></a>

### raw

```python
def raw() -> requests.Response
```

Return the raw byte stream of object content.

**Returns**:

- `requests.Response` - Raw byte stream of the object content.

<a id="obj.object_reader.ObjectReader.as_file"></a>

### as\_file

```python
def as_file(buffer_size: Optional[int] = None,
            max_resume: Optional[int] = 5) -> BufferedIOBase
```

Create a read-only, non-seekable `ObjectFile` instance for streaming object data in chunks.
This file-like object primarily implements the `read()` method to retrieve data sequentially,
with automatic retry/resumption in case of stream interruptions such as `ChunkedEncodingError`.

**Arguments**:

- `buffer_size` _int, optional_ - Currently unused; retained for backward compatibility and future
  enhancements.
- `max_resume` _int, optional_ - Total number of retry attempts allowed to resume the stream in case of
  interruptions. Defaults to 5.
  

**Returns**:

- `BufferedIOBase` - A read-only, non-seekable file-like object for streaming object content.
  

**Raises**:

- `ValueError` - If `max_resume` is invalid (must be a non-negative integer).

<a id="obj.object_reader.ObjectReader.iter_from_position"></a>

### iter\_from\_position

```python
def iter_from_position(start_position: int = 0) -> Iterator[bytes]
```

Make a request to get a stream from the provided object starting at a specific byte position
and yield chunks of the stream content.

**Arguments**:

- `start_position` _int, optional_ - The byte position to start reading from. Defaults to 0.
  

**Returns**:

- `Iterator[bytes]` - An iterator over each chunk of bytes in the object starting from the specific position.

<a id="obj.object_reader.ObjectReader.__iter__"></a>

### \_\_iter\_\_

```python
def __iter__() -> Iterator[bytes]
```

Make a request to get a stream from the provided object and yield chunks of the stream content.

**Returns**:

- `Iterator[bytes]` - An iterator over each chunk of bytes in the object.

<a id="obj.obj_file.object_file.ObjectFile"></a>

## Class: ObjectFile

```python
class ObjectFile(BufferedIOBase)
```

A sequential read-only file-like object extending `BufferedIOBase` for reading object data, with support for both
reading a fixed size of data and reading until the end of file (EOF).

When a read is requested, any remaining data from a previously fetched chunk is returned first. If the remaining
data is insufficient to satisfy the request, the `read()` method fetches additional chunks from the provided
`content_iterator` as needed, until the requested size is fulfilled or the end of the stream is reached.

In case of stream interruptions (e.g., `ChunkedEncodingError`), the `read()` method automatically retries and
resumes fetching data from the last successfully retrieved chunk. The `max_resume` parameter controls how many
retry attempts are made before an error is raised.

**Arguments**:

- `content_iterator` _ContentIterator_ - An iterator that can fetch object data from AIS in chunks.
- `max_resume` _int_ - Maximum number of resumes allowed for an ObjectFile instance.

<a id="obj.obj_file.object_file.ObjectFile.readable"></a>

### readable

```python
@override
def readable() -> bool
```

Return whether the file is readable.

<a id="obj.obj_file.object_file.ObjectFile.read"></a>

### read

```python
@override
def read(size: Optional[int] = -1) -> bytes
```

Read up to 'size' bytes from the object. If size is -1, read until the end of the stream.

**Arguments**:

- `size` _int, optional_ - The number of bytes to read. If -1, reads until EOF.
  

**Returns**:

- `bytes` - The read data as a bytes object.
  

**Raises**:

  ObjectFileStreamError if a connection cannot be made.
  ObjectFileMaxResumeError if the stream is interrupted more than the allowed maximum.
- `ValueError` - I/O operation on a closed file.
- `Exception` - Any other errors while streaming and reading.

<a id="obj.obj_file.object_file.ObjectFile.close"></a>

### close

```python
@override
def close() -> None
```

Close the file.

<a id="obj.obj_file.object_file.ObjectFileWriter"></a>

## Class: ObjectFileWriter

```python
class ObjectFileWriter(BufferedWriter)
```

A file-like writer object for AIStore, extending `BufferedWriter`.

**Arguments**:

- `obj_writer` _ObjectWriter_ - The ObjectWriter instance for handling write operations.
- `mode` _str_ - Specifies the mode in which the file is opened.
  - `'w'`: Write mode. Opens the object for writing, truncating any existing content.
  Writing starts from the beginning of the object.
  - `'a'`: Append mode. Opens the object for appending. Existing content is preserved,
  and writing starts from the end of the object.

<a id="obj.obj_file.object_file.ObjectFileWriter.write"></a>

### write

```python
@override
def write(buffer: bytes) -> int
```

Write data to the object.

**Arguments**:

- `data` _bytes_ - The data to write.
  

**Returns**:

- `int` - Number of bytes written.
  

**Raises**:

- `ValueError` - I/O operation on a closed file.

<a id="obj.obj_file.object_file.ObjectFileWriter.flush"></a>

### flush

```python
@override
def flush() -> None
```

Flush the writer, ensuring the object is finalized.

This does not close the writer but makes the current state accessible.

**Raises**:

- `ValueError` - I/O operation on a closed file.

<a id="obj.obj_file.object_file.ObjectFileWriter.close"></a>

### close

```python
@override
def close() -> None
```

Close the writer and finalize the object.

<a id="obj.object_props.ObjectProps"></a>

## Class: ObjectProps

```python
class ObjectProps(ObjectAttributes)
```

Represents the attributes parsed from the response headers returned from an API call to get an object.
Extends ObjectAtributes and is a superset of that class.

**Arguments**:

- `response_headers` _CaseInsensitiveDict, optional_ - Response header dict containing object attributes

<a id="obj.object_props.ObjectProps.bucket_name"></a>

### bucket\_name

```python
@property
def bucket_name()
```

Name of object's bucket

<a id="obj.object_props.ObjectProps.bucket_provider"></a>

### bucket\_provider

```python
@property
def bucket_provider()
```

Provider of object's bucket.

<a id="obj.object_props.ObjectProps.name"></a>

### name

```python
@property
def name() -> str
```

Name of the object.

<a id="obj.object_props.ObjectProps.location"></a>

### location

```python
@property
def location() -> str
```

Location of the object.

<a id="obj.object_props.ObjectProps.mirror_paths"></a>

### mirror\_paths

```python
@property
def mirror_paths() -> List[str]
```

List of mirror paths.

<a id="obj.object_props.ObjectProps.mirror_copies"></a>

### mirror\_copies

```python
@property
def mirror_copies() -> int
```

Number of mirror copies.

<a id="obj.object_props.ObjectProps.present"></a>

### present

```python
@property
def present() -> bool
```

True if object is present in cluster.

<a id="obj.object_attributes.ObjectAttributes"></a>

## Class: ObjectAttributes

```python
class ObjectAttributes()
```

Represents the attributes parsed from the response headers returned from an API call to get an object.

**Arguments**:

- `response_headers` _CaseInsensitiveDict_ - Response header dict containing object attributes

<a id="obj.object_attributes.ObjectAttributes.size"></a>

### size

```python
@property
def size() -> int
```

Size of object content.

<a id="obj.object_attributes.ObjectAttributes.checksum_type"></a>

### checksum\_type

```python
@property
def checksum_type() -> str
```

Type of checksum, e.g. xxhash or md5.

<a id="obj.object_attributes.ObjectAttributes.checksum_value"></a>

### checksum\_value

```python
@property
def checksum_value() -> str
```

Checksum value.

<a id="obj.object_attributes.ObjectAttributes.access_time"></a>

### access\_time

```python
@property
def access_time() -> str
```

Time this object was accessed.

<a id="obj.object_attributes.ObjectAttributes.obj_version"></a>

### obj\_version

```python
@property
def obj_version() -> str
```

Object version.

<a id="obj.object_attributes.ObjectAttributes.custom_metadata"></a>

### custom\_metadata

```python
@property
def custom_metadata() -> Dict[str, str]
```

Dictionary of custom metadata.

