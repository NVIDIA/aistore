---
layout: post
title: PYTHON API
permalink: /docs/python-api
redirect_from:
 - /python_api.md/
 - /docs/python_api.md/
---

AIStore Python SDK is a growing set of client-side APIs to access and utilize AIS clusters.

* [api](#api)
  * [Client](#api.Client)
    * [bucket](#api.Client.bucket)
    * [cluster](#api.Client.cluster)
    * [xaction](#api.Client.xaction)
    * [list\_objects\_iter](#api.Client.list_objects_iter)
    * [get\_object](#api.Client.get_object)
* [cluster](#cluster)
  * [Cluster](#cluster.Cluster)
    * [client](#cluster.Cluster.client)
    * [get\_info](#cluster.Cluster.get_info)
    * [list\_buckets](#cluster.Cluster.list_buckets)
    * [is\_aistore\_running](#cluster.Cluster.is_aistore_running)
* [bucket](#bucket)
  * [Bucket](#bucket.Bucket)
    * [client](#bucket.Bucket.client)
    * [bck](#bucket.Bucket.bck)
    * [qparam](#bucket.Bucket.qparam)
    * [provider](#bucket.Bucket.provider)
    * [name](#bucket.Bucket.name)
    * [namespace](#bucket.Bucket.namespace)
    * [create](#bucket.Bucket.create)
    * [delete](#bucket.Bucket.delete)
    * [rename](#bucket.Bucket.rename)
    * [evict](#bucket.Bucket.evict)
    * [head](#bucket.Bucket.head)
    * [copy](#bucket.Bucket.copy)
    * [list\_objects](#bucket.Bucket.list_objects)
    * [list\_objects\_iter](#bucket.Bucket.list_objects_iter)
    * [list\_all\_objects](#bucket.Bucket.list_all_objects)
    * [object](#bucket.Bucket.object)
* [object](#object)
  * [Object](#object.Object)
    * [bck](#object.Object.bck)
    * [obj\_name](#object.Object.obj_name)
    * [head](#object.Object.head)
    * [get](#object.Object.get)
    * [put](#object.Object.put)
    * [delete](#object.Object.delete)

<a id="api.Client"></a>

## Class: Client

```python
class Client()
```

AIStore client for managing buckets, objects, ETL jobs

**Arguments**:

- `endpoint` _str_ - AIStore endpoint

<a id="api.Client.bucket"></a>

### bucket

```python
def bucket(bck_name: str, provider: str = ProviderAIS, ns: str = "")
```

Factory constructor for bucket object.
Does not make any HTTP request, only instantiates a bucket object owned by the client.

**Arguments**:

- `bck_name` _str_ - Name of bucket (optional, defaults to "ais").
- `provider` _str_ - Provider of bucket (one of "ais", "aws", "gcp", ...).
  

**Returns**:

  The bucket object created.

<a id="api.Client.cluster"></a>

### cluster

```python
def cluster()
```

Factory constructor for cluster object.
Does not make any HTTP request, only instantiates a cluster object owned by the client.

**Arguments**:

  None
  

**Returns**:

  The cluster object created.

<a id="api.Client.xaction"></a>

### xaction

```python
def xaction()
```

Factory constructor for xaction object, which contains xaction-related functions.
Does not make any HTTP request, only instantiates an xaction object bound to the client.

**Arguments**:

  None
  

**Returns**:

  The xaction object created.

<a id="api.Client.list_objects_iter"></a>

### list\_objects\_iter

```python
def list_objects_iter(bck_name: str,
                      provider: str = ProviderAIS,
                      prefix: str = "",
                      props: str = "",
                      page_size: int = 0) -> BucketLister
```

Returns an iterator for all objects in a bucket

**Arguments**:

- `bck_name` _str_ - Name of a bucket
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  Defaults to "ais". Empty provider returns buckets of all providers.
- `prefix` _str, optional_ - return only objects that start with the prefix
- `props` _str, optional_ - comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".

**Returns**:

- `BucketLister` - object iterator

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="api.Client.get_object"></a>

### get\_object

```python
def get_object(bck_name: str,
               obj_name: str,
               provider: str = ProviderAIS,
               archpath: str = "",
               chunk_size: int = 1) -> ObjStream
```

Reads an object

**Arguments**:

- `bck_name` _str_ - Name of a bucket
- `obj_name` _str_ - Name of an object in the bucket
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
- `archpath` _str, optional_ - If the object is an archive, use `archpath` to extract a single file from the archive
- `chunk_size` _int, optional_ - chunk_size to use while reading from stream

**Returns**:

  The stream of bytes to read an object or a file inside an archive.

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="cluster.Cluster"></a>

## Class: Cluster

```python
class Cluster()
```

A class representing a cluster bound to an AIS client.

**Arguments**:

  None

<a id="cluster.Cluster.client"></a>

### client

```python
@property
def client()
```

The client object bound to this cluster.

<a id="cluster.Cluster.get_info"></a>

### get\_info

```python
def get_info() -> Smap
```

Returns state of AIS cluster, including the detailed information about its nodes.

**Arguments**:

  None
  

**Returns**:

- `aistore.msg.Smap` - Smap containing cluster information
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="cluster.Cluster.list_buckets"></a>

### list\_buckets

```python
def list_buckets(provider: str = ProviderAIS)
```

Returns list of buckets in AIStore cluster.

**Arguments**:

- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  Defaults to "ais". Empty provider returns buckets of all providers.
  

**Returns**:

- `List[Bck]` - A list of buckets
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="cluster.Cluster.is_aistore_running"></a>

### is\_aistore\_running

```python
def is_aistore_running() -> bool
```

Returns True if cluster is ready, or false if cluster is still setting up.

**Arguments**:

  None
  

**Returns**:

- `bool` - True if cluster is ready, or false if cluster is still setting up

<a id="bucket.Bucket"></a>

## Class: Bucket

```python
class Bucket()
```

A class representing a bucket that contains user data.

**Arguments**:

- `bck_name` _str_ - name of bucket
- `provider` _str, optional_ - provider of bucket (one of "ais", "aws", "gcp", ...), defaults to "ais"
- `ns` _str, optional_ - namespace of bucket, defaults to ""

<a id="bucket.Bucket.client"></a>

### client

```python
@property
def client()
```

The client bound to this bucket.

<a id="bucket.Bucket.bck"></a>

### bck

```python
@property
def bck()
```

The custom type [Bck] corresponding to this bucket.

<a id="bucket.Bucket.qparam"></a>

### qparam

```python
@property
def qparam()
```

The QParamProvider of this bucket.

<a id="bucket.Bucket.provider"></a>

### provider

```python
@property
def provider()
```

The provider for this bucket.

<a id="bucket.Bucket.name"></a>

### name

```python
@property
def name()
```

The name of this bucket.

<a id="bucket.Bucket.namespace"></a>

### namespace

```python
@property
def namespace()
```

The namespace for this bucket.

<a id="bucket.Bucket.create"></a>

### create

```python
def create()
```

Creates a bucket in AIStore cluster.
Can only create a bucket for AIS provider on localized cluster. Remote cloud buckets do not support creation.

**Arguments**:

  None
  

**Returns**:

  None
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
- `aistore.client.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.delete"></a>

### delete

```python
def delete()
```

Destroys bucket in AIStore cluster.
In all cases removes both the bucket's content _and_ the bucket's metadata from the cluster.
Note: AIS will _not_ call the remote backend provider to delete the corresponding Cloud bucket
(iff the bucket in question is, in fact, a Cloud bucket).

**Arguments**:

  None
  

**Returns**:

  None
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
- `aistore.client.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.rename"></a>

### rename

```python
def rename(to_bck: str) -> str
```

Renames bucket in AIStore cluster.
Only works on AIS buckets. Returns xaction id that can be used later to check the status of the asynchronous operation.

**Arguments**:

- `to_bck` _str_ - New bucket name for bucket to be renamed as
  

**Returns**:

  xaction id (as str) that can be used to check the status of the operation
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
- `aistore.client.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
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

- `keep_md` _bool, optional_ - If true, evicts objects but keeps the bucket's metadata (i.e., the bucket's name and its properties)
  

**Returns**:

  None
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
- `aistore.client.errors.InvalidBckProvider` - Invalid bucket provider for requested operation
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

**Arguments**:

  None
  

**Returns**:

  Response header with the bucket properties
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.copy"></a>

### copy

```python
def copy(to_bck_name: str,
         prefix: str = "",
         dry_run: bool = False,
         force: bool = False,
         to_provider: str = ProviderAIS) -> str
```

Returns xaction id that can be used later to check the status of the asynchronous operation.

**Arguments**:

- `to_bck_name` _str_ - Name of the destination bucket
- `prefix` _str, optional_ - If set, only the objects starting with
  provider prefix will be copied
- `dry_run` _bool, optional_ - Determines if the copy should actually
  happen or not
- `force` _bool, optional_ - Override existing destination bucket
- `to_provider` _str, optional_ - Name of destination bucket provider
  

**Returns**:

  Xaction id (as str) that can be used to check the status of the operation
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
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
                 continuation_token: str = "") -> BucketList
```

Returns a structure that contains a page of objects, xaction UUID, and continuation token (to read the next page, if available).

**Arguments**:

- `prefix` _str, optional_ - Return only objects that start with the prefix
- `props` _str, optional_ - Comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
- `page_size` _int, optional_ - Return at most "page_size" objects.
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects.
- `uuid` _str, optional_ - Job UUID, required to get the next page of objects
- `continuation_token` _str, optional_ - Marks the object to start reading the next page
  

**Returns**:

- `BucketList` - the page of objects in the bucket and the continuation token to get the next page
  Empty continuation token marks the final page of the object list
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
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
                      page_size: int = 0) -> BucketLister
```

Returns an iterator for all objects in bucket

**Arguments**:

- `prefix` _str, optional_ - Return only objects that start with the prefix
- `props` _str, optional_ - Comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
- `page_size` _int, optional_ - return at most "page_size" objects
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects
  

**Returns**:

- `BucketLister` - object iterator
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
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
                     page_size: int = 0) -> List[BucketEntry]
```

Returns a list of all objects in bucket

**Arguments**:

- `prefix` _str, optional_ - return only objects that start with the prefix
- `props` _str, optional_ - comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
- `page_size` _int, optional_ - return at most "page_size" objects
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects
  

**Returns**:

- `List[BucketEntry]` - list of objects in bucket
  

**Raises**:

- `aistore.client.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.object"></a>

### object

```python
def object(obj_name: str)
```

Factory constructor for object bound to bucket.
Does not make any HTTP request, only instantiates an object in a bucket owned by the client.

**Arguments**:

- `obj_name` _str_ - Name of object
  

**Returns**:

  The object created.

<a id="object.Object"></a>

## Class: Object

```python
class Object()
```

A class representing an object of a bucket bound to a client.

**Arguments**:

- `obj_name` _str_ - name of object

<a id="object.Object.bck"></a>

### bck

```python
@property
def bck()
```

The custom type [Bck] bound to this object.

<a id="object.Object.obj_name"></a>

### obj\_name

```python
@property
def obj_name()
```

The name of this object.

<a id="object.Object.head"></a>

### head

```python
def head() -> Header
```

Requests object properties.

**Arguments**:

  None
  

**Returns**:

  Response header with the object properties.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exeptions.HTTPError(404)` - The object does not exist

<a id="object.Object.get"></a>

### get

```python
def get(archpath: str = "", chunk_size: int = DEFAULT_CHUNK_SIZE) -> ObjStream
```

Reads an object

**Arguments**:

- `archpath` _str, optional_ - If the object is an archive, use `archpath` to extract a single file from the archive
- `chunk_size` _int, optional_ - chunk_size to use while reading from stream
  

**Returns**:

  The stream of bytes to read an object or a file inside an archive.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="object.Object.put"></a>

### put

```python
def put(path: str) -> Header
```

Puts a local file as an object to a bucket in AIS storage.

**Arguments**:

- `path` _str_ - path to local file.
  

**Returns**:

  Object properties
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="object.Object.delete"></a>

### delete

```python
def delete()
```

Delete an object from a bucket.

**Arguments**:

  None
  

**Returns**:

  None
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exeptions.HTTPError(404)` - The object does not exist

