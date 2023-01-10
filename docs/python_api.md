---
layout: post
title: PYTHON API
permalink: /docs/python-api
redirect_from:
 - /python_api.md/
 - /docs/python_api.md/
---

AIStore Python API is a growing set of client-side objects and methods to access and utilize AIS clusters.

> For PyTorch integration and usage examples, please refer to [AIS Python SDK](https://pypi.org/project/aistore) available via Python Package Index (PyPI), or see [https://github.com/NVIDIA/aistore/tree/master/python/aistore](https://github.com/NVIDIA/aistore/tree/master/python/aistore).

* [api](#api)
  * [Client](#api.Client)
    * [bucket](#api.Client.bucket)
    * [cluster](#api.Client.cluster)
    * [job](#api.Client.job)
    * [etl](#api.Client.etl)
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
    * [transform](#bucket.Bucket.transform)
    * [object](#bucket.Bucket.object)
    * [objects](#bucket.Bucket.objects)
    * [make\_request](#bucket.Bucket.make_request)
* [object](#object)
  * [Object](#object.Object)
    * [bck](#object.Object.bck)
    * [obj\_name](#object.Object.obj_name)
    * [head](#object.Object.head)
    * [get](#object.Object.get)
    * [put](#object.Object.put)
    * [delete](#object.Object.delete)
* [object\_group](#object_group)
  * [ObjectGroup](#object_group.ObjectGroup)
    * [delete](#object_group.ObjectGroup.delete)
    * [evict](#object_group.ObjectGroup.evict)
    * [prefetch](#object_group.ObjectGroup.prefetch)
* [etl](#etl)
  * [get\_default\_runtime](#etl.get_default_runtime)
  * [Etl](#etl.Etl)
    * [client](#etl.Etl.client)
    * [init\_spec](#etl.Etl.init_spec)
    * [init\_code](#etl.Etl.init_code)
    * [list](#etl.Etl.list)
    * [view](#etl.Etl.view)
    * [start](#etl.Etl.start)
    * [stop](#etl.Etl.stop)
    * [delete](#etl.Etl.delete)

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
def bucket(bck_name: str, provider: str = ProviderAIS, ns: Namespace = None)
```

Factory constructor for bucket object.
Does not make any HTTP request, only instantiates a bucket object owned by the client.

**Arguments**:

- `bck_name` _str_ - Name of bucket (optional, defaults to "ais").
- `provider` _str_ - Provider of bucket (one of "ais", "aws", "gcp", ...).
- `ns` _Namespace_ - Namespace of bucket (optional, defaults to None).
  

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

<a id="api.Client.job"></a>

### job

```python
def job()
```

Factory constructor for job object, which contains job-related functions.
Does not make any HTTP request, only instantiates a job object bound to the client.

**Arguments**:

  None
  

**Returns**:

  The job object created.

<a id="api.Client.etl"></a>

### etl

```python
def etl()
```

Factory constructor for ETL object.
Contains APIs related to AIStore ETL operations.
Does not make any HTTP request, only instantiates an ETL object bound to the client.

**Arguments**:

  None
  

**Returns**:

  The ETL object created.

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

- `aistore.sdk.types.Smap` - Smap containing cluster information
  

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
- `ns` _Namespace, optional_ - namespace of bucket, defaults to None

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
def rename(to_bck: str) -> str
```

Renames bucket in AIStore cluster.
Only works on AIS buckets. Returns job ID that can be used later to check the status of the asynchronous operation.

**Arguments**:

- `to_bck` _str_ - New bucket name for bucket to be renamed as
  

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

- `keep_md` _bool, optional_ - If true, evicts objects but keeps the bucket's metadata (i.e., the bucket's name and its properties)
  

**Returns**:

  None
  

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

**Arguments**:

  None
  

**Returns**:

  Response header with the bucket properties
  

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
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

Returns job ID that can be used later to check the status of the asynchronous operation.

**Arguments**:

- `to_bck_name` _str_ - Name of the destination bucket
- `prefix` _str, optional_ - If set, only the objects starting with
  provider prefix will be copied
- `dry_run` _bool, optional_ - Determines if the copy should actually
  happen or not
- `force` _bool, optional_ - Override existing destination bucket
- `to_provider` _str, optional_ - Name of destination bucket provider
  

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
                 continuation_token: str = "") -> BucketList
```

Returns a structure that contains a page of objects, job ID, and continuation token (to read the next page, if available).

**Arguments**:

- `prefix` _str, optional_ - Return only objects that start with the prefix
- `props` _str, optional_ - Comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
- `page_size` _int, optional_ - Return at most "page_size" objects.
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects.
- `uuid` _str, optional_ - Job ID, required to get the next page of objects
- `continuation_token` _str, optional_ - Marks the object to start reading the next page
  

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

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="bucket.Bucket.transform"></a>

### transform

```python
def transform(etl_id: str,
              to_bck: str,
              prefix: str = "",
              ext: Dict[str, str] = None,
              force: bool = False,
              dry_run: bool = False)
```

Transforms all objects in a bucket and puts them to destination bucket.

**Arguments**:

- `etl_id` _str_ - id of etl to be used for transformations
- `to_bck` _str_ - destination bucket for transformations
- `prefix` _str, optional_ - prefix to be added to resulting transformed objects
- `ext` _Dict[str, str], optional_ - dict of new extension followed by extension to be replaced (i.e. {"jpg": "txt"})
- `dry_run` _bool, optional_ - determines if the copy should actually happen or not
- `force` _bool, optional_ - override existing destination bucket
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="bucket.Bucket.object"></a>

### object

```python
def object(obj_name: str)
```

Factory constructor for object belonging to this bucket.
Does not make any HTTP request, only instantiates an object in a bucket owned by the client.

**Arguments**:

- `obj_name` _str_ - Name of object
  

**Returns**:

  The object created.

<a id="bucket.Bucket.objects"></a>

### objects

```python
def objects(obj_names: list = None,
            obj_range: ObjectRange = None,
            obj_template: str = None)
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
                 value: dict = None,
                 params: dict = None) -> requests.Response
```

Use the bucket's client to make a request to the bucket endpoint on the AIS server

**Arguments**:

- `method` _str_ - HTTP method to use, e.g. POST/GET/DELETE
- `action` _str_ - Action string used to create an ActionMsg to pass to the server
- `value` _dict_ - Additional value parameter to pass in the ActionMsg
- `params` _dict, optional_ - Optional parameters to pass in the request
  

**Returns**:

  Response from the server

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
def get(archpath: str = "",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_id: str = None) -> ObjStream
```

Reads an object

**Arguments**:

- `archpath` _str, optional_ - If the object is an archive, use `archpath` to extract a single file from the archive
- `chunk_size` _int, optional_ - chunk_size to use while reading from stream
  etl_id(str, optional): Transforms an object based on ETL with etl_id
  

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
def put(path: str = None, content: bytes = None) -> Header
```

Puts a local file or bytes as an object to a bucket in AIS storage.

**Arguments**:

- `path` _str_ - path to local file or bytes.
- `content` _bytes_ - bytes to put as an object.
  

**Returns**:

  Object properties
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `ValueError` - Path and content are mutually exclusive

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

<a id="object_group.ObjectGroup"></a>

## Class: ObjectGroup

```python
class ObjectGroup()
```

A class representing multiple objects within the same bucket. Only one of obj_names or obj_range should be provided.

**Arguments**:

- `bck` _Bucket_ - Bucket the objects belong to
- `obj_names` _list[str], optional_ - List of object names to include in this collection
- `obj_range` _ObjectRange, optional_ - Range defining which object names in the bucket should be included
- `obj_template` _str, optional_ - String argument to pass as template value directly to api

<a id="object_group.ObjectGroup.delete"></a>

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

<a id="object_group.ObjectGroup.evict"></a>

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

<a id="object_group.ObjectGroup.prefetch"></a>

### prefetch

```python
def prefetch()
```

Prefetches a list or range of objects in a bucket so that they are cached in AIS
NOTE: only Cloud buckets can be prefetched.

**Raises**:

- `aistore.sdk.errors.AISError` - All other types of errors with AIStore
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.exceptions.HTTPError` - Service unavailable
- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ReadTimeout` - Timed out receiving response from AIStore
  

**Returns**:

  Job ID (as str) that can be used to check the status of the operation

<a id="etl.get_default_runtime"></a>

### get\_default\_runtime

```python
def get_default_runtime()
```

Determines etl runtime to use if not specified

**Returns**:

  String of runtime

<a id="etl.Etl"></a>

## Class: Etl

```python
class Etl()
```

A class containing ETL-related functions.

**Arguments**:

  None

<a id="etl.Etl.client"></a>

### client

```python
@property
def client()
```

The client bound to this ETL object.

<a id="etl.Etl.init_spec"></a>

### init\_spec

```python
def init_spec(template: str,
              etl_id: str,
              communication_type: str = "hpush",
              timeout: str = "5m")
```

Initializes ETL based on POD spec template. Returns ETL_ID.
Existing templates can be found at `sdk.etl_templates`
For more information visit: https://github.com/NVIDIA/ais-etl/tree/master/transformers

**Arguments**:

- `docker_image` _str_ - docker image name looks like: <hub-user>/<repo-name>:<tag>
- `etl_id` _str_ - id of new ETL
- `communication_type` _str_ - Communication type of the ETL (options: hpull, hrev, hpush)
- `timeout` _str_ - timeout of the ETL (eg. 5m for 5 minutes)

**Returns**:

- `etl_id` _str_ - ETL ID

<a id="etl.Etl.init_code"></a>

### init\_code

```python
def init_code(transform: Callable,
              etl_id: str,
              dependencies: List[str] = None,
              runtime: str = get_default_runtime(),
              communication_type: str = "hpush",
              timeout: str = "5m",
              chunk_size: int = None)
```

Initializes ETL based on the provided source code. Returns ETL_ID.

**Arguments**:

- `transform` _Callable_ - Transform function of the ETL
- `etl_id` _str_ - ID of new ETL
- `dependencies` _list[str]_ - Python dependencies to install
- `runtime` _str_ - [optional, default= V2 implementation of the current python version if supported, else
  python3.8v2] Runtime environment of the ETL [choose from: python3.8v2, python3.10v2, python3.11v2]
  (see ext/etl/runtime/all.go)
- `communication_type` _str_ - [optional, default="hpush"] Communication type of the ETL (options: hpull, hrev, hpush, io)
- `timeout` _str_ - [optional, default="5m"] Timeout of the ETL (e.g. 5m for 5 minutes)
- `chunk_size` _int_ - Chunk size in bytes if transform function in streaming data. (whole object is read by default)

**Returns**:

- `etl_id` _str_ - ETL ID

<a id="etl.Etl.list"></a>

### list

```python
def list() -> List[ETLDetails]
```

Lists all running ETLs.

Note: Does not list ETLs that have been stopped or deleted.

**Arguments**:

  Nothing

**Returns**:

- `List[ETL]` - A list of running ETLs

<a id="etl.Etl.view"></a>

### view

```python
def view(etl_id: str) -> ETLDetails
```

View ETLs Init spec/code

**Arguments**:

- `etl_id` _str_ - id of ETL

**Returns**:

- `ETLDetails` - details of the ETL

<a id="etl.Etl.start"></a>

### start

```python
def start(etl_id: str)
```

Resumes a stopped ETL with given ETL_ID.

Note: Deleted ETLs cannot be started.

**Arguments**:

- `etl_id` _str_ - id of ETL

**Returns**:

  Nothing

<a id="etl.Etl.stop"></a>

### stop

```python
def stop(etl_id: str)
```

Stops ETL with given ETL_ID. Stops (but does not delete) all the pods created by Kubernetes for this ETL and terminates any transforms.

**Arguments**:

- `etl_id` _str_ - id of ETL

**Returns**:

  Nothing

<a id="etl.Etl.delete"></a>

### delete

```python
def delete(etl_id: str)
```

Delete ETL with given ETL_ID. Deletes pods created by Kubernetes for this ETL and specifications for this ETL in Kubernetes.

Note: Running ETLs cannot be deleted.

**Arguments**:

- `etl_id` _str_ - id of ETL

**Returns**:

  Nothing

