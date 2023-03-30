---
layout: post
title: PYTHON SDK
permalink: /docs/python-sdk
redirect_from:
 - /python_sdk.md/
 - /docs/python_sdk.md/
---

AIStore Python SDK is a growing set of client-side objects and methods to access and utilize AIS clusters.

> For PyTorch integration and usage examples, please refer to [AIS Python SDK](https://pypi.org/project/aistore) available via Python Package Index (PyPI), or see [https://github.com/NVIDIA/aistore/tree/master/python/aistore](https://github.com/NVIDIA/aistore/tree/master/python/aistore).

* [client](#client)
  * [Client](#client.Client)
    * [bucket](#client.Client.bucket)
    * [cluster](#client.Client.cluster)
    * [job](#client.Client.job)
    * [etl](#client.Client.etl)
* [cluster](#cluster)
  * [Cluster](#cluster.Cluster)
    * [client](#cluster.Cluster.client)
    * [get\_info](#cluster.Cluster.get_info)
    * [list\_buckets](#cluster.Cluster.list_buckets)
    * [list\_jobs\_status](#cluster.Cluster.list_jobs_status)
    * [list\_running\_jobs](#cluster.Cluster.list_running_jobs)
    * [is\_aistore\_running](#cluster.Cluster.is_aistore_running)
* [bucket](#bucket)
  * [Bucket](#bucket.Bucket)
    * [client](#bucket.Bucket.client)
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
    * [put\_files](#bucket.Bucket.put_files)
    * [object](#bucket.Bucket.object)
    * [objects](#bucket.Bucket.objects)
    * [make\_request](#bucket.Bucket.make_request)
    * [verify\_cloud\_bucket](#bucket.Bucket.verify_cloud_bucket)
    * [get\_path](#bucket.Bucket.get_path)
    * [as\_model](#bucket.Bucket.as_model)
* [object](#object)
  * [Object](#object.Object)
    * [bucket](#object.Object.bucket)
    * [name](#object.Object.name)
    * [head](#object.Object.head)
    * [get](#object.Object.get)
    * [put\_content](#object.Object.put_content)
    * [put\_file](#object.Object.put_file)
    * [promote](#object.Object.promote)
    * [delete](#object.Object.delete)
* [multiobj.object\_group](#multiobj.object_group)
  * [ObjectGroup](#multiobj.object_group.ObjectGroup)
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
* [multiobj.object\_template](#multiobj.object_template)
  * [ObjectTemplate](#multiobj.object_template.ObjectTemplate)
* [job](#job)
  * [Job](#job.Job)
    * [job\_id](#job.Job.job_id)
    * [job\_kind](#job.Job.job_kind)
    * [status](#job.Job.status)
    * [wait](#job.Job.wait)
    * [wait\_for\_idle](#job.Job.wait_for_idle)
    * [start](#job.Job.start)
* [object\_reader](#object_reader)
  * [ObjectReader](#object_reader.ObjectReader)
    * [attributes](#object_reader.ObjectReader.attributes)
    * [read\_all](#object_reader.ObjectReader.read_all)
    * [raw](#object_reader.ObjectReader.raw)
    * [\_\_iter\_\_](#object_reader.ObjectReader.__iter__)
* [object\_iterator](#object_iterator)
  * [ObjectIterator](#object_iterator.ObjectIterator)
* [etl](#etl)
  * [Etl](#etl.Etl)
    * [client](#etl.Etl.client)
    * [init\_spec](#etl.Etl.init_spec)
    * [init\_code](#etl.Etl.init_code)
    * [list](#etl.Etl.list)
    * [view](#etl.Etl.view)
    * [start](#etl.Etl.start)
    * [stop](#etl.Etl.stop)
    * [delete](#etl.Etl.delete)

<a id="client.Client"></a>

## Class: Client

```python
class Client()
```

AIStore client for managing buckets, objects, ETL jobs

**Arguments**:

- `endpoint` _str_ - AIStore endpoint

<a id="client.Client.bucket"></a>

### bucket

```python
def bucket(bck_name: str,
           provider: str = PROVIDER_AIS,
           namespace: Namespace = None)
```

Factory constructor for bucket object.
Does not make any HTTP request, only instantiates a bucket object.

**Arguments**:

- `bck_name` _str_ - Name of bucket
- `provider` _str_ - Provider of bucket, one of "ais", "aws", "gcp", ... (optional, defaults to ais)
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
def etl()
```

Factory constructor for ETL object.
Contains APIs related to AIStore ETL operations.
Does not make any HTTP request, only instantiates an ETL object.

**Returns**:

  The ETL object created.

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

<a id="cluster.Cluster.list_buckets"></a>

### list\_buckets

```python
def list_buckets(provider: str = PROVIDER_AIS)
```

Returns list of buckets in AIStore cluster.

**Arguments**:

- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
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

<a id="cluster.Cluster.is_aistore_running"></a>

### is\_aistore\_running

```python
def is_aistore_running() -> bool
```

Checks if cluster is ready or still setting up.

**Returns**:

- `bool` - True if cluster is ready, or false if cluster is still setting up

<a id="bucket.Bucket"></a>

## Class: Bucket

```python
class Bucket()
```

A class representing a bucket that contains user data.

**Arguments**:

- `client` _RequestClient_ - Client for interfacing with AIS cluster
- `name` _str_ - name of bucket
- `provider` _str, optional_ - Provider of bucket (one of "ais", "aws", "gcp", ...), defaults to "ais"
- `namespace` _Namespace, optional_ - Namespace of bucket, defaults to None

<a id="bucket.Bucket.client"></a>

### client

```python
@property
def client() -> RequestClient
```

The client bound to this bucket.

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
def provider() -> str
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

<a id="bucket.Bucket.copy"></a>

### copy

```python
def copy(to_bck: Bucket,
         prefix_filter: str = "",
         prepend: str = "",
         dry_run: bool = False,
         force: bool = False) -> str
```

Returns job ID that can be used later to check the status of the asynchronous operation.

**Arguments**:

- `to_bck` _Bucket_ - Destination bucket
- `prefix_filter` _str, optional_ - Only copy objects with names starting with this prefix
- `prepend` _str, optional_ - Value to prepend to the name of copied objects
- `dry_run` _bool, optional_ - Determines if the copy should actually
  happen or not
- `force` _bool, optional_ - Override existing destination bucket
  

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
              dry_run: bool = False) -> str
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
- `pattern` _str, optional_ - Regex pattern to filter files
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
def object(obj_name: str) -> Object
```

Factory constructor for an object in this bucket.
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

<a id="object.Object"></a>

## Class: Object

```python
class Object()
```

A class representing an object of a bucket bound to a client.

**Arguments**:

- `bucket` _Bucket_ - Bucket to which this object belongs
- `name` _str_ - name of object

<a id="object.Object.bucket"></a>

### bucket

```python
@property
def bucket()
```

Bucket containing this object

<a id="object.Object.name"></a>

### name

```python
@property
def name()
```

Name of this object

<a id="object.Object.head"></a>

### head

```python
def head() -> Header
```

Requests object properties.

**Returns**:

  Response header with the object properties.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exceptions.HTTPError(404)` - The object does not exist

<a id="object.Object.get"></a>

### get

```python
def get(archpath: str = "",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etl_name: str = None,
        writer: BufferedWriter = None) -> ObjectReader
```

Reads an object

**Arguments**:

- `archpath` _str, optional_ - If the object is an archive, use `archpath` to extract a single file
  from the archive
- `chunk_size` _int, optional_ - chunk_size to use while reading from stream
- `etl_name` _str, optional_ - Transforms an object based on ETL with etl_name
- `writer` _BufferedWriter, optional_ - User-provided writer for writing content output.
  User is responsible for closing the writer
  

**Returns**:

  The stream of bytes to read an object or a file inside an archive.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="object.Object.put_content"></a>

### put\_content

```python
def put_content(content: bytes) -> Header
```

Puts bytes as an object to a bucket in AIS storage.

**Arguments**:

- `content` _bytes_ - Bytes to put as an object.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="object.Object.put_file"></a>

### put\_file

```python
def put_file(path: str = None)
```

Puts a local file as an object to a bucket in AIS storage.

**Arguments**:

- `path` _str_ - Path to local file
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `ValueError` - The path provided is not a valid file

<a id="object.Object.promote"></a>

### promote

```python
def promote(path: str,
            target_id: str = "",
            recursive: bool = False,
            overwrite_dest: bool = False,
            delete_source: bool = False,
            src_not_file_share: bool = False) -> Header
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

  Object properties
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `AISError` - Path does not exist on the AIS cluster storage

<a id="object.Object.delete"></a>

### delete

```python
def delete()
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

<a id="multiobj.object_group.ObjectGroup"></a>

## Class: ObjectGroup

```python
class ObjectGroup()
```

A class representing multiple objects within the same bucket. Only one of obj_names, obj_range, or obj_template
should be provided.

**Arguments**:

- `bck` _Bucket_ - Bucket the objects belong to
- `obj_names` _list[str], optional_ - List of object names to include in this collection
- `obj_range` _ObjectRange, optional_ - Range defining which object names in the bucket should be included
- `obj_template` _str, optional_ - String argument to pass as template value directly to api

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

<a id="multiobj.object_group.ObjectGroup.copy"></a>

### copy

```python
def copy(to_bck: "Bucket",
         prepend: str = "",
         continue_on_error: bool = False,
         dry_run: bool = False,
         force: bool = False)
```

Copies a list or range of objects in a bucket

**Arguments**:

- `to_bck` _Bucket_ - Destination bucket
- `prepend` _str, optional_ - Value to prepend to the name of copied objects
- `continue_on_error` _bool, optional_ - Whether to continue if there is an error copying a single object
- `dry_run` _bool, optional_ - Skip performing the copy and just log the intended actions
- `force` _bool, optional_ - Force this job to run over others in case it conflicts
  (see "limited coexistence" and xact/xreg/xreg.go)
  

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
              force: bool = False)
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

<a id="multiobj.object_template.ObjectTemplate"></a>

## Class: ObjectTemplate

```python
class ObjectTemplate(ObjectCollection)
```

A collection of object names specified by a template in the bash brace expansion format

**Arguments**:

- `template` _str_ - A string template that defines the names of objects to include in the collection

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

<a id="object_reader.ObjectReader"></a>

## Class: ObjectReader

```python
class ObjectReader()
```

Represents the data returned by the API when getting an object, including access to the content stream and object
attributes

<a id="object_reader.ObjectReader.attributes"></a>

### attributes

```python
@property
def attributes() -> ObjectAttributes
```

Object metadata attributes

**Returns**:

  Object attributes parsed from the headers returned by AIS

<a id="object_reader.ObjectReader.read_all"></a>

### read\_all

```python
def read_all() -> bytes
```

Read all byte data from the object content stream.
This uses a bytes cast which makes it slightly slower and requires all object content to fit in memory at once

**Returns**:

  Object content as bytes

<a id="object_reader.ObjectReader.raw"></a>

### raw

```python
def raw() -> bytes
```

Returns: Raw byte stream of object content

<a id="object_reader.ObjectReader.__iter__"></a>

### \_\_iter\_\_

```python
def __iter__() -> Iterator[bytes]
```

Creates a generator to read the stream content in chunks

**Returns**:

  An iterator with access to the next chunk of bytes

<a id="object_iterator.ObjectIterator"></a>

## Class: ObjectIterator

```python
class ObjectIterator()
```

Represents an iterable that will fetch all objects from a bucket, querying as needed with the specified function

**Arguments**:

- `list_objects` _Callable_ - Function returning a BucketList from an AIS cluster

<a id="etl.Etl"></a>

## Class: Etl

```python
class Etl()
```

A class containing ETL-related functions.

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
              etl_name: str,
              communication_type: str = DEFAULT_ETL_COMM,
              timeout: str = DEFAULT_ETL_TIMEOUT)
```

Initializes ETL based on Kubernetes pod spec template. Returns etl_name.

**Arguments**:

- `template` _str_ - Kubernetes pod spec template
  Existing templates can be found at `sdk.etl_templates`
  For more information visit: https://github.com/NVIDIA/ais-etl/tree/master/transformers
- `etl_name` _str_ - Name of new ETL
- `communication_type` _str_ - Communication type of the ETL (options: hpull, hrev, hpush)
- `timeout` _str_ - Timeout of the ETL job (e.g. 5m for 5 minutes)

**Returns**:

- `etl_name` _str_ - ETL name

<a id="etl.Etl.init_code"></a>

### init\_code

```python
def init_code(transform: Callable,
              etl_name: str,
              dependencies: List[str] = None,
              runtime: str = _get_default_runtime(),
              communication_type: str = DEFAULT_ETL_COMM,
              timeout: str = DEFAULT_ETL_TIMEOUT,
              chunk_size: int = None)
```

Initializes ETL based on the provided source code. Returns etl_name.

**Arguments**:

- `transform` _Callable_ - Transform function of the ETL
- `etl_name` _str_ - Name of new ETL
- `dependencies` _list[str]_ - Python dependencies to install
- `runtime` _str_ - [optional, default= V2 implementation of the current python version if supported, else
  python3.8v2] Runtime environment of the ETL [choose from: python3.8v2, python3.10v2, python3.11v2]
  (see ext/etl/runtime/all.go)
- `communication_type` _str_ - [optional, default="hpush"] Communication type of the ETL (options: hpull, hrev,
  hpush, io)
- `timeout` _str_ - [optional, default="5m"] Timeout of the ETL job (e.g. 5m for 5 minutes)
- `chunk_size` _int_ - Chunk size in bytes if transform function in streaming data.
  (whole object is read by default)

**Returns**:

- `etl_name` _str_ - ETL name

<a id="etl.Etl.list"></a>

### list

```python
def list() -> List[ETLDetails]
```

Lists all running ETLs.

Note: Does not list ETLs that have been stopped or deleted.

**Returns**:

- `List[ETL]` - A list of running ETLs

<a id="etl.Etl.view"></a>

### view

```python
def view(etl_name: str) -> ETLDetails
```

View ETLs Init spec/code

**Arguments**:

- `etl_name` _str_ - name of ETL

**Returns**:

- `ETLDetails` - details of the ETL

<a id="etl.Etl.start"></a>

### start

```python
def start(etl_name: str)
```

Resumes a stopped ETL with given ETL name.

Note: Deleted ETLs cannot be started.

**Arguments**:

- `etl_name` _str_ - name of ETL

<a id="etl.Etl.stop"></a>

### stop

```python
def stop(etl_name: str)
```

Stops ETL with given ETL name. Stops (but does not delete) all the pods created by Kubernetes for this ETL and
terminates any transforms.

**Arguments**:

- `etl_name` _str_ - name of ETL

<a id="etl.Etl.delete"></a>

### delete

```python
def delete(etl_name: str)
```

Delete ETL with given ETL name. Deletes pods created by Kubernetes for this ETL and specifications for this ETL
in Kubernetes.

Note: Running ETLs cannot be deleted.

**Arguments**:

- `etl_name` _str_ - name of ETL

