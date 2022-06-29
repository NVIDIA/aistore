---
layout: post
title: PYTHON API
permalink: /docs/python-api
redirect_from:
 - /python_api.md/
 - /docs/python_api.md/
---

# Python AIStore Client API

AIStore Python SDK is a growing set of client-side APIs to access and utilize AIS clusters.

* [api](#api)
  * [Client](#api.Client)
    * [list\_buckets](#api.Client.list_buckets)
    * [is\_aistore\_running](#api.Client.is_aistore_running)
    * [create\_bucket](#api.Client.create_bucket)
    * [destroy\_bucket](#api.Client.destroy_bucket)
    * [evict\_bucket](#api.Client.evict_bucket)
    * [head\_bucket](#api.Client.head_bucket)
    * [copy\_bucket](#api.Client.copy_bucket)
    * [list\_objects](#api.Client.list_objects)
    * [list\_objects\_iter](#api.Client.list_objects_iter)
    * [list\_all\_objects](#api.Client.list_all_objects)
    * [head\_object](#api.Client.head_object)
    * [get\_object](#api.Client.get_object)
    * [put\_object](#api.Client.put_object)
    * [delete\_object](#api.Client.delete_object)
    * [get\_cluster\_info](#api.Client.get_cluster_info)
    * [xact\_status](#api.Client.xact_status)
    * [wait\_for\_xaction\_finished](#api.Client.wait_for_xaction_finished)
    * [xact\_start](#api.Client.xact_start)

<a id="api.Client"></a>

## Client

```python
class Client()
```

AIStore client for managing buckets, objects, ETL jobs

**Arguments**:

- `endpoint` _str_ - AIStore endpoint

<a id="api.Client.list_buckets"></a>

### list\_buckets

```python
def list_buckets(provider: str = ProviderAIS)
```

Returns list of buckets in AIStore cluster

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

<a id="api.Client.is_aistore_running"></a>

### is\_aistore\_running

```python
def is_aistore_running() -> bool
```

Returns True if cluster is ready and false if cluster is still setting up

**Arguments**:

  None
  

**Returns**:

- `bool` - True if cluster is ready and false if cluster is still setting up

<a id="api.Client.create_bucket"></a>

### create\_bucket

```python
def create_bucket(bck_name: str)
```

Creates a bucket in AIStore cluster.
Always creates a bucket for AIS provider. Other providers do not support bucket creation.

**Arguments**:

- `bck_name` _str_ - Name of the new bucket.
  

**Returns**:

  Nothing
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exceptions.HTTPError(409)` - Bucket already exists

<a id="api.Client.destroy_bucket"></a>

### destroy\_bucket

```python
def destroy_bucket(bck_name: str)
```

Destroys a bucket in AIStore cluster.
Can delete only AIS buckets. Other providers do not support bucket deletion.

**Arguments**:

- `bck_name` _str_ - Name of the existing bucket
  

**Returns**:

  Nothing
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="api.Client.evict_bucket"></a>

### evict\_bucket

```python
def evict_bucket(bck_name: str, provider: str, keep_md: bool = True)
```

Evicts a bucket in AIStore cluster.
NOTE: only Cloud buckets can be evicted

**Arguments**:

- `bck_name` _str_ - Name of the existing bucket
- `provider` _str_ - Name of bucket provider, one of "aws", "gcp", "az", "hdfs" or "ht"
- `keep_md` _bool, optional_ - if true, it evicts objects but keeps bucket metadata
  

**Returns**:

  Nothing
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `InvalidBckProvider` - Evicting AIS bucket

<a id="api.Client.head_bucket"></a>

### head\_bucket

```python
def head_bucket(bck_name: str, provider: str = ProviderAIS) -> Header
```

Requests bucket properties.

**Arguments**:

- `bck_name` _str_ - Name of the new bucket
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  Defaults to "ais". Empty provider returns buckets of all providers.
  

**Returns**:

  Response header with the bucket properties
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exeptions.HTTPError(404)` - The bucket does not exist

<a id="api.Client.copy_bucket"></a>

### copy\_bucket

```python
def copy_bucket(from_bck_name: str,
                to_bck_name: str,
                prefix: str = "",
                dry_run: bool = False,
                force: bool = False,
                from_provider: str = ProviderAIS,
                to_provider: str = ProviderAIS) -> str
```

Returns xaction id that can be used later to check the status of the
asynchronous operation.

**Arguments**:

- `from_bck_name` _str_ - Name of the source bucket.
- `to_bck_name` _str_ - Name of the destination bucket.
- `prefix` _str, optional_ - If set, only the objects starting with
  provider prefix will be copied.
- `dry_run` _bool, optional_ - Determines if the copy should actually
  happen or not.
- `force` _bool, optional_ - Override existing destination bucket.
- `from_provider` _str, optional_ - Name of source bucket provider.
- `to_provider` _str, optional_ - Name of destination bucket provider.
  

**Returns**:

  Xaction id (as str) that can be used to check the status of the operation.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out receiving response from AIStore

<a id="api.Client.list_objects"></a>

### list\_objects

```python
def list_objects(bck_name: str,
                 provider: str = ProviderAIS,
                 prefix: str = "",
                 props: str = "",
                 page_size: int = 0,
                 uuid: str = "",
                 continuation_token: str = "") -> BucketList
```

Returns a structure that contains a page of objects, xaction UUID, and continuation token (to read the next page, if available).

**Arguments**:

- `bck_name` _str_ - Name of a bucket
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  Defaults to "ais". Empty provider returns buckets of all providers.
- `prefix` _str, optional_ - return only objects that start with the prefix
- `props` _str, optional_ - comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
- `page_size` _int, optional_ - return at most "page_size" objects.
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects.
- `uuid` _str, optional_ - job UUID, required to get the next page of objects
- `continuation_token` _str, optional_ - marks the object to start reading the next page
  

**Returns**:

- `BucketList` - the page of objects in the bucket and the continuation token to get the next page.
  Empty continuation token marks the final page of the object list.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

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

<a id="api.Client.list_all_objects"></a>

### list\_all\_objects

```python
def list_all_objects(bck_name: str,
                     provider: str = ProviderAIS,
                     prefix: str = "",
                     props: str = "",
                     page_size: int = 0) -> List[BucketEntry]
```

Returns a list of all objects in a bucket

**Arguments**:

- `bck_name` _str_ - Name of a bucket
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  Defaults to "ais". Empty provider returns buckets of all providers.
- `prefix` _str, optional_ - return only objects that start with the prefix
- `props` _str, optional_ - comma-separated list of object properties to return. Default value is "name,size". Properties: "name", "size", "atime", "version", "checksum", "cached", "target_url", "status", "copies", "ec", "custom", "node".
- `page_size` _int, optional_ - return at most "page_size" objects.
  The maximum number of objects in response depends on the bucket backend. E.g, AWS bucket cannot return more than 5,000 objects in a single page.
- `NOTE` - If "page_size" is greater than a backend maximum, the backend maximum objects are returned.
  Defaults to "0" - return maximum number objects.
  

**Returns**:

- `List[BucketEntry]` - list of objects in a bucket
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="api.Client.head_object"></a>

### head\_object

```python
def head_object(bck_name: str,
                obj_name: str,
                provider: str = ProviderAIS) -> Header
```

Requests object properties.

**Arguments**:

- `bck_name` _str_ - Name of the new bucket
- `obj_name` _str_ - Name of an object in the bucket
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  Defaults to "ais". Empty provider returns buckets of all providers.
  

**Returns**:

  Response header with the object properties.
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exeptions.HTTPError(404)` - The object does not exist

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

<a id="api.Client.put_object"></a>

### put\_object

```python
def put_object(bck_name: str,
               obj_name: str,
               path: str,
               provider: str = ProviderAIS) -> Header
```

Puts a local file as an object to a bucket in AIS storage

**Arguments**:

- `bck_name` _str_ - Name of a bucket
- `obj_name` _str_ - Name of an object in the bucket
- `path` _str_ - path to local file
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  

**Returns**:

  Object properties
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="api.Client.delete_object"></a>

### delete\_object

```python
def delete_object(bck_name: str, obj_name: str, provider: str = ProviderAIS)
```

Delete an object from a bucket.

**Arguments**:

- `bck_name` _str_ - Name of the new bucket
- `obj_name` _str_ - Name of an object in the bucket
- `provider` _str, optional_ - Name of bucket provider, one of "ais", "aws", "gcp", "az", "hdfs" or "ht".
  Defaults to "ais".
  

**Returns**:

  None
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `requests.exeptions.HTTPError(404)` - The object does not exist

<a id="api.Client.get_cluster_info"></a>

### get\_cluster\_info

```python
def get_cluster_info() -> Smap
```

Returns state of AIS cluster, including the detailed information about its nodes

**Arguments**:

  No arguments
  

**Returns**:

  aistore.msg.Smap
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="api.Client.xact_status"></a>

### xact\_status

```python
def xact_status(xact_id: str = "",
                xact_kind: str = "",
                daemon_id: str = "",
                only_running: bool = False) -> XactStatus
```

Return status of an eXtended Action (xaction)

**Arguments**:

- `xact_id` _str, optional_ - UUID of the xaction. Empty - all xactions
- `xact_kind` _str, optional_ - kind of the xaction. Empty - all kinds
- `daemon_id` _str, optional_ - return xactions only running on the daemon_id
- `only_running` _bool, optional_ - True - return only currently running xactions, False - include in the list also finished and aborted ones
  

**Returns**:

  The xaction description
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

<a id="api.Client.wait_for_xaction_finished"></a>

### wait\_for\_xaction\_finished

```python
def wait_for_xaction_finished(xact_id: str = "",
                              xact_kind: str = "",
                              daemon_id: str = "",
                              timeout: int = 300)
```

Wait for an eXtended Action (xaction) to finish

**Arguments**:

- `xact_id` _str, optional_ - UUID of the xaction. Empty - all xactions
- `xact_kind` _str, optional_ - kind of the xaction. Empty - all kinds
- `daemon_id` _str, optional_ - return xactions only running on the daemon_id
- `timeout` _int, optional_ - the maximum time to wait for the xaction, in seconds. Default timeout is 5 minutes
  

**Returns**:

  None
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore
- `errors.Timeout` - Timeout while waiting for the xaction to finish

<a id="api.Client.xact_start"></a>

### xact\_start

```python
def xact_start(xact_kind: str = "",
               daemon_id: str = "",
               force: bool = False,
               buckets: List[Bck] = None) -> str
```

Start an eXtended Action (xaction) and return its UUID

**Arguments**:

- `xact_kind` _str, optional_ - `kind` of the xaction (for supported kinds, see api/apc/const.go). Empty - all kinds.
- `daemon_id` _str, optional_ - return xactions only running on the daemon_id
- `force` _bool, optional_ - override existing restrictions for a bucket (e.g., run LRU eviction even if the bucket has LRU disabled)
- `buckets` _List[Bck], optional_ - list of one or more buckets; applicable only for xactions that have bucket scope (for details and full enumeration, see xact/table.go)
  

**Returns**:

  The running xaction UUID
  

**Raises**:

- `requests.RequestException` - "There was an ambiguous exception that occurred while handling..."
- `requests.ConnectionError` - Connection error
- `requests.ConnectionTimeout` - Timed out connecting to AIStore
- `requests.ReadTimeout` - Timed out waiting response from AIStore

