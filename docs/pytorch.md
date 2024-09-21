---
layout: post
title: PYTORCH
permalink: /docs/pytorch
redirect_from:
 - /pytorch.md/
 - /docs/pytorch.md/
---

In AIStore, PyTorch integration is a growing set of datasets (both iterable and map-style), samplers, and dataloaders. This readme illustrates taxonomy of the associated abstractions and provides API reference documentation.

For usage examples, please see:
* [AIS plugin for PyTorch](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch/README.md)
* [Jupyter notebook examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/aisio-pytorch/)

![PyTorch Structure](/docs/images/pytorch_structure.webp)
* [base\_map\_dataset](#base_map_dataset)
  * [AISBaseMapDataset](#base_map_dataset.AISBaseMapDataset)
* [base\_iter\_dataset](#base_iter_dataset)
  * [AISBaseIterDataset](#base_iter_dataset.AISBaseIterDataset)
    * [\_\_iter\_\_](#base_iter_dataset.AISBaseIterDataset.__iter__)
    * [\_\_len\_\_](#base_iter_dataset.AISBaseIterDataset.__len__)
* [map\_dataset](#map_dataset)
  * [AISMapDataset](#map_dataset.AISMapDataset)
* [iter\_dataset](#iter_dataset)
  * [AISIterDataset](#iter_dataset.AISIterDataset)
* [shard\_reader](#shard_reader)
  * [AISShardReader](#shard_reader.AISShardReader)
    * [\_\_len\_\_](#shard_reader.AISShardReader.__len__)
    * [ZeroDict](#shard_reader.AISShardReader.ZeroDict)
* [worker\_request\_client](#worker_request_client)
  * [WorkerRequestClient](#worker_request_client.WorkerRequestClient)
    * [session](#worker_request_client.WorkerRequestClient.session)
* [multishard\_dataset](#multishard_dataset)
  * [AISMultiShardStream](#multishard_dataset.AISMultiShardStream)
* [aisio](#aisio)
  * [AISFileListerIterDataPipe](#aisio.AISFileListerIterDataPipe)
  * [AISFileLoaderIterDataPipe](#aisio.AISFileLoaderIterDataPipe)
  * [AISSourceLister](#aisio.AISSourceLister)
    * [\_\_init\_\_](#aisio.AISSourceLister.__init__)

Base class for AIS Map Style Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.

<a id="base_map_dataset.AISBaseMapDataset"></a>

## Class: AISBaseMapDataset

```python
class AISBaseMapDataset(ABC, Dataset)
```

A base class for creating map-style AIS Datasets. Should not be instantiated directly. Subclasses
should implement :meth:`__getitem__` which fetches a samples given a key from the dataset and can optionally
override other methods from torch Dataset such as :meth:`__len__` and :meth:`__getitems__`.

**Arguments**:

- `ais_source_list` _Union[AISSource, List[AISSource]]_ - Single or list of AISSource objects to load data
  prefix_map (Dict(AISSource, List[str]), optional): Map of AISSource objects to list of prefixes that only allows
  objects with the specified prefixes to be used from each source

Base class for AIS Iterable Style Datasets

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.

<a id="base_iter_dataset.AISBaseIterDataset"></a>

## Class: AISBaseIterDataset

```python
class AISBaseIterDataset(ABC, IterableDataset)
```

A base class for creating AIS Iterable Datasets. Should not be instantiated directly. Subclasses
should implement :meth:`__iter__` which returns the samples from the dataset and can optionally
override other methods from torch IterableDataset such as :meth:`__len__`.

**Arguments**:

- `ais_source_list` _Union[AISSource, List[AISSource]]_ - Single or list of AISSource objects to load data
  prefix_map (Dict(AISSource, List[str]), optional): Map of AISSource objects to list of prefixes that only allows
  objects with the specified prefixes to be used from each source

<a id="base_iter_dataset.AISBaseIterDataset.__iter__"></a>

### \_\_iter\_\_

```python
@abstractmethod
def __iter__() -> Iterator
```

Return iterator with samples in this dataset.

**Returns**:

- `Iterator` - Iterator of samples

<a id="base_iter_dataset.AISBaseIterDataset.__len__"></a>

### \_\_len\_\_

```python
def __len__()
```

Returns the length of the dataset. Note that calling this
will iterate through the dataset, taking O(N) time.

NOTE: If you want the length of the dataset after iterating through
it, use `for i, data in enumerate(dataset)` instead.

PyTorch Dataset for AIS.

Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.

<a id="map_dataset.AISMapDataset"></a>

## Class: AISMapDataset

```python
class AISMapDataset(AISBaseMapDataset)
```

A map-style dataset for objects in AIS.
If `etl_name` is provided, that ETL must already exist on the AIStore cluster.

**Arguments**:

- `ais_source_list` _Union[AISSource, List[AISSource]]_ - Single or list of AISSource objects to load data
  prefix_map (Dict(AISSource, List[str]), optional): Map of AISSource objects to list of prefixes that only allows
  objects with the specified prefixes to be used from each source
- `etl_name` _str, optional_ - Optional ETL on the AIS cluster to apply to each object
  
  NOTE:
  Each object is represented as a tuple of object_name (str) and object_content (bytes)

Iterable Dataset for AIS

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.

<a id="iter_dataset.AISIterDataset"></a>

## Class: AISIterDataset

```python
class AISIterDataset(AISBaseIterDataset)
```

An iterable-style dataset that iterates over objects in AIS and yields
samples represented as a tuple of object_name (str) and object_content (bytes).
If `etl_name` is provided, that ETL must already exist on the AIStore cluster.

**Arguments**:

- `ais_source_list` _Union[AISSource, List[AISSource]]_ - Single or list of AISSource objects to load data
  prefix_map (Dict(AISSource, Union[str, List[str]]), optional): Map of AISSource objects to list of prefixes that only allows
  objects with the specified prefixes to be used from each source
- `etl_name` _str, optional_ - Optional ETL on the AIS cluster to apply to each object
- `show_progress` _bool, optional_ - Enables console dataset reading progress indicator
  

**Yields**:

  Tuple[str, bytes]: Each item is a tuple where the first element is the name of the object and the
  second element is the byte representation of the object data.

AIS Shard Reader for PyTorch

PyTorch Dataset and DataLoader for AIS.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.

<a id="shard_reader.AISShardReader"></a>

## Class: AISShardReader

```python
class AISShardReader(AISBaseIterDataset)
```

An iterable-style dataset that iterates over objects stored as Webdataset shards
and yields samples represented as a tuple of basename (str) and contents (dictionary).

**Arguments**:

- `bucket_list` _Union[Bucket, List[Bucket]]_ - Single or list of Bucket objects to load data
  prefix_map (Dict(AISSource, Union[str, List[str]]), optional): Map of Bucket objects to list of prefixes that only allows
  objects with the specified prefixes to be used from each source
- `etl_name` _str, optional_ - Optional ETL on the AIS cluster to apply to each object
- `show_progress` _bool, optional_ - Enables console shard reading progress indicator
  

**Yields**:

  Tuple[str, Dict(str, bytes)]: Each item is a tuple where the first element is the basename of the shard
  and the second element is a dictionary mapping strings of file extensions to bytes.

<a id="shard_reader.AISShardReader.__len__"></a>

### \_\_len\_\_

```python
def __len__()
```

Returns the length of the dataset. Note that calling this
will iterate through the dataset, taking O(N) time.

NOTE: If you want the length of the dataset after iterating through
it, use `for i, data in enumerate(dataset)` instead.

<a id="shard_reader.AISShardReader.ZeroDict"></a>

## Class: ZeroDict

```python
class ZeroDict(dict)
```

When `collate_fn` is called while using ShardReader with a dataloader,
the content dictionaries for each sample are merged into a single dictionary
with file extensions as keys and lists of contents as values. This means,
however, that each sample must have a value for that file extension in the batch
at iteration time or else collation will fail. To avoid forcing the user to
pass in a custom collation function, we workaround the default implementation
of collation.

As such, we define a dictionary that has a default value of `b""` (zero bytes)
for every key that we have seen so far. We cannot use None as collation
does not accept None. Initially, when we open a shard tar, we collect every file type
(pre-processing pass) from its members and cache those. Then, we read the shard files.
Lastly, before yielding the sample, we wrap its content dictionary with this custom dictionary
to insert any keys that it does not contain, hence ensuring consistent keys across
samples.

NOTE: For our use case, `defaultdict` does not work due to needing
a `lambda` which cannot be pickled in multithreaded contexts.

Worker Supported Request Client for PyTorch

This client allows PyTorch workers to have separate request sessions per thread
which is needed in order to use workers in a DataLoader as
the default implementation of RequestClient and requests is not thread-safe.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.

<a id="worker_request_client.WorkerRequestClient"></a>

## Class: WorkerRequestClient

```python
class WorkerRequestClient(RequestClient)
```

Extension that supports PyTorch and multiple workers of internal client for
buckets, objects, jobs, etc. to use for making requests to an AIS cluster.

**Arguments**:

- `client` _RequestClient_ - Existing RequestClient to replace

<a id="worker_request_client.WorkerRequestClient.session"></a>

### session

```python
@property
def session()
```

Returns: Active request session acquired for a specific PyTorch dataloader worker

Multishard Stream Dataset for AIS.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.

<a id="multishard_dataset.AISMultiShardStream"></a>

## Class: AISMultiShardStream

```python
class AISMultiShardStream(IterableDataset)
```

An iterable-style dataset that iterates over multiple shard streams and yields combined samples.

**Arguments**:

- `data_sources` _List[DataShard]_ - List of DataShard objects
  

**Returns**:

- `Iterable` - Iterable over the combined samples, where each sample is a tuple of
  one object bytes from each shard stream

AIS IO Datapipe
Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.

<a id="aisio.AISFileListerIterDataPipe"></a>

## Class: AISFileListerIterDataPipe

```python
@functional_datapipe("ais_list_files")
class AISFileListerIterDataPipe(IterDataPipe[str])
```

Iterable Datapipe that lists files from the AIStore backends with the given URL prefixes.
(functional name: ``list_files_by_ais``).
Acceptable prefixes include but not limited to - `ais://bucket-name`, `ais://bucket-name/`

**Notes**:

  -   This function also supports files from multiple backends (`aws://..`, `gcp://..`, etc.)
  -   Input must be a list and direct URLs are not supported.
  -   length is -1 by default, all calls to len() are invalid as
  not all items are iterated at the start.
  -   This internally uses AIStore Python SDK.

**Arguments**:

- `source_datapipe(IterDataPipe[str])` - a DataPipe that contains URLs/URL
  prefixes to objects on AIS
- `length(int)` - length of the datapipe
- `url(str)` - AIStore endpoint

**Example**:

  >>> from torchdata.datapipes.iter import IterableWrapper, AISFileLister
  >>> ais_prefixes = IterableWrapper(['gcp://bucket-name/folder/', 'aws:bucket-name/folder/',
  >>>        'ais://bucket-name/folder/', ...])
  >>> dp_ais_urls = AISFileLister(url='localhost:8080', source_datapipe=ais_prefixes)
  >>> for url in dp_ais_urls:
  ...     pass
  >>> # Functional API
  >>> dp_ais_urls = ais_prefixes.list_files_by_ais(url='localhost:8080')
  >>> for url in dp_ais_urls:
  ...     pass

<a id="aisio.AISFileLoaderIterDataPipe"></a>

**Notes**:

  - `http://localhost:8080` address (above and elsewhere) is used for purely demonstration purposes and must be understood as a placeholder for an _arbitrary_ AIStore endpoint (`AIS_ENDPOINT`).

## Class: AISFileLoaderIterDataPipe

```python
@functional_datapipe("ais_load_files")
class AISFileLoaderIterDataPipe(IterDataPipe[Tuple[str, StreamWrapper]])
```

Iterable DataPipe that loads files from AIStore with the given URLs (functional name: ``load_files_by_ais``).
Iterates all files in BytesIO format and returns a tuple (url, BytesIO).

**Notes**:

  -   This function also supports files from multiple backends (`aws://..`, `gcp://..`, etc)
  -   Input must be a list and direct URLs are not supported.
  -   This internally uses AIStore Python SDK.
  -   An `etl_name` can be provided to run an existing ETL on the AIS cluster.
  See https://github.com/NVIDIA/aistore/blob/main/docs/etl.md for more info on AIStore ETL.

**Arguments**:

- `source_datapipe(IterDataPipe[str])` - a DataPipe that contains URLs/URL prefixes to objects
- `length(int)` - length of the datapipe
- `url(str)` - AIStore endpoint
- `etl_name` _str, optional_ - Optional etl on the AIS cluster to apply to each object

**Example**:

  >>> from torchdata.datapipes.iter import IterableWrapper, AISFileLister,AISFileLoader
  >>> ais_prefixes = IterableWrapper(['gcp://bucket-name/folder/', 'aws:bucket-name/folder/',
  >>>     'ais://bucket-name/folder/', ...])
  >>> dp_ais_urls = AISFileLister(url='localhost:8080', source_datapipe=ais_prefixes)
  >>> dp_cloud_files = AISFileLoader(url='localhost:8080', source_datapipe=dp_ais_urls)
  >>> for url, file in dp_cloud_files:
  ...     pass
  >>> # Functional API
  >>> dp_cloud_files = dp_ais_urls.load_files_by_ais(url='localhost:8080')
  >>> for url, file in dp_cloud_files:
  ...     pass

<a id="aisio.AISSourceLister"></a>

## Class: AISSourceLister

```python
@functional_datapipe("ais_list_sources")
class AISSourceLister(IterDataPipe[str])
```

<a id="aisio.AISSourceLister.__init__"></a>

### \_\_init\_\_

```python
def __init__(ais_sources: List[AISSource], prefix="", etl_name=None)
```

Iterable DataPipe over the full URLs for each of the provided AIS source object types

**Arguments**:

- `ais_sources` _List[AISSource]_ - List of types implementing the AISSource interface: Bucket, ObjectGroup,
  Object, etc.
- `prefix` _str, optional_ - Filter results to only include objects with names starting with this prefix
- `etl_name` _str, optional_ - Pre-existing ETL on AIS to apply to all selected objects on the cluster side

