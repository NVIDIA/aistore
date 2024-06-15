# AIS Plugin for PyTorch

## PyTorch Dataset and DataLoader for AIS

AIS plugin is a PyTorch dataset library to access datasets stored on AIStore.

PyTorch comes with powerful data loading capabilities, but loading data in PyTorch is fairly complex. One of the best ways to handle it is to start small and then add complexities as and when you need them. Below are some of the ways one can import data stored on AIS in PyTorch.

### PyTorch DataLoader

The PyTorch DataLoader class gives you an iterable over a Dataset. It can be used to shuffle, batch and parallelize operations over your data.

### PyTorch Dataset

To create a DataLoader, you need to first create a Dataset, which is a class to read samples into memory. Most of the logic of the DataLoader resides on the Dataset.

PyTorch offers two styles of Dataset class: Map-style and Iterable-style.

**Note:** Both datasets can be initialized with a urls_list parameter and/or an ais_source_list parameter that defines which objects to reference in AIS.
```urls_list``` can be a single prefix url or a list of prefixes. Eg. ```"ais://bucket1/file-"``` or ```["aws://bucket2/train/", "ais://bucket3/train/"]```.
Likewise ```ais_source_list``` can be a single [AISSource](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/ais_source.py) object or a list of [AISSource](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/ais_source.py) objects. Eg. ```"Client.bucket()``` or ```[Client.bucket("bucket1"), Client.bucket("bucket2")]```.

#### ***Map-style Dataset***

A map-style dataset in PyTorch implements the `__getitem__()` and `__len__()` functions and provides the user a map from indices/keys to data samples.

For example, we can access the i-th index label and its corresponding image by dataset[i] from a bucket in AIStore.

```python
from aistore.pytorch.dataset import AISDataset
from aistore.sdk import Client
import os

ais_url = os.getenv("AIS_ENDPOINT", "http://localhost:8080")

dataset = AISDataset(client_url=ais_url, urls_list='ais://bucket1/')

# or, if using ais_source_list
# client = Client(ais_url)
# dataset = AISDataset(client_url=ais_url, ais_source_list=[client.bucket(bck_name="bucket1"), client.bucket(bck_name="bucket2")])

for i in range(len(dataset)):
    print(dataset[i])  # Get object URL and byte array of the object

```


#### ***Iterable-style datasets***

Iterable-style datasets in PyTorch are tailored for scenarios where data needs to be processed as a stream, and direct indexing is either not feasible or inefficient. Such datasets inherit from IterableDataset and override the `__iter__()` method, providing an iterator over the data samples.

These datasets are particularly useful when dealing with large data streams that cannot be loaded entirely into memory, or when data is continuously generated or fetched from external sources like databases, remote servers, or live data feeds.

We have extended support for iterable-style datasets to AIStore (AIS) backends, enabling efficient streaming of data directly from AIS buckets. This approach is ideal for training models on large datasets stored in AIS, minimizing memory overhead and facilitating seamless data ingestion from AIS's distributed object storage.

Here's how you can use an iterable-style dataset with AIStore:

```python
from aistore.pytorch.dataset import AISIterDataset
from aistore.sdk import Client
import os

ais_url = os.getenv("AIS_ENDPOINT", "http://localhost:8080")
client = Client(ais_url)
bucket = client.bucket("my-bck").create(exist_ok=True)

# Creating objects in our bucket
object_names = [f"example_obj_{i}" for i in range(10)]
for name in object_names:
    bucket.object(name).put_content("object content".encode("utf-8"))

# Creating an object group
my_objects = bucket.objects(obj_names=object_names)

# Initialize the dataset with the AIS client URL and the data source location
dataset = AISIterDataset(client_url=ais_url, urls_list="ais://my-bck/", ais_source_list=my_objects)

# Iterate over the dataset to fetch data samples as a stream
for data_sample in dataset:
    print(data_sample)  # Each iteration fetches a data sample (object name and byte array)

```


**Creating DataLoader from AISDataset**
```python
from aistore.pytorch.dataset import AISDataset

train_loader = torch.utils.data.DataLoader(
    AISDataset(
        "http://ais-gateway-url:8080", urls_list=["ais://dataset1/train/", "ais://dataset2/train/"]),
    batch_size=args.batch_size, shuffle=True,
    num_workers=args.workers, pin_memory=True,
)

```

**Using ShardReader to read WebDataset Formatted Shards**
```python
from aistore.pytorch.dataset import ShardReader

shard_reader = AISShardReader(
    client_url="http://ais-gateway-url:8080", urls_list=["ais://dataset1/example.tar/", "ais://dataset2/example.tar/"] # bucket_list=[bucket] can also pass in other sources
)

for basename, content_dict in shard_reader:
    # We now have the basenames and content dictionary (file extension, bytes) for every sample
    # Since you know the file extension, we can load the file content in the appropriate way
```

See the [ShardReader example notebook](../../examples/aisio-pytorch/shard_reader_example.ipynb) for more examples. Since the shard reader is also an iterable dataset, you can also use it with the `torch.utils.data.DataLoader` class for additional features.

## AIS IO Datapipe

### AIS File Lister

Iterable Datapipe that lists files from the AIS backends with the given URL  prefixes. Acceptable prefixes include but not limited to - `ais://bucket-name`, `ais://bucket-name/`, `ais://bucket-name/folder`, `ais://bucket-name/folder/`, `ais://bucket-name/prefix`.

**Note:**
1) This function also supports files from multiple backends (`aws://..`, `gcp://..`, etc)
2) Input *must* be a list and direct URLs are not supported.
3) `length` is -1 by default, all calls to `len()` are invalid as not all items are iterated at the start.
4) This internally uses [AIStore Python SDK](https://github.com/NVIDIA/aistore/tree/main/python).

### AIS File Loader

Iterable Datapipe that loads files from the AIS backends with the given list of URLs (no prefixes allowed). Iterates all files in BytesIO format and returns a tuple (url, BytesIO).
**Note:**
1) This function also supports files from multiple backends (`aws://..`, `gcp://..`, etc)
2) Input *must* be a list and direct URLs are not supported.
3) This internally uses [AIStore Python SDK](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk).

### Example
```python
from aistore.pytorch.aisio import AISFileListerIterDataPipe, AISFileLoaderIterDataPipe

prefixes = ['ais://bucket1/train/', 'aws://bucket2/train/']

list_of_files = AISFileListerIterDataPipe(url='http://ais-gateway-url:8080', source_datapipe=prefixes)

files = AISFileLoaderIterDataPipe(url='http://ais-gateway-url:8080', source_datapipe=list_of_files)
```

For a more in-depth example, see [here](https://github.com/NVIDIA/aistore/blob/main/python/examples/aisio_pytorch_example.ipynb)
