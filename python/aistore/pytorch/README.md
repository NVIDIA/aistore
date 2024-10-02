# AIS Plugin for PyTorch

## PyTorch Dataset and DataLoader for AIS

AIS plugin is a PyTorch dataset library to access datasets stored on AIStore.

PyTorch comes with powerful data loading capabilities, but loading data in PyTorch is fairly complex. One of the best ways to handle it is to start small and then add complexities as and when you need them.

![PyTorch Structure](/docs/images/pytorch_structure.webp)

In our plugin, we extend the base Dataset, Sampler, and IterableDataset Torch clases to provide AIStore Object functionality natively to PyTorch. You can extend AISBaseMapDataset instead of Dataset and AISBaseIterDataset instead of IterableDataset in your custom datasets to automatically obtain object fetching functionality. But if you'd like fully complete datasets that fetch objects and load their data, then you can use AISMapDataset and AISIterData.

### PyTorch DataLoader

The PyTorch DataLoader class gives you an iterable over a Dataset. It can be used to shuffle, batch and parallelize operations over your data.

### PyTorch Dataset

To create a DataLoader, you need to first create a Dataset, which is a class to read samples into memory. Most of the logic of the DataLoader resides on the Dataset.

PyTorch offers two styles of Dataset class: Map-style and Iterable-style. We have implemented ```AISMapDataset``` and ```AISIterDataset``` to load objects and their data for you.

**Note:** Both datasets can be initialized with an ais_source_list parameter that defines which objects to reference in AIS.
 An AISSource is any AIS SDK object that defines a set of storage objects. Currently, this includes buckets and object groups.
```ais_source_list``` can be a single [AISSource](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/ais_source.py) object or a list of [AISSource](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/ais_source.py) objects. Eg. ```"Client.bucket()``` or ```[Client.bucket("bucket1"), Client.bucket("bucket2")]```.

Additionally, if you want to create your own custom datasets,
extend ```AISBaseMapDataset``` and ```AISBaseIterDataset``` depending on the style you want.
Note that for all datasets, you can override ```_get_sample_iter_from_source(self, source: AISSource, prefix: str)``` to change the behavior of how data is obtained from the source. For example, this is used in ```ShardReader``` as the sources contain WebDataset formatted objects.

#### ***Map-style Dataset***

A map-style dataset in PyTorch implements the `__getitem__()` and `__len__()` functions and provides the user a map from indices/keys to data samples.

For example, we can access the i-th index label and its corresponding image by ```dataset[i]``` from a bucket in AIStore.

```python
from aistore.pytorch import AISMapDataset
from aistore.sdk import Client
import os

ais_url = os.getenv("AIS_ENDPOINT", "http://localhost:8080")
client = Client(ais_url)

dataset = AISMapDataset(ais_source_list=[client.bucket(bck_name="bucket1"), client.bucket(bck_name="bucket2")])

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
dataset = AISIterDataset(ais_source_list=my_objects)

# Iterate over the dataset to fetch data samples as a stream
for data_sample in dataset:
    print(data_sample)  # Each iteration fetches a data sample (object name and byte array)
```

For more examples on how to use AISMapDataset and AISIterDataset, see the [Dataset Example Notebook](../../examples/aisio-pytorch/dataset_example.ipynb).


**Creating DataLoader from AISMapDataset**
```python
from aistore.pytorch import AISMapDataset
from aistore.sdk import Client
import os

ais_url = os.getenv("AIS_ENDPOINT", "http://localhost:8080")
client = Client(ais_url)
dataset1_bck = client.bucket("dataset1").create(exist_ok=True)
dataset2_bck = client.bucket("dataset2").create(exist_ok=True)

train_loader = torch.utils.data.DataLoader(
    AISMapDataset(
        ais_source_list = [dataset1_bck, dataset2_bck]
    ),
    batch_size=args.batch_size, shuffle=True,
    num_workers=args.workers, pin_memory=True,
)
```

**Using ShardReader to read WebDataset Formatted Shards**
```python
from aistore.pytorch import AISShardReader
from aistore.sdk import Client
import os

ais_url = os.getenv("AIS_ENDPOINT", "http://localhost:8080")
client = Client(ais_url)
bucket = client.bucket("dataset-example").create(exist_ok=True)

# Note that shard_reader currently only supports buckets
shard_reader = AISShardReader(
    bucket_list=bucket
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

For a more in-depth example, see [here](https://github.com/NVIDIA/aistore/blob/main/python/examples/aisio-pytorch/aisio_pytorch_example.ipynb)
