# AIS Plugin for PyTorch

## PyTorch Dataset and DataLoader for AIS 


AIS plugin is a PyTorch dataset library to access datasets stored on AIStore.

PyTorch comes with powerful data loading capabilities, but loading data in PyTorch is fairly complex. One of the best ways to handle it is to start small and then add complexities as and when you need them. Below are some of the ways one can import data stored on AIS in PyTorch.

### PyTorch DataLoader

The PyTorch DataLoader class gives you an iterable over a Dataset. It can be used to shuffle, batch and parallelize operations over your data.

### PyTorch Dataset

But, to create a DataLoader you have to first create a Dataset, which is a class to read samples into memory. Most of the logic of the DataLoader resides on the Dataset.

PyTorch offers two styles of Dataset class: Map-style and Iterable-style. 

#### ***Map-style Dataset***

A map-style dataset in PyTorch implements the `__getitem__()` and `__len__()` functions and provides the user a map from indices/keys to data samples.

For example, we can access the i-th index label and its corresponding image by dataset[i] from a bucket in AIStore.

```
from aistore.pytorch.dataset import AISDataset

dataset = AISDataset(client_url="http://ais-gateway-url:8080", urls_list='ais://bucket1/')

for i in range(len(dataset)): # calculate length of all items present using len() function
    print(dataset[i]) # get object url and byte array of the object

```

**Note:** ```urls_list``` can be a single prefix url or a list of prefixs. Eg. ```"ais://bucket1/file-"``` or ```["aws://bucket2/train/", "ais://bucket3/train/"]```

#### ***Iterable-style datasets***

Iterable-style datasets are an instance of a subclass of the IterableDataset. Every Iterable-style dataset has to implement the `iter()` function, which represents iterable over a sample. This kind of dataset is mostly suitable for tasks where random reads are expensive or even not possible.

Examples of such datasets include a stream of data readings from a database, remote server, or logs-stream.

More information on Iterable-style datasets in PyTorch can be found [here](https://pytorch.org/data/main/torchdata.datapipes.iter.html).

**Currently, we are working on supporting Iterable-style datasets for AIS backends**

**Creating DataLoader from AISDataset**
```
from aistore.pytorch.dataset import AISDataset

train_loader = torch.utils.data.DataLoader(
    AISDataset(
        "http://ais-gateway-url:8080", urls_list=["ais://dataset1/train/", "ais://dataset2/train/"],
    batch_size=args.batch_size, shuffle=True,
    num_workers=args.workers, pin_memory=True,
)

```

## AIS IO Datapipe

### AIS File Lister

Iterable Datapipe that lists files from the AIS backends with the given URL  prefixes. Acceptable prefixes include but not limited to - `ais://bucket-name`, `ais://bucket-name/`, `ais://bucket-name/folder`, `ais://bucket-name/folder/`, `ais://bucket-name/prefix`. 

**Note:** 
1) This function also supports files from multiple backends (`aws://..`, `gcp://..`, etc)
2) Input *must* be a list and direct URLs are not supported.
3) `length` is -1 by default, all calls to `len()` are invalid as not all items are iterated at the start.
4) This internally uses [AIStore Python SDK](https://gitlab-master.nvidia.com/aistorage/aistore/-/tree/master/python/aistore/sdk).

### AIS File Loader

Iterable Datapipe that loads files from the AIS backends with the given list of URLs (no prefixes allowed). Iterates all files in BytesIO format and returns a tuple (url, BytesIO). 
**Note:** 
1) This function also supports files from multiple backends (`aws://..`, `gcp://..`, etc)
2) Input *must* be a list and direct URLs are not supported.
3) This internally uses [AIStore Python SDK](https://gitlab-master.nvidia.com/aistorage/aistore/-/tree/master/python/aistore/sdk).

### Example
```
from aistore.pytorch.aisio import AISFileListerIterDataPipe, AISFileLoaderIterDataPipe

prefixes = ['ais://bucket1/train/', 'aws://bucket2/train/']

list_of_files = AISFileListerIterDataPipe(url='http://ais-gateway-url:8080', source_datapipe=prefixes)

files = AISFileLoaderIterDataPipe(url='http://ais-gateway-url:8080', source_datapipe=list_of_files) 
```