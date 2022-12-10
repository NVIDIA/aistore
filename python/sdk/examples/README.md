# AIStore Python SDK Examples

Here is a curated list of examples that use AIStore Python SDK:

| Filename | Description |
| --- | --- |
| [sdk-tutorial.ipynb](python/sdk/examples/sdk/sdk-etl-tutorial.ipynb) | AIStore Python APIs to access and utilize AIS clusters, buckets, and objects |
| [sdk-etl-tutorial.ipynb](python/sdk/examples/sdk/sdk-etl-tutorial.ipynb) | AIStore Python APIs related to initializing, starting, stopping and deleting ETLs. Also includes transforming buckets and objects with ETLs |
| [aisio_pytorch_example.ipynb](python/sdk/examples/aisio-pytorch/aisio_pytorch_example.ipynb) | Listing and loading data from AIS buckets (buckets that are not 3rd party backend-based) and remote cloud buckets (3rd party backend-based cloud buckets) using [AISFileLister](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLister.html#aisfilelister) and [AISFileLoader](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLoader.html#torchdata.datapipes.iter.AISFileLoader) |
| [dask-aistore-demo.ipynb](python/sdk/examples/dask/dask-aistore-demo.ipynb) | Demonstration of using Dask DataFrames for data analysis with data on AIStore |
| [etl_convert_img_to_npy.py](python/sdk/examples/ais-etl/etl_convert_img_to_npy.py) | AIS-ETL to convert images to numpy arrays using communication type: `hpush://`|
| [etl_md5_hpush.py](python/sdk/examples/ais-etl/etl_md5_hpush.py) | AIS-ETL to calculate md5 of an object using communication type: `hpush://`|
| [etl_md5_hpush_streaming.py](python/sdk/examples/ais-etl/etl_md5_hpush_streaming.py) | AIS-ETL to calculate md5 of an object using communication type: `hpush://`. Demonstrates the use of `before()` and `after()` functions with streaming using `CHUNK_SIZE`|
| [etl_md5_io.py](python/sdk/examples/ais-etl/etl_md5_io.py) | AIS-ETL to calculate md5 of an object using communication type: `io://` |
| [etl_torchvision_io.py](python/sdk/examples/ais-etl/etl_torchvision_io.py) | AIS-ETL to transform images with torchvision using communication type: `io://` |
| [etl_torchvision_hpush.py](python/sdk/examples/ais-etl/etl_torchvision_hpush.py) | AIS-ETL to transform images with torchvision using communication type: `hpush://` |
