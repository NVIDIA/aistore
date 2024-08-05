# Dataset Module for Python SDK

The Dataset Module in the Python SDK offers robust tools to create and manage datasets for data science and machine learning projects. It simplifies the handling of data transformations, loading, and storage, providing a streamlined workflow for users. It utilizes the [WebDataset](https://github.com/webdataset/webdataset) format to store the datasets, with support for a number of different [backend providers](https://aistore.nvidia.com/docs/providers).

WebDataset is the chosen format due to its compatibility with AIStore's capabilities for efficiently managing TAR files. This choice enhances performance and flexibility for operations such as reading, writing, listing, and appending to TAR archives. For more details on AIStore's handling of TAR files, see:

- [AIStore Archive Operations](https://aistore.nvidia.com/docs/archive)
- [AIStore and TAR Append](https://aistore.nvidia.com/blog/2021/08/10/tar-append)

## Features

- **[Writing Datasets](#writing-datasets)**:
  - Write datasets to a bucket in the WebDataset format. This format is particularly suited for large-scale machine learning datasets due to its efficiency and flexibility.

In future we are planning to expand our dataset module with several exciting features:

- **Dataset Reader**: Enabling efficient reading and manipulation of datasets stored in AIStore directly through our Python SDK.
- **ETL Operations**: Integrating [ETL](https://aistore.nvidia.com/docs/etl) capabilities to facilitate complex data transformations and processing, enhancing data preparation workflows.
- **Dsort**: Implementing [Dsort](https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md) to provide high-performance, scalable sorting solutions, optimizing data organization and retrieval processes within AIStore.

## Writing Datasets

The `write_dataset` function enables writing datasets as shards directly into a bucket using the WebDataset `ShardWriter`. Here's how you can use the `write_dataset` function:

```python
from pathlib import Path
from aistore.sdk import Client
from aistore.sdk.dataset.dataset_config import DatasetConfig
from aistore.sdk.dataset.data_attribute import DataAttribute
from aistore.sdk.dataset.label_attribute import LabelAttribute

ais_url = os.getenv("AIS_ENDPOINT", "http://localhost:8080")
client = Client(ais_url)
bucket = client.bucket("my-bck").create(exist_ok=True)

dataset_config = DatasetConfig(
    primary_attribute=DataAttribute(path=Path("your/image/directory"), file_type="jpg", name="image"),
    secondary_attributes=[
        LabelAttribute(name="cls", label_identifier=your_class_lookup_fn)
    ],
)

bucket.write_dataset(config=dataset_config, pattern="img_dataset")
```

**Note:** This is a beta feature and is still in development.

## References

- [WebDataset](https://github.com/webdataset/webdataset)
- [AIStore Archive Operations](https://aistore.nvidia.com/docs/archive)
- [AIStore and TAR Append](https://aistore.nvidia.com/blog/2021/08/10/tar-append)
