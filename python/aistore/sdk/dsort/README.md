# Using the DsortFramework in Python SDK

## DsortFramework

The `DsortFramework` class in the Python SDK enables you to define and manage dSort jobs. As a framework, `DsortFramework` itself does not make any HTTP requests until it is passed to the `Client.dsort` instance. It provides an abstracted way to configure your desired dSort job in a Pythonic manner, consistent with the AIStore SDK style. Below are instructions on how to construct a `DsortFramework` instance and start a dSort job directly from the Python SDK.

### Example Usage

1. **Creating a DsortFramework from a JSON/YAML Specification File:**

   ```python
   from aistore.sdk.dsort import DsortFramework

   # Create a DsortFramework instance from a specification file
   dsort_framework = DsortFramework.from_file("path/to/spec.json")

   # Start the dSort job
   client.dsort().start(dsort_framework)
   ```

2. **Creating a DsortFramework Directly:**

   ```python
    from aistore.sdk import Client
    from aistore.sdk.multiobj import ObjectNames, ObjectRange
    from aistore.sdk.dsort import DsortFramework, DsortShardsGroup, ExternalKeyMap

    # Initialize the AIStore client
    client = Client("http://your-aistore-url:8080")

    # Define the input bucket
    input_bucket = client.bucket(bck_name="input-bucket")

    # Define the output bucket
    output_bucket = client.bucket(bck_name="output-bucket")

    # Define the input format as ObjectRange
    input_format = ObjectRange(
        prefix="input-shard-",
        min_index=0,
        max_index=99,
        pad_width=2,
        suffix=".tar",
    )

    # Define the output format as ExternalKeyMap
    output_format = ExternalKeyMap()
    output_format["output-shard-0"] = ObjectNames(objnames=["file1.txt", "file2.txt"])
    output_format["output-shard-1"] = ObjectNames(objnames=["file3.txt", "file4.txt"])

    # Define the input shards group
    input_shards_group = DsortShardsGroup(
        bck=input_bucket.as_model(),
        role="input",
        format=input_format,
        extension=".tar",
    )

    # Define the output shards group
    output_shards_group = DsortShardsGroup(
        bck=output_bucket.as_model(),
        role="output",
        format=output_format,
        extension=".tar",
    )

    # Define the dSort framework
    dsort_framework = DsortFramework(
        input_shards=input_shards_group,
        output_shards=output_shards_group,
        description="Example dSort job with ObjectRange and ExternalKeyMap",
        output_shard_size="100KiB",
    )

    # Convert the framework to a dictionary
    dsort_spec = dsort_framework.to_spec()

    # Start the dSort job
    client.dsort().start(dsort_spec)
   ```

   ```bash
   # Input shards structure
   input-shard-00.tar
    ├── file1.txt
    ├── file3.txt
    └── file4.txt
   input-shard-01.tar
    └── file2.txt

    # Expected output shards structure
    output-shard-0.tar
    ├── file1.txt
    └── file2.txt
    output-shard-1.tar
    ├── file3.txt
    └── file4.txt
   ```

## DsortShardsGroup

The `DsortShardsGroup` class represents a set of shards (either input end or output end) involved during the dSort transformation. There are four fields to fully describe such a set of shards:
- `role`: Specifies whether this instance represents the input set or output set of the dSort job. Valid values are "input" or "output".
- `bck`: Indicates the source (for input) or destination (for output) bucket where the data of the dSort job should reside in AIStore.
- `extension`: Specifies the file extension used for the shards (e.g., .tar, .tgz, .zip).
- `format` Provides a template for a dSort job to acquire (for input) or generate (for output) target shards. The format can be one of the following types:
  - For the `input` role, AIStore currently supports `ObjectNames` or `ObjectRange` formats.
  - For the `output` role, AIStore currently supports `ObjectRange` or `ExternalKeyMap` formats.

### Example Usage

```python
from aistore.sdk.types import BucketModel
from aistore.sdk.multiobj import ObjectNames, ObjectRange

input_bucket = BucketModel(name="input_bucket", provider="aws")
output_bucket = BucketModel(name="output_bucket", provider="aws")

input_format = ObjectNames(objnames=["input-shard-1", "input-shard-2"])
output_format = ObjectRange(prefix="output-shard-", min_index=0, max_index=99, pad_width=2)

input_shards_group = DsortShardsGroup(
    bck=input_bucket,
    role="input",
    format=input_format,
    extension=".tar"
)

output_shards_group = DsortShardsGroup(
    bck=output_bucket,
    role="output",
    format=output_format,
    extension=".tar"
)

print(input_shards_group.as_dict())
print(output_shards_group.as_dict())
```

## ExternalKeyMap

The `ExternalKeyMap` (EKM) class provides users with an interface to specify the exact mapping from the record key to the output shard. Specifically, `ExternalKeyMap` is a dictionary that specifies which `ObjectNames` belong to which output shard (named as a template string). For each entry of the `ExternalKeyMap`, objects within the `ObjectNames` instance that appear in the input `DsortShardsGroup` will be collected into one of the specified output shards, according to the order defined by the specified algorithm and the size specified in `output_shard_size`.

### Example Usage

```python
from aistore.sdk.multiobj import ObjectNames

# Create an instance of ExternalKeyMap
ekm = ExternalKeyMap()

# Add ObjectNames instances to the ExternalKeyMap with corresponding shard formats
ekm["output-shard-0"] = ObjectNames(objnames=["file1.txt", "file2.txt"])
ekm["output-shard-1"] = ObjectNames(objnames=["file3.txt", "file4.txt"])

# Convert to dictionary representation
print(ekm.as_dict())
```

For more detailed information, refer to the original [dSort documentation](https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md). Also see the [dSort CLI](https://github.com/NVIDIA/aistore/blob/main/docs/cli/dsort.md).
