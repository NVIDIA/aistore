# Using the DsortFramework in Python SDK

The `DsortFramework` class in the Python SDK provides a way to define and manage dSort jobs. Below is an instruction on how to construct a `DsortFramework` instance and start a dSort job directly from the Python SDK.

## Key Fields

- **extension**: The extension of input and output shards (either .tar, .tgz or .zip).
- **input_format.template**: Name template for input shard.
- **output_format**: Name template for output shard.
- **input_bck.name**: Bucket name where shards objects are stored.
- **input_bck.provider**: Bucket backend provider.
- **output_bck.name**: Bucket name where new output shards will be saved.
- **output_bck.provider**: Bucket backend provider.
- **description**: Description of the dSort job.
- **output_shard_size**: Size (in bytes) of the output shard, can be in the form of raw numbers (e.g., 10240) or suffixed (e.g., 10KB).
- **algorithm.kind**: Determines which sorting algorithm dSort job uses. Available options are: "alphanumeric", "shuffle", "content".
- **algorithm.decreasing**: Determines if the algorithm should sort the records in decreasing or increasing order, used for kind=alphanumeric or kind=content.
- **algorithm.seed**: Seed provided to the random generator, used when kind=shuffle.
- **algorithm.extension**: Content of the file with the provided extension will be used as the sorting key, used when kind=content.
- **algorithm.content_key_type**: Content key type; may have one of the following values: "int", "float", or "string"; used exclusively with kind=content sorting.

## Example Usage

1. **Creating a DsortFramework from a JSON/YAML Specification File:**

   ```python
   from aistore.sdk.dsort import DsortFramework

   # Create a DsortFramework instance from a specification file
   dsort_framework = DsortFramework.from_file("path/to/spec.json")

   # Start the dSort job
   dsort_job_id = client.dsort().start(dsort_framework)
   ```

2. **Creating a DsortFramework Directly:**

   ```python
   from aistore.sdk.dsort import DsortFramework, DsortShardsGroup, DsortAlgorithm
   from aistore.sdk.types import BucketModel

   # Define the input and output shard configurations
   input_shards = DsortShardsGroup(
       bck=BucketModel(name="input_bucket", provider="aws"),
       role="input",
       format={"template": "input_template"},
       extension=".tar"
   )

   output_shards = DsortShardsGroup(
       bck=BucketModel(name="output_bucket", provider="aws"),
       role="output",
       format="output_template",
       extension=".tar"
   )

   # Define the algorithm configuration
   algorithm = DsortAlgorithm(
       kind="content",
       decreasing=False,
       seed="",
       extension=".key",
       content_key_type="int"
   )

   # Create a DsortFramework instance
   dsort_framework = DsortFramework(
       input_shards=input_shards,
       output_shards=output_shards,
       algorithm=algorithm,
       description="Dsort Job Description",
       output_shard_size="10MB"
   )

   # Start the dSort job
   dsort_job_id = client.dsort().start(dsort_framework)
   ```

## Important Notes

- **Input and Output Formats:**
  - The `input_format` must be a dictionary containing the key "template".
  - The `output_format` must be a string.
  
- **Algorithm Kind:**
  - For the `content` kind, `extension` and `content_key_type` fields are required.
  - For other kinds, these fields should not be provided.

- **Error Handling:**
  - Validation errors are raised if the required fields are missing or incorrectly formatted.
  
For more detailed information, refer to the original [dSort documentation](https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md). Also see the [dSort CLI](https://github.com/NVIDIA/aistore/blob/main/docs/cli/dsort.md).
