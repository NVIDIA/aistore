# PyAISLoader

PyAISLoader is a CLI for running benchmarks that leverage the AIStore Python SDK.

## Getting Started

From `aistore/python/pyaisloader`, run the following to install all required dependencies:

```shell
make install
```

## Usage

The general usage is:

```shell
pyaisloader [TYPE] --bucket [BUCKET] --workers [WORKERS] --cleanup ...
```

> Options are specific to the type of benchmark being performed. For more information on the benchmark-specific options, run `pyaisloader PUT --help`, `pyaisloader GET --help`, `pyaisloader MIXED --help`, or `pyaisloader LIST --help`, or refer to the documentation below.

> For all benchmark types, `--cleanup`, or `-c`, if set to `True`, clean-up will either **(i)** destroy the entire bucket if the benchmark created the bucket or **(ii)** destroy any objects that were added to the pre-existing bucket during the benchmark (and pre-population). 

## Benchmark Results

Results from all workers are combined before they are displayed. `Latency Avg` is the total operation time divided by the total number of operations across all workers, so workers are weighted by the number of operations they completed. Workers that completed no operations are excluded from `Latency Min` and `Latency Max`. If no workers completed an operation, all latency values are reported as zero.

#### Type: PUT

Runs time/size based benchmark with 100% PUT workload.

> **Note:** At least one of `duration` or `totalsize` must be specified. If both parameters are provided, the benchmark will terminate once either condition is fulfilled."

| Option     | Aliases | Description                                                                                                 | Required | Default Value |
|------------|---------|-------------------------------------------------------------------------------------------------------------|----------|---------------|
| --bucket   | -b      | Bucket (e.g. ais://mybck, s3://mybck, gs://mybck)                                                           | Yes      | N/A           |
| --cleanup  | -c      | Whether bucket (or objects) should be destroyed or not upon benchmark completion                            | No       | False         |
| --totalsize| -s      | Total size to PUT during the benchmark                                                                      | No       | N/A           |
| --minsize  | -min    | Minimum size of objects to be PUT in bucket during the benchmark                                            | Yes      | N/A           |
| --maxsize  | -max    | Maximum size of objects to be PUT in bucket during the benchmark                                            | Yes      | N/A           |
| --duration | -d      | Duration for which benchmark should be run                                                                  | No       | N/A           |
| --workers  | -w      | Number of workers                                                                                           | Yes      | N/A           |

#### Type: GET

Runs a time-based benchmark with 100% GET workload.

> **Note:** `totalsize` represents the desired total size of the bucket prior to initiating the benchmark. If the current size of the bucket is less than `totalsize`, the benchmark will pre-populate the bucket to reach totalsize. This pre-populating process involves the addition of objects whose sizes range between `minsize` and `maxsize`. It's important to note that all three parameters must be provided together. If one or two of these parameters are missing, none should be provided. These parameters are interdependent and the benchmark requires the specification of all or none of them. If `totalsize`, `minsize`, and `maxsize` are not provided, the benchmark will run on the existing contents of the bucket as is, without any prior adjustment or pre-population.

> **Note:** If the benchmark creates a bucket, or if the provided bucket is empty, it will start by creating a single object within the bucket. If you'd like a more specific load, please use `totalsize`, `minsize`, and `maxsize`, or use a bucket that is not empty.

| Option     | Aliases | Description                                                                                                 | Required | Default Value |
|------------|---------|-------------------------------------------------------------------------------------------------------------|----------|---------------|
| --bucket   | -b      | Bucket (e.g. ais://mybck, s3://mybck, gs://mybck)                                                           | Yes      | N/A           |
| --cleanup  | -c      | Whether bucket (or objects) should be destroyed or not upon benchmark completion                            | No       | False         |
| --totalsize| -s      | Total size bucket should be filled to prior to start                                                        | No       | N/A           |
| --minsize  | -min    | Minimum size of objects to be PUT in bucket (if bucket is smaller than total size)                          | No       | N/A           |
| --maxsize  | -max    | Maximum size of objects to be PUT in bucket (if bucket is smaller than total size)                          | No       | N/A           |
| --duration | -d      | Duration for which benchmark should be run                                                                  | Yes      | N/A           |
| --workers  | -w      | Number of workers                                                                                           | Yes      | N/A           |
| --etl      | -e      | Whether objects from aisloader GETs should undergoes the specified ETL transformation                       | No       | N/A           |

#### Type: MIXED

Runs a time-based benchmark with a mixed load of GETs and PUTs (based on `putpct`).

> **Note:** If `totalsize` is provided and the bucket is smaller than that size, the benchmark pre-populates it before timed operations begin. Pre-populated object sizes range from `minsize` to `maxsize`. Without `totalsize`, an empty bucket is seeded with one initial object so that GET operations can run.

| Option     | Aliases | Description                                                                                                 | Required | Default Value |
|------------|---------|-------------------------------------------------------------------------------------------------------------|----------|---------------|
| --bucket   | -b      | Bucket (e.g. ais://mybck, s3://mybck, gs://mybck)                                                           | Yes      | N/A           |
| --cleanup  | -c      | Whether bucket (or objects) should be destroyed or not upon benchmark completion                            | No       | False         |
| --minsize  | -min    | Minimum size of objects to be PUT in bucket during the benchmark                                            | Yes      | N/A           |
| --maxsize  | -max    | Maximum size of objects to be PUT in bucket during the benchmark                                            | Yes      | N/A           |
| --totalsize| -s      | Total size to which the bucket should be filled before the benchmark                                        | No       | N/A           |
| --putpct   | -p      | Percentage for PUT operations in MIXED benchmark                                                            | Yes      | N/A           |
| --duration | -d      | Duration for which benchmark should be run                                                                  | Yes      | N/A           |
| --workers  | -w      | Number of workers                                                                                           | Yes      | N/A           |
| --etl      | -e      | Whether objects from aisloader GETs should undergoes the specified ETL transformation                       | No       | N/A           |

#### Type: LIST

Runs a benchmark to LIST objects in the bucket.

> **Note:** If you provide an `objects` value, the benchmark will pre-populate the bucket until it contains the specified number of objects. If the `objects` value is not given, the benchmark will simply run on the current state of the bucket, without adding any additional items.

| Option         | Aliases | Description                                                                           | Required | Default Value |
|----------------|---------|---------------------------------------------------------------------------------------|----------|---------------|
| --bucket       | -b      | Bucket (e.g. ais://mybck, s3://mybck, gs://mybck)                                     | Yes      | N/A           |
| --cleanup      | -c      | Whether bucket (or objects) should be destroyed or not upon benchmark completion      | No       | False         |
| --objects      | -o      | Number of objects bucket should contain prior to benchmark start                      | No       | N/A           |
| --workers      | -w      | Number of workers (only for pre-population of bucket)                                 | Yes      | N/A           |

#### Type: AISDataset

Runs a time-based benchmark to randomly get objects in the bucket through AISDataset.

> **Note:** If the selected bucket is empty, the benchmark creates one initial object so that the dataset can be read. For a larger initial dataset, provide `totalsize`, `minsize`, and `maxsize` together to pre-populate the bucket before the benchmark starts.

| Option     | Aliases | Description                                                                                                 | Required | Default Value |
|------------|---------|-------------------------------------------------------------------------------------------------------------|----------|---------------|
| --bucket   | -b      | Bucket (e.g. ais://mybck, s3://mybck, gs://mybck)                                                           | Yes      | N/A           |
| --cleanup  | -c      | Whether the bucket or benchmark-created objects should be destroyed upon completion                         | No       | False         |
| --totalsize| -s      | Total size to which the bucket should be filled before the benchmark                                        | No       | N/A           |
| --minsize  | -min    | Minimum generated object size when pre-populating                                                           | No       | N/A           |
| --maxsize  | -max    | Maximum generated object size when pre-populating                                                           | No       | N/A           |
| --duration | -d      | Duration for which benchmark should be run                                                                  | Yes      | N/A           |
| --workers  | -w      | Number of workers                                                                                           | Yes      | N/A           |

#### Type: AISIterDataset

Runs a time-based benchmark to sequentially iterate over objects in the bucket through AISIterDataset.

> **Note:** The benchmark runs until `duration` is reached. If `iterations` is provided, it stops when either the duration or iteration limit is reached. If `iterations` is omitted, iteration continues until the duration is reached.
>
> **Note:** If the selected bucket is empty, the benchmark creates one initial object so that `AISIterDataset` can yield samples in `get_benchmark()`. For a larger initial dataset, provide `totalsize`, `minsize`, and `maxsize` together to pre-populate the bucket before the benchmark starts.

| Option     | Aliases | Description                                                                                                 | Required | Default Value |
|------------|---------|-------------------------------------------------------------------------------------------------------------|----------|---------------|
| --bucket   | -b      | Bucket (e.g. ais://mybck, s3://mybck, gs://mybck)                                                           | Yes      | N/A           |
| --cleanup  | -c      | Whether the bucket or benchmark-created objects should be destroyed upon completion                         | No       | False         |
| --totalsize| -s      | Total size to which the bucket should be filled before the benchmark                                        | No       | N/A           |
| --minsize  | -min    | Minimum generated object size when pre-populating                                                           | No       | N/A           |
| --maxsize  | -max    | Maximum generated object size when pre-populating                                                           | No       | N/A           |
| --duration | -d      | Duration for which benchmark should be run                                                                  | Yes      | N/A           |
| --iterations| -i     | Maximum number of iterations over the dataset                                                               | No       | Unlimited     |
| --workers  | -w      | Number of workers                                                                                           | Yes      | N/A           |

### Examples

There are a few sample benchmarks in the provided Makefile. Run `make help` for more information on the sample benchmarks.

This section provides a rundown of the sample benchmarks defined in the Makefile. You can use `make <target>` to run these benchmarks, where `<target>` is replaced by the desired benchmark. Use `make help` to display the list of available targets.

> **Note:** All benchmark Makefile targets use a configurable `BUCKET` variable (defaults to `ais://pyaisloader`).

1. `make install`
This command installs the current project and the dependencies declared in `pyproject.toml`.

2. `short_put`
This command runs a short `PUT` benchmark on the bucket. The benchmark will stop either when the specified `duration` has elapsed or when the total size of data `PUT` into the bucket reaches `totalsize`.

3. `short_get`
This command runs a short `GET` benchmark on the bucket. If the total size of contents of the bucket are smaller than the specified `totalsize`, the bucket will be pre-populated up to `totalsize`, with the size of individual objects ranging from `minsize` to `maxsize`. The benchmark will terminate when `duration` amount of time has passed.

4. `short_mixed`
This command runs a short `MIXED` benchmark on the bucket. The parameter `putpct` determines the ratio of `PUT` operations to `GET` operations (e.g. a `putpct` of `50` approximately implies that 50% of the operations will be `PUT` operations, and the remaining 50% will be `GET` operations). The benchmark will terminate when `duration` amount of time has passed.

5. `short_list`
This command runs a short `LIST` benchmark on the bucket. If there are less than `objects` amount of objects in the bucket, the bucket will be pre-populated to contain `objects` number of objects.

6. `short_ais_dataset`
This command runs a short benchmark to randomly get objects in the bucket through AISDataset, an AIS Plugin for PyTorch map-style dataset. If the total size of contents of the bucket are smaller than the specified `totalsize`, the bucket will be pre-populated up to `totalsize`, with the size of individual objects ranging from `minsize` to `maxsize`. The benchmark will terminate when `duration` amount of time has passed.

7. `short_ais_iter_dataset`
This command runs a short benchmark to sequentially iterate over objects in the bucket through AISIterDataset, an AIS Plugin for PyTorch iteratable-style dataset. If the total size of contents of the bucket are smaller than the specified `totalsize`, the bucket will be pre-populated up to `totalsize`, with the size of individual objects ranging from `minsize` to `maxsize`. The benchmark will terminate when the specified duration is reached or when the defined number of iterations is completed.

8. `long_put`
This command runs a long `PUT` benchmark on the bucket. The benchmark will stop when the specified `duration` of 30 minutes has elapsed or when the total size of data `PUT` into the bucket reaches `totalsize` of 10GB. The size of individual objects ranges from `minsize` of 50MB to `maxsize` of 100MB, and the number of `worker` threads used is increased to 32 compared to the short `PUT` benchmark.

9. `long_get`
This command runs a long `GET` benchmark on the bucket. The primary differences are that this benchmark runs for a longer `duration` (30 minutes as opposed to 30 seconds) and uses more `worker` threads (32 instead of 16).

10. `long_mixed`
This command runs a long `MIXED` benchmark on the bucket. The `putpct` parameter still determines the ratio of `PUT` operations to `GET` operations. The differences here are the longer `duration` of 30 minutes and and the increased number of `worker` threads (32 instead of 16).

11. `long_list`
This command runs a long `LIST` benchmark on the bucket. If there are fewer than `objects` amount of objects in the bucket, the bucket will be pre-populated to contain `objects` number of objects. The `long_list` benchmark differs from `short_list` in the number of `objects` (500,000 instead of 50,000) and the number of `worker` threads used (32 instead of 16).

12. `help`
This command displays a list of available targets in the Makefile along with their descriptions, providing a helpful guide for understanding and using the available commands.
