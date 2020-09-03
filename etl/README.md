# ETL package

The `etl` package compiles into `aisnode` executable to facilitate running custom ETL containers and communicating with those containers at runtime.

Generally, AIStore v3.2 and later supports on the fly and offline user-defined dataset transformations, which allows moving I/O intensive (and expensive) operations from the computing client(s) into the storage cluster.
Popular use cases include - but are not limited to - *dataset augmentation* (of any kind) and filtering of AI datasets.

For prerequisites, 3 (three) supported ais <=> container communication mechanisms, and further details, please refer to [ETL readme](/docs/etl.md).


## On the fly ETL example

<img src="/docs/images/etl-md5.gif" alt="ETL-MD5" width="900">

The example above uses [AIS CLI](/cmd/cli/README.md) to:
1. **Create** a new AIS bucket

2. **PUT** an object into this bucket

3. **Init** ETL container that performs simple MD5 computation.

   > Both the container itself and its [YAML specification]((https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml) below are included primarily for illustration purposes.

   * [MD5 ETL YAML](https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml)

4. **Transform** the object on the fly via custom ETL - the "transformation" in this case boils down to computing the object's MD5.

5. **Compare** the output with locally computed MD5.

## Management and Benchmarking
- [AIS CLI](/cmd/cli/resources/etl.md) includes commands to start, stop, and monitor ETL at runtime.
- [AIS Loader](/bench/aisloader/README.md) has been extended to benchmark and stress test AIS clusters by running a number of pre-defined transformations that we include with the source code.

For more information and details, please refer to [ETL readme](/docs/etl.md).
