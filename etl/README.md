# ETL package

The `etl` package compiles into `aisnode` executable to facilitate running custom ETL containers and communicating with those containers at runtime.

Generally, AIStore v3.2 and later supports on the fly and offline user-defined dataset transformations, which allows moving I/O intensive (and expensive) operations from the computing client(s) into the storage cluster.
Popular use cases include - but are not limited to - *dataset augmentation* (of any kind) and filtering of AI datasets.

For prerequisites, 3 (three) supported ais <=> container communication mechanisms, and usage guides please refer to [ETL readme](/docs/etl.md).
This documentation contains techincal details, architecture overview and is obligatory to be read before using ETL.

## Architecture

The AIStore ETL extension is designed to maximize effectiveness of the transform process.
It minimizes resources waste on unnecessary operations like exchanging data between storage and compute nodes, which takes place in conventional ETL systems.

Based on specification provided by a user, each target starts its own ETL container (worker), which from now on will be responsible for transforming objects stored on the corresponding target.
This approach minimizes I/O operations, as well as assures scalability of ETL with the number of targets in the cluster.

The following picture presents architecture of the ETL extension.

<img src="/docs/images/etl-arch.png" alt="ETL architecture" width="80%">

## Management and Benchmarking
- [AIS CLI](/cmd/cli/resources/etl.md) includes commands to start, stop, and monitor ETL at runtime.
- [AIS Loader](/bench/aisloader/README.md) has been extended to benchmark and stress test AIS clusters by running a number of pre-defined transformations that we include with the source code.

For more information and details, please refer to [ETL readme](/docs/etl.md).
