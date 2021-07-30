---
layout: post
title: ETL
permalink: etl
redirect_from:
 - etl/README.md/
---

# ETL package

The `etl` package compiles into `aisnode` executable to facilitate running custom ETL containers and communicating with those containers at runtime.

AIStore supports both on the fly (aka *inline*) and offline user-defined dataset transformations. All the respective I/O intensive (and expensive) operation is confined to the storage cluster, with computing clients retaining all their resources to execute computation over transformed, filtered, and sorted data.

Popular use cases include - but are not limited to - *dataset augmentation* (of any kind) and filtering of AI datasets.

Please refer to [ETL readme](/docs/etl.md) for the prerequisites, 3 (three) supported ais <=> container communication mechanisms, and usage examples.

> [ETL readme](/docs/etl.md) also contains an overview of the architecture, important technical details, and further guidance.

## Architecture

AIS-ETL extension is designed to maximize the effectiveness of the transformation process. In particular, AIS-ETL optimizes-out the entire networking operation that would otherwise be required to move pre-transformed data between storage and compute nodes.

Based on the specification provided by a user, each target starts its own ETL container (worker) - one ETL container per each storage target in the cluster. From now this "local" ETL container will be responsible for transforming objects stored on "its" AIS target. This approach allows us to run custom transformations **close to data**. This approach also ensures performance and scalability of the transformation workloads - the scalability that for all intents and purposes must be considered practically unlimited.

The following figure illustrates a cluster of 3 AIS proxies (gateways) and 4 storage targets, with each target running user-defined ETL in parallel:

<img src="/aistore/docs/images/etl-arch.png" alt="ETL architecture" width="80%">

## Management and Benchmarking
- [AIS CLI](/cmd/cli/resources/etl.md) includes commands to start, stop, and monitor ETL at runtime.
- [AIS Loader](/bench/aisloader/README.md) has been extended to benchmark and stress test AIS clusters by running a number of pre-defined transformations that we include with the source code.

For more information and details, please refer to [ETL readme](/docs/etl.md).
