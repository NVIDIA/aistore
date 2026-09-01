---
layout: post
title: "NVIDIA AIStore at MLPerf Storage v3.0: Linear Scale-Out and Multi-Cloud Portability"
date: September 1, 2026
author: Abhishek Gaikwad and Pradeep Madhavarapu
categories: aistore mlperf storage performance cloud
---

Scale linearly. Deploy anywhere. Those have been AIStore's design goals from the start, and NVIDIA's [MLPerf Storage v3.0 submission](https://mlcommons.org/2026/09/mlperf-storage-v3-0-results/) put both through a common, peer-reviewed benchmark.

Increasing the AIStore cluster from 3 to 12 nodes (4x) delivered 3.97x the UNet3D training I/O and 3.99x the Llama 3 1T checkpoint recovery throughput, while per-node training throughput remained effectively constant. The same S3-compatible data path ran on Amazon Web Services (AWS), Google Cloud, and Oracle Cloud Infrastructure (OCI), using publicly available instances, local NVMe drives, and no proprietary storage hardware.

We have measured AIStore's scale-out behavior for years with [aisloader](https://aistore.nvidia.com/docs/aisloader), our own load-generation tool. Those benchmarks were valuable engineering measurements, but MLPerf Storage gave us something different: a standardized workload, peer review, and testing through the S3-compatible API most applications already use.

[NVIDIA AIStore](https://github.com/NVIDIA/aistore) is open-source software that NVIDIA deploys as a cache close to GPU compute in a public cloud or on-premises. In production, it sits in front of existing object storage, bringing frequently accessed data closer to compute for faster access while the backend remains the system of record.

## What MLPerf Storage v3.0 measures

[MLPerf Storage](https://mlcommons.org/benchmarks/storage/) measures whether a storage system can meet the I/O demands of machine learning workloads. The benchmark runs the data-loading path with synthetic datasets that reproduce each workload's data sizes and access patterns. Model computation is replaced by calibrated compute time, so the test can emulate accelerators without requiring the corresponding GPUs.

NVIDIA published 20 Closed Division results across training and checkpointing workloads. For the submitted configurations, AIStore ran on public-cloud instances and served data from native AIStore buckets through its [S3-compatible API](https://docs.nvidia.com/aistore/s3compat). Cloud object storage was not part of the measured data path.

For the training workloads, Accelerator Utilization (AU) estimates the share of benchmark time that the simulated accelerators spend computing instead of waiting for data. Higher AU means storage keeps the simulated accelerators well fed.

We submitted two training workloads and one checkpointing workload:

| Workload | What it is | Average object size | Required AU |
|:---------|:-----------|:--------------------|:---------|
| UNet3D | 3D medical image segmentation | About 140 MiB per sample | 90% |
| RetinaNet | Object detection | About 315 KiB per object | 85% |
| Checkpointing | Llama 3 save and recovery read at 8B, 70B, 405B, and 1T parameters | Varies by model size | Not applicable |

The checkpoint test measures a different part of training. It writes ten model-state checkpoints to the storage system under test, clears client caches when required, then reads the checkpoints back to emulate recovery after an interruption. The reported metrics are aggregate write and recovery-read bandwidth rather than AU.

Our submission used [NVIDIA AIStore v4.8](https://github.com/NVIDIA/aistore/releases/tag/v1.4.8) on Kubernetes. In MLCommons terminology, the submission is classified as Available, meaning all system components are publicly available, and as Shared Remote Object storage: multiple clients access the storage over a network through an object API. It was submitted to the Closed Division, where benchmark workload options are fixed for comparable testing.

This post focuses on the configurations that show scale-out behavior and cloud portability most clearly.

## Scaling from 3 to 12 storage nodes

For the OCI scale-out series, we used [BM.DenseIO.E5.128](https://docs.oracle.com/en-us/iaas/Content/Compute/References/computeshapes.htm) storage nodes. Each node contributed twelve local NVMe drives and a single 100 Gb/s network interface. We tested three, six, and twelve AIStore nodes. The training runs scaled from 5 to 10 to 20 clients and simulated B200 accelerators. The 1T-parameter model checkpoint runs used 8, 8, and 16 clients, respectively, with two data-parallel instances at every scale.

![AIStore scale-out results for MLPerf Storage v3.0](/assets/mlperf-storage-v3/scale-out.jpg)

*At four times the node count, AIStore delivered 3.97x the UNet3D training I/O and 3.99x the checkpoint recovery throughput.*

| AIStore nodes | Training clients / simulated B200 GPUs | 1T checkpoint clients | UNet3D I/O | Mean AU | Llama 3 1T write | Llama 3 1T recovery read |
|--------------:|------------------------------------:|----------------------:|-----------:|--------:|------------------:|-------------------------:|
| 3 | 5 / 5 | 8 | 29.15 GiB/s | 98.86% | 33.62 GiB/s | 34.20 GiB/s |
| 6 | 10 / 10 | 8 | 58.22 GiB/s | 98.75% | 64.50 GiB/s | 70.01 GiB/s |
| 12 | 20 / 20 | 16 | 115.58 GiB/s | 98.02% | 133.27 GiB/s | 136.54 GiB/s |

UNet3D throughput remained nearly constant per storage node: 9.72 GiB/s at three nodes, 9.70 GiB/s at six, and 9.63 GiB/s at twelve. That stability produced 3.97x the aggregate training I/O at 4x the node count, while mean AU remained above 98%. The simulated accelerators received data fast enough to spend very little benchmark time waiting on storage.

Checkpoint recovery followed the same scale-out curve. The twelve-node result reached 3.99x the three-node read rate. Each storage node had a nominal 100 Gb/s network interface, equivalent to 11.64 GiB/s before protocol overhead. Across twelve nodes, that provided 139.7 GiB/s of nominal aggregate NIC bandwidth. The benchmark delivered 136.54 GiB/s of recovery-read throughput and 133.27 GiB/s of write throughput, equivalent to 97.7% and 95.4% of that nominal figure, respectively.

RetinaNet adds useful context. At about 315 KiB per object, RetinaNet has roughly 450 times as many objects per GiB as UNet3D, whose average sample is 140 MiB. With less data in each object, fixed per-request work such as network round trips, S3 request parsing, object lookup, and scheduling consumes a larger share of retrieval time, making data movement less efficient for smaller objects. From three to six OCI nodes, RetinaNet I/O increased from 27.09 to 50.04 GiB/s, a 1.85x gain. Mean AU was 94.06% at three nodes and 89.25% at six nodes, both above the workload's 85% requirement. Large-object bandwidth tends to track the hardware more closely, while smaller objects put more pressure on request processing.

## The same software on three clouds

We also ran the UNet3D workload on separate three-node AIStore deployments in AWS, Google Cloud, and OCI. Each deployment used local NVMe storage, Kubernetes, and the same S3 data path.

| Cloud | AIStore storage shape | Clients / simulated B200s | UNet3D I/O | I/O per simulated B200 | Mean AU |
|:------|:----------------------|--------------------------:|-----------:|----------------------:|--------:|
| AWS | 3 x i8ge.24xlarge | 4 / 8 | 46.41 GiB/s | 5.80 GiB/s | 98.38% |
| Google Cloud | 3 x z3-highmem-176-standardlssd | 8 / 8 | 46.15 GiB/s | 5.77 GiB/s | 97.88% |
| OCI | 3 x BM.DenseIO.E5.128 | 5 / 5 | 29.15 GiB/s | 5.83 GiB/s | 98.86% |

These are portability results, not a comparison of cloud providers. The instance shapes, network limits, client counts, datasets, and tuning differ. The aggregate results reflect different simulated accelerator counts, while I/O per simulated B200 stayed in a narrow range of 5.77 to 5.83 GiB/s. What the results establish is that AIStore ran the same benchmark workload through S3 on all three clouds and kept the simulated accelerators fed in each environment.

## Where AIStore fits when used as a cache

The submission exercised AIStore's S3-compatible API against native AIStore buckets. In production, the same API can expose cloud-backed buckets, allowing AIStore to sit between training jobs and existing object storage without changing the system of record.

For a complete view of AIStore's frontend APIs, cluster components, and supported backends, see the [AIStore architecture overview](https://docs.nvidia.com/aistore/overview#at-a-glance).

GPU capacity does not always come from the same region for every job. A team may train in one region today and another for its next allocation while the source dataset stays in the original bucket. Repeated cross-region reads can add latency and data-transfer charges. An AIStore cluster in the same region as the GPUs can cache that data locally, avoiding repeated cross-region reads.

On a cold read, AIStore retrieves the object from Amazon S3, Google Cloud Storage, Azure Blob Storage, OCI Object Storage, or a remote AIStore cluster and stores a protected local copy. Later reads are served locally, and datasets can be prefetched before a job starts. This improves throughput and reduces repeated-access latency while the backend remains the system of record. We use this pattern inside NVIDIA for selected production workloads.

The [KubeCon + CloudNativeCon talk](https://www.youtube.com/watch?v=N-d9cbROndg) covers the architecture and earlier measurements in more detail.

## What the results tell us

MLPerf results describe specific systems, software versions, and test conditions. They are not a promise that every deployment will produce the same numbers. They do give us three useful measurements from a common benchmark:

* UNet3D training I/O tracked the number of OCI storage nodes, reaching 3.97x throughput at 4x the node count.
* Llama 3 1T checkpoint recovery reached 3.99x throughput and 97.7% of nominal aggregate storage network bandwidth on twelve nodes.
* Three separate cloud deployments sustained more than 97% mean AU through the S3 object API.

---

_AIStore is open-source, MIT-licensed software, not an NVIDIA-managed storage service, and any vendor or cloud provider is free to adopt or productize it. Source code and documentation are available in the [AIStore repository](https://github.com/NVIDIA/aistore), with supported cloud backends documented in the [provider guide](https://docs.nvidia.com/aistore/providers). Kubernetes deployment guides and recipes, including the operator, Helm charts, and Ansible playbooks, are available in the [ais-k8s repository](https://github.com/NVIDIA/ais-k8s)._

_MLPerf Storage v3.0, Closed Division. The MLPerf name and logo are trademarks of MLCommons Association in the United States and other countries._
