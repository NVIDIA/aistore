---
layout: post
title:  "Google Colab + AIStore: Easier Cloud Data Access for AI/ML Experiments"
date: September 18, 2024
author: Abhishek Gaikwad
categories: aistore google-colab data-loading
---

Working with data stored in cloud services like GCP, AWS, and Azure in [Google Colab](https://colab.research.google.com/) can be challenging. The entire process—from installing libraries and configuring the backend to pulling data and dealing with performance and usability issues—is often frustrating. We often end up downloading the same data repeatedly instead of caching it locally.

In this blog, we'll show you how to use AIStore on Google Colab for quick experiments and easy data loading. AIStore stores your data on Colab's local disk space, so after the initial download from the cloud, your data is cached locally on the instance. This means subsequent data access is much faster, as it loads directly from the local disk instead of the cloud. With Google Colab's free tier offering around **60 GB** of disk space, you can cache a significant amount of data without any additional cost.

![google colab](/assets/google_colab_aistore/aistore-google-colab.png)

The process is simple. Just click the button below to load the notebook and follow the steps to get started.

<a href="https://colab.research.google.com/github/NVIDIA/aistore/blob/main/python/examples/google_colab/aistore_deployment.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

**Important Notes:**
- This example installs [Go v1.23.1](https://go.dev/doc/install), which is the supported Go version and toolchain at the time of writing.
- AIStore runs in the background during the experiments. However, if you interrupt any cell, it sends a `SIGINT` (termination signal) to all background processes, including AIStore. If this happens, simply rerun the appropriate cell to restart AIStore and continue your work.

## Stats for Nerds

Before diving into performance evaluation, let’s take a look at the machine specs. All the experiments were conducted using Google Colab’s **free-tier** resources.

> Colab offers free resources with dynamic usage limits, which can fluctuate based on availability. While these limits aren't published, they may vary over time. [Learn more](https://research.google.com/colaboratory/faq.html#usage-limits).

At the time of testing, here were the specs for the default CPU setup:

- **CPU**: Intel Xeon with 2 virtual CPUs (vCPUs)
- **RAM**: 13 GB (upgradable)
- **Disk Space**: ~110 GB total, with ~60 GB free
- **Network**: 10 Gbps NIC

### Stress Testing the Cluster

To benchmark a brand-new freshly deployed AIStore cluster, we used [`aisloader`](https://github.com/NVIDIA/aistore/blob/main/docs/aisloader.md).

> [`aisloader`](https://github.com/NVIDIA/aistore/blob/main/docs/aisloader.md) is a home-grown general-purpose tool to measure storage performance. It is a load generator capable of producing variety of synthetic AI workloads that we constantly use to benchmark and stress-test AIStore with and without Cloud backends. In addition, `aisloader` can read and write S3 buckets _directly_, which makes it quite convenient when comparing storage performance **with** aistore in front of S3 and **without**.

The tests were run on a 30 TiB S3 bucket, with an average object size of 10 MiB. We conducted the experiments using 20 worker threads. During the **COLD** run, where objects were not cached locally, we achieved a average GET throughput of **70 MiB/s**. For subsequent **WARM** reads, where objects were fetched from local disk, the average throughput increased to **250 MiB/s**.

![throughput comparison](/assets/google_colab_aistore/throughput.png)

### Resource Limitations

Since the Colab free-tier provides only 2 vCPUs, increasing the number of worker threads or targets led to noticeable slowdowns during the experiments. These limitations restrict the ability to fully scale out tasks, but even with these constraints, AIStore demonstrated impressive throughput improvements once data was cached locally.

## Conclusion

Setting up AIStore from scratch on Google Colab takes about 5 minutes. Once deployed, you can conveniently use [Python SDK](https://pypi.org/project/aistore/) to run workloads, and [CLI](https://aistore.nvidia.com/docs/cli) to see what's going on, memory, capacity, and performance-wise. Perhaps the biggest advantage is that after the first training epoch—or more generally, after any remote-read operation—the data is stored locally. Which ultimately means - **performance**. Time saved, and maybe money saved as well.

Give it a try by running the notebook, and see how it can enhance your workflow! 🚀
