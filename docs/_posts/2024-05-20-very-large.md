---
layout: post
title:  "Very large"
date:   May 20, 2024
author: Alex Aizman
categories: blob large-scale performance aistore
---

The idea of _extremely large_ is constantly shifting, evolving. As time passes by we quickly adopt its new numeric definition and only rarely, with a mild sense of amusement, recall the old one.

Take, for instance, [aistore](https://github.com/NVIDIA/aistore) – an open-source storage cluster that deploys ad-hoc and runs anywhere. Possibly because of its innate scalability (proportional to the number of data drives) the associated “largeness” is often interpreted as total throughput. The throughput of something like this particular installation:

![16 nodes: v3.23 on DenseIO.E5](/assets/very-large/GET-throughput-16-nodes-167GiB.png)

where 16 gateways and 16 [DenseIO.E5](https://docs.oracle.com/en-us/iaas/Content/Compute/References/computeshapes.htm#bm-dense) storage nodes provide a total sustained 167 GiB/s.

But no, not that kind of “large” we’ll be talking about – a different one.

[Read more...](https://storagetarget.com/2024/05/20/very-large)
