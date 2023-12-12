---
layout: post
title:  "What's new in AIS v3.9"
date:   Mar 15, 2022
author: Alex Aizman
categories: aistore
---

AIS **v3.9** is substantial [productization and performance-improving release](https://github.com/NVIDIA/aistore/releases/tag/3.9). Much of the codebase has been refactored for consistency, with micro-optimization and stabilization fixes across the board.

## Highlights

* [promote](/docs/overview.md#promote-local-or-shared-files): redefine to handle remote file shares; collaborate when promoting via entire cluster; add usability options; productize;
* [xmeta](https://github.com/NVIDIA/aistore/blob/main/cmd/xmeta/README.md): extend to also dump in a human-readable format: a) erasure-coded metadata and b) object metadata;
* memory usage and fragmentation: consistently use mem-pooling (via `sync.Pool`) for all control structures in the datapath;
* optimistic concurrency when running batch `prefetch` jobs; refactor and productize;
* optimize PUT datapath;
* core logic to deconflict running concurrent `xactions` (asynchronous jobs): bucket rename vs bucket copy, put a node into maintenance mode vs offline ETL, and similar;
* extend and reinforce resilvering logic to withstand simultaneous disk losses/attachments - at runtime and with no downtime;
* stabilize global rebalance to successfully pass multiple hours of random node "kills" and restarts - *node-left* and *node-joined* events - in presence of stressful data traffic;
* self-healing: object metadata cache to support recovery upon `mountpath` events (e.g., drive failures);
* error handling: phase out generic `fmt.Errorf` and consistently use assorted error types instead;
* additional options to speedup listing of very large buckets ([list-objects](/docs/bucket.md#list-objects));
* numerous micro-optimizing improvements: fast datapath query (`DPQ`) and many more.
