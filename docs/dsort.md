# dSort and iShard retirement

AIStore 5.3 removes dSort and `ishard`. The proxy keeps `/v1/sort` for one release so older CLI binaries can still list jobs.

dSort v0.1 dates to June 2018 (`18fb94efd`). On January 20, 2026, dSort moved out of default builds behind the `sharding` tag (`2642e1683`). The tag was renamed to `dsort` later that day (`b3778efa5`). On February 7, `distributed_sort` was dropped from the default configuration (`bde49523b`).

[dSort documentation for AIStore 5.2](https://github.com/NVIDIA/aistore/blob/v1.5.2/docs/dsort.md)
