---
layout: post
title: METRICS-REFERENCE
permalink: /docs/metrics-reference
redirect_from:
 - /metrics-reference.md/
 - /docs/metrics-reference.md/
---

## Common (targets and gateways) metrics

| Internal name | Public name | Internal Type | Description (Prometheus help) | Prometheus labels |
| --- | --- | --- | --- | --- |
| `get.n` | `get_count` | counter | total number of executed GET(object) requests | default |
| `put.n` | `put_count` | counter | total number of executed PUT(object) requests | default |
| `head.n` | `head_count` | counter | total number of executed HEAD(object) requests | default |
| `append.n` | `append_count` | counter | total number of executed APPEND(object) requests | default |
| `del.n` | `del_count` | counter | total number of executed DELETE(object) requests | default |
| `ren.n` | `ren_count` | counter | total number of executed rename(object) requests | default |
| `lst.n` | `lst_count` | counter | total number of executed list-objects requests | default |
| `err.get.n` | `err_get_count` | counter | total number of GET(object) errors | default |
| `err.put.n` | `err_put_count` | counter | total number of PUT(object) errors | default |
| `err.head.n` | `err_head_count` | counter | total number of HEAD(object) errors | default |
| `err.append.n` | `err_append_count` | counter | total number of APPEND(object) errors | default |
| `err.del.n` | `err_del_count` | counter | total number of DELETE(object) errors | default |
| `err.ren.n` | `err_ren_count` | counter | total number of rename(object) errors | default |
| `err.lst.n` | `err_lst_count` | counter | total number of list-objects errors | default |
| `err.http.write.n` | `err_http_write_count` | counter | total number of HTTP write-response errors | default |
| `err.dl.n` | `err_dl_count` | counter | downloader: number of download errors | default |
| `err.put.mirror.n` | `err_put_mirror_count` | counter | number of n-way mirroring errors | default |
| `get.ns` | `get_ms` | latency | GET: average time (milliseconds) over the last periodic.stats_time interval | default |
| `get.ns.total` | `get_ns_total` | total | GET: total cumulative time (nanoseconds) | default |
| `lst.ns` | `lst_ms` | latency | list-objects: average time (milliseconds) over the last periodic.stats_time interval | default |
| `kalive.ns` | `kalive_ms` | latency | in-cluster keep-alive (heartbeat): average time (milliseconds) over the last periodic.stats_time interval | default |
| `up.ns.time` | `uptime` | special | this node's uptime since its startup (seconds) | default |
| `state.flags` | `state_flags` | gauge | bitwise 64-bit value that carries enumerated node-state flags, including warnings and alerts; see https://github.com/NVIDIA/aistore/blob/main/cmn/cos/node_state_info.go |

## Target metrics

| Internal name | Public name | Internal Type | Description (Prometheus help) | Prometheus labels |
| --- | --- | --- | --- | --- |
| `disk.<DISK-NAME>.read.bps` | `disk_read_mbps` | computed-bandwidth | read bandwidth (MB/s) | map[disk:`<DISK-NAME>` node_id:`<AIS-NODE-ID>`] |
| `disk.<DISK-NAME>.avg.rsize` | `disk_avg_rsize` | gauge | average read size (bytes) | map[disk:`<DISK-NAME>` node_id:`<AIS-NODE-ID>`] |
| `disk.<DISK-NAME>.write.bps` | `disk_write_mbps` | computed-bandwidth | write bandwidth (MB/s) | map[disk:`<DISK-NAME>` node_id:`<AIS-NODE-ID>`] |
| `disk.<DISK-NAME>.avg.wsize` | `disk_avg_wsize` | gauge | average write size (bytes) | map[disk:`<DISK-NAME>` node_id:`<AIS-NODE-ID>`] |
| `disk.<DISK-NAME>.util` | `disk_util` | gauge | disk utilization (%%) | map[disk:`<DISK-NAME>` node_id:`<AIS-NODE-ID>`] |
| `lru.evict.n` | `lru_evict_count` | counter | number of LRU evictions | default |
| `lru.evict.size` | `lru_evict_bytes` | size | total cumulative size (bytes) of LRU evictions | default |
| `cleanup.store.n` | `cleanup_store_count` | counter | space cleanup: number of removed misplaced objects and old work files | default |
| `cleanup.store.size` | `cleanup_store_bytes` | size | space cleanup: total size (bytes) of all removed misplaced objects and old work files (not including removed deleted objects) | default |
| `ver.change.n` | `ver_change_count` | counter | number of out-of-band updates (by a 3rd party performing remote PUTs from outside this cluster) | default |
| `ver.change.size` | `ver_change_bytes` | size | total cumulative size (bytes) of objects that were updated out-of-band across all backends combined | defaul t |
| `remote.deleted.del.n` | `remote_deleted_del_count` | counter | number of out-of-band deletes (by a 3rd party remote DELETE(object) from outside this cluster) | default |
| `put.ns` | `put_ms` | latency | PUT: average time (milliseconds) over the last periodic.stats_time interval | default |
| `put.ns.total` | `put_ns_total` | total | PUT: total cumulative time (nanoseconds) | default |
| `append.ns` | `append_ms` | latency | APPEND(object): average time (milliseconds) over the last periodic.stats_time interval | default |
| `get.redir.ns` | `get_redir_ms` | latency | GET: average gateway-to-target HTTP redirect latency (milliseconds) over the last periodic.stats_time interval | default |
| `put.redir.ns` | `put_redir_ms` | latency | PUT: average gateway-to-target HTTP redirect latency (milliseconds) over the last periodic.stats_time interval | default |
| `get.bps` | `get_mbps` | bandwidth | GET: average throughput (MB/s) over the last periodic.stats_time interval | default |
| `put.bps` | `put_mbps` | bandwidth | PUT: average throughput (MB/s) over the last periodic.stats_time interval | default |
| `get.size` | `get_bytes` | size | GET: total cumulative size (bytes) | default |
| `put.size` | `put_bytes` | size | PUT: total cumulative size (bytes) | default |
| `err.cksum.n` | `err_cksum_count` | counter | number of executed GET(object) requests | default |
| `err.cksum.size` | `err_cksum_bytes` | size | number of executed GET(object) requests | default |
| `err.fshc.n` | `err_fshc_count` | counter | number of times filesystem health checker (FSHC) was triggered by an I/O error or errors | default |
| `err.io.get.n` | `err_io_get_count` | counter | GET: number of I/O errors _not_ including remote backend and network errors | default |
| `err.io.put.n` | `err_io_put_count` | counter | PUT: number of I/O errors _not_ including remote backend and network errors | default |
| `err.io.del.n` | `err_io_del_count` | counter | DELETE(object): number of I/O errors _not_ including remote backend and network errors | default |
| `stream.out.n` | `stream_out_count` | counter | intra-cluster streaming communications: number of sent objects | default |
| `stream.out.size` | `stream_out_bytes` | size | intra-cluster streaming communications: total cumulative size (bytes) of all transmitted objects | default |
| `stream.in.n` | `stream_in_count` | counter | intra-cluster streaming communications: number of received objects | default |
| `stream.in.size` | `stream_in_bytes` | size | intra-cluster streaming communications: total cumulative size (bytes) of all received objects | default |
| `dl.size` | `dl_bytes` | size | total downloaded size (bytes) | default |
| `dl.ns` | `dl_ms` | latency | total time it took to execute dowload requests (milliseconds) | default |
| `dsort.creation.req.n` | `dsort_creation_req_count` | counter | dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics | default |
| `dsort.creation.resp.n` | `dsort_creation_resp_count` | counter | dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics | default |
| `dsort.creation.resp.ns` | `dsort_creation_resp_ms` | latency | dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics | default |
| `dsort.extract.shard.dsk.n` | `dsort_extract_shard_dsk_count` | counter | dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics | default |
| `dsort.extract.shard.mem.n` | `dsort_extract_shard_mem_count` | counter | dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics | default |
| `dsort.extract.shard.size` | `dsort_extract_shard_bytes` | size | dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics | default |
| `lcache.collision.n` | `lcache_collision_count` | counter | number of LOM cache collisions (core, internal) | default |
| `lcache.evicted.n` | `lcache_evicted_count` | counter | number of LOM cache evictions (core, internal) | default |
| `lcache.flush.cold.n` | `lcache_flush_cold_count` | counter | number of times a LOM from cache was written to stable storage (core, internal) | default |
| `remais.get.n` | `remote_get_count` | counter | GET: total number of executed remote requests (cold GETs) | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.get.ns.total` | `remote_get_ns_total` | total | GET: total cumulative time (nanoseconds) to execute cold GETs and store new object versions in-cluster | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.e2e.get.ns.total` | `remote_e2e_get_ns_total` | total | GET: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving request, executing cold-GET, storing new object version in-cluster, and transmitting response | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.get.size` | `remote_get_bytes_total` | size | GET: total cumulative size (bytes) of all cold-GET transactions | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.head.n` | `remote_head_count` | counter | HEAD: total number of executed remote requests to a given backend | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.put.n` | `remote_put_count` | counter | PUT: total number of executed remote requests to a given backend | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.put.ns.total` | `remote_put_ns_total` | total | PUT: total cumulative time (nanoseconds) to execute remote requests and store new object versions in-cluster | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.e2e.put.ns.total` | `remote_e2e_put_ns_total` | total | PUT: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving PUT payload, storing it in-cluster, executing remote PUT, finalizing new in-cluster object | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.put.size` | `remote_e2e_put_bytes_total` | size | PUT: total cumulative size (bytes) of all PUTs to a given remote backend | map[backend:remais node_id:ClCt8081] |
| `remais.ver.change.n` | `remote_ver_change_count` | counter | number of out-of-band updates (by a 3rd party performing remote PUTs outside this cluster) | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `remais.ver.change.size` | `remote_ver_change_bytes_total` | size | total cumulative size of objects that were updated out-of-band | map[backend:remais node_id:`<AIS-NODE-ID>`] |
| `gcp.get.n` | `remote_get_count` | counter | GET: total number of executed remote requests (cold GETs) | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.get.ns.total` | `remote_get_ns_total` | total | GET: total cumulative time (nanoseconds) to execute cold GETs and store new object versions in-cluster | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.e2e.get.ns.total` | `remote_e2e_get_ns_total` | total | GET: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving request, executing cold-GET, storing new object version in-cluster, and transmitting response | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.get.size` | `remote_get_bytes_total` | size | GET: total cumulative size (bytes) of all cold-GET transactions | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.head.n` | `remote_head_count` | counter | HEAD: total number of executed remote requests to a given backend | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.put.n` | `remote_put_count` | counter | PUT: total number of executed remote requests to a given backend | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.put.ns.total` | `remote_put_ns_total` | total | PUT: total cumulative time (nanoseconds) to execute remote requests and store new object versions in-cluster | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.e2e.put.ns.total` | `remote_e2e_put_ns_total` | total | PUT: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving PUT payload, storing it in-cluster, executing remote PUT, finalizing new in-cluster object | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.put.size` | `remote_e2e_put_bytes_total` | size | PUT: total cumulative size (bytes) of all PUTs to a given remote backend | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.ver.change.n` | `remote_ver_change_count` | counter | number of out-of-band updates (by a 3rd party performing remote PUTs outside this cluster) | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `gcp.ver.change.size` | `remote_ver_change_bytes_total` | size | total cumulative size of objects that were updated out-of-band | map[backend:gcp node_id:`<AIS-NODE-ID>`] |
| `aws.get.n` | `remote_get_count` | counter | GET: total number of executed remote requests (cold GETs) | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.get.ns.total` | `remote_get_ns_total` | total | GET: total cumulative time (nanoseconds) to execute cold GETs and store new object versions in-cluster | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.e2e.get.ns.total` | `remote_e2e_get_ns_total` | total | GET: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving request , executing cold-GET, storing new object version in-cluster, and transmitting response | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.get.size` | `remote_get_bytes_total` | size | GET: total cumulative size (bytes) of all cold-GET transactions | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.head.n` | `remote_head_count` | counter | HEAD: total number of executed remote requests to a given backend | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.put.n` | `remote_put_count` | counter | PUT: total number of executed remote requests to a given backend | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.put.ns.total` | `remote_put_ns_total` | total | PUT: total cumulative time (nanoseconds) to execute remote requests and store new object versions in-cluster | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.e2e.put.ns.total` | `remote_e2e_put_ns_total` | total | PUT: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving PUT payload, storing it in-cluster, executing remote PUT, finalizing new in-cluster object | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.put.size` | `remote_e2e_put_bytes_total` | size | PUT: total cumulative size (bytes) of all PUTs to a given remote backend | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.ver.change.n` | `remote_ver_change_count` | counter | number of out-of-band updates (by a 3rd party performing remote PUTs outside this cluster) | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `aws.ver.change.size` | `remote_ver_change_bytes_total` | size | total cumulative size of objects that were updated out-of-band | map[backend:aws node_id:`<AIS-NODE-ID>`] |
| `azure.get.n` | `remote_get_count` | counter | GET: total number of executed remote requests (cold GETs) | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.get.ns.total` | `remote_get_ns_total` | total | GET: total cumulative time (nanoseconds) to execute cold GETs and store new object versions in-cluster | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.e2e.get.ns.total` | `remote_e2e_get_ns_total` | total | GET: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving request, executing cold-GET, storing new object version in-cluster, and transmitting response | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.get.size` | `remote_get_bytes_total` | size | GET: total cumulative size (bytes) of all cold-GET transactions | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.head.n` | `remote_head_count` | counter | HEAD: total number of executed remote requests to a given backend | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.put.n` | `remote_put_count` | counter | PUT: total number of executed remote requests to a given backend | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.put.ns.total` | `remote_put_ns_total` | total | PUT: total cumulative time (nanoseconds) to execute remote requests and store new object versions in-cluster | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.e2e.put.ns.total` | `remote_e2e_put_ns_total` | total | PUT: total end-to-end time (nanoseconds) servicing remote requests; includes: receiving PUT payload, storing it in-cluster, executing remote PUT, finalizing new in-cluster object | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.put.size` | `remote_e2e_put_bytes_total` | size | PUT: total cumulative size (bytes) of all PUTs to a given remote backend | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.ver.change.n` | `remote_ver_change_count` | counter | number of out-of-band updates (by a 3rd party performing remote PUTs outside this cluster) | map[backend:azure node_id:`<AIS-NODE-ID>`] |
| `azure.ver.change.size` | `remote_ver_change_bytes_total` | size | total cumulative size of objects that were updated out-of-band | map[backend:azure node_id:`<AIS-NODE-ID>`] |
