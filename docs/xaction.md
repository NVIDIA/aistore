## Table of Contents
- [Extended Actions (xactions)](#extended-actions-xactions)

## Extended Actions (xactions)

Extended actions (xactions) are batch operations that may take seconds, sometimes minutes or even hours, to execute. Xactions run asynchronously, have one of the enumerated kinds, start/stop times, and xaction-specific statistics. Xactions start running based on a wide variety of runtime conditions that include:

* periodic (defined by a configured interval of time)
* resource utilization (e.g., usable capacity falling below configured watermark)
* certain type of workload (e.g., PUT into a mirrored or erasure-coded bucket)
* user request (e.g., to reduce a number of local object copies in a given bucket)
* adding or removing storage targets (the events that trigger cluster-wide rebalancing)
* adding or removing local disks (the events that cause local rebalancer to start moving stored content between *mountpaths* - see [Managing filesystems](/docs/configuration.md#managing-filesystems))
* and more.

Further, to reduce congestion and minimize interference with user-generated workload, extended actions (self-)throttle themselves based on configurable watermarks. The latter include `disk_util_low_wm` and `disk_util_high_wm` (see [configuration](/ais/setup/config.sh)). Roughly speaking, the idea is that when local disk utilization falls below the low watermark (`disk_util_low_wm`) extended actions that utilize local storage can run at full throttle. And vice versa.

The amount of throttling that a given xaction imposes on itself is always defined by a combination of dynamic factors. To give concrete examples, an extended action that runs LRU evictions performs its "balancing act" by taking into account remaining storage capacity _and_ the current utilization of the local filesystems. The two-way mirroring (xaction) takes into account congestion on its communication channel that callers use for posting requests to create local replicas. And the `atimer` - extended action responsible for [access time updates](/atime/atime.go) - self-throttles based on the remaining space (to buffer atimes), etc.

Supported extended actions are enumerated in the [user-facing API](/cmn/api.go) and include:

* Cluster-wide rebalancing (denoted as `ActGlobalReb` in the [API](/cmn/api.go)) that gets triggered when storage targets join or leave the cluster;
* LRU-based cache eviction (see [LRU](/docs/storage_svcs.md#lru)) that depends on the remaining free capacity and [configuration](/ais/setup/config.sh);
* Prefetching batches of objects (or arbitrary size) from the Cloud (see [List/Range Operations](/docs/batch.md));
* Consensus voting (when conducting new leader [election](/docs/ha.md#election));
* Erasure-encoding objects in a EC-configured bucket (see [Erasure coding](/docs/storage_svcs.md#erasure-coding));
* Creating additional local replicas, and
* Reducing number of object replicas in a given locally-mirrored bucket (see [Storage Services](/docs/storage_svcs.md));
* and more.

There are different actions which may be taken upon xaction. Actions include stats, start and stop.
List of supported actions can be found in the [API](/cmn/api.go)

Xaction requests are generic for all xactions, but responses from each xaction are different. See [below](#start-&-stop).
The request looks as follows:  
1.Single target request:
```shell
curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "actiontype", "name": "xactionname", "value":{"bucket":"bucketname"}}' 'http://T/v1/daemon?what=xaction'
```
To simplify the logic, result is always an array, even if there's only one element in the result

2.Proxy request, which executes a request on all targets within the cluster, and responds with list of targets' responses:
```shell
curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "actiontype", "name": "xactionname", "value":{"bucket":"bucketname"}}' 'http://G/v1/cluster?what=xaction'
```
Response of a query to proxy is a map of daemonID -> target's response. If any of targets responded with error status code, the proxy's response
will result in the same error response.


### Start & Stop
For a successful request, the response only contains the HTTP status code. If the request was sent to the proxy and all targets
responded with a successful HTTP code, the proxy would respond with the successful HTTP code. Response body should be omitted.

For an unsuccessful request, the target's response contains the error code and error message. If the request was sent to proxy and at least one
of targets responded with an error code, the proxy will respond with the same error code and error message.

>> As always, `G` above (and throughout this entire README) serves as a placeholder for the _real_ gateway's hostname/IP address and `T` serves for placeholder for target's hostname/IP address. More information in [notation section](/docs/http_api.md#notation).

The corresponding [RESTful API](/docs/http_api.md) includes support for querying all xactions including global-rebalancing and prefetch operations.

### Stats

Stats request results in list of requested xactions. Statistics of each xaction share a common base format which looks as follow:

```json
[  
   {  
      "id":1,
      "kind":"ecget",
      "bucket":"test",
      "startTime":"2019-04-15T12:40:18.721697505-07:00",
      "endTime":"0001-01-01T00:00:00Z",
      "status":"InProgress"
   },
   {  
      "id":2,
      "kind":"ecput",
      "bucket":"test",
      "startTime":"2019-04-15T12:40:18.721723865-07:00",
      "endTime":"0001-01-01T00:00:00Z",
      "status":"InProgress"
   }
]
```
Any xaction can have additional fields, which are included in additional field called `"ext"`

Example rebalance stats response:
```json
[
    {
      "id": 3,
      "kind": "rebalance",
      "bucket": "",
      "start_time": "2019-04-15T13:38:51.556388821-07:00",
      "end_time": "0001-01-01T00:00:00Z",
      "status": "InProgress",
      "count": 0,
      "ext": {
        "num_sent_files": 0,
        "num_sent_bytes": 0,
        "num_recv_files": 0,
        "num_recv_bytes": 0
      }
    }
]
```