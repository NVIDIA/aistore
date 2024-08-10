This is the top eXtended Action (`xaction`) directory containing much of the common functionality and interfaces used by the rest of the code.

In addition, it contains subdirectories:

* `xreg` - xaction registry
* `xs` - concrete named xactions, e.g. `apc.ActRebalance`, `apc.ActPromote`, `apc.ActSummaryBck` and other enumerated *kinds*.

> For all supported xactions, their *kinds* and static properties, see `xact.Table`.

> Xaction *kinds* are generally consistent with the API constants from `api/apc/const.go`.

## Extended Actions (xactions)

Batch operations that may take many seconds (minutes, hours) to execute are called eXtended actions or *xactions*.

Xactions run asynchronously, have one of the enumerated kinds, start/stop times, and xaction-specific statistics.
Xactions start running based on a wide variety of runtime conditions that include:

* periodic (defined by a configured interval of time)
* resource utilization (e.g., usable capacity falling below configured watermark)
* certain type of workload (e.g., PUT into a mirrored or erasure-coded bucket)
* user request (e.g., to reduce the number of local object copies in a given bucket)
* adding or removing storage targets (the events that trigger cluster-wide rebalancing)
* adding or removing local disks (the events that cause resilver to start moving stored content between *mountpaths* - see [Managing mountpaths](/docs/configuration.md#managing-mountpaths))

Further, to reduce congestion and minimize interference with user-generated workload, extended actions (self-)throttle themselves based on configurable watermarks. The latter include `disk_util_low_wm` and `disk_util_high_wm` (see [configuration](/deploy/dev/local/aisnode_config.sh)). Roughly speaking, the idea is that when local disk utilization falls below the low watermark (`disk_util_low_wm`) extended actions that utilize local storage can run at full throttle. And vice versa.

The amount of throttling that a given xaction imposes on itself is always defined by a combination of dynamic factors.
To give concrete examples, an extended action that runs LRU evictions performs its "balancing act" by taking into account the remaining storage capacity **and** the current utilization of the local filesystems.
The mirroring (xaction) takes into account congestion on its communication channel that callers use for posting requests to create local replicas.

---------------------------------------------------------------

**NOTE (Dec 2021):** rest of this document is somewhat **outdated** and must be revisited. For the most recently updated information on running and monitoring *xactions*, please see:

* [Batch operations](/docs/batch.md)
* [CLI documentation](/docs/cli.md), and in particular:
  - [`ais show job`](/docs/cli/job.md)
  - [`ais show job dsort`](/docs/cli/dsort.md)
  - [`ais show job download`](/docs/cli/download.md)
  - [`ais show rebalance`](/docs/rebalance.md)
* And also:
  - [`ais etl`](/docs/cli/etl.md)
  - [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges)
  - [reading, writing, and listing archives](/docs/cli/object.md)
  - [copying buckets](/docs/cli/bucket.md#copy-bucket)

---------------------------------------------------------------


Supported extended actions are enumerated in the [user-facing API](/cmn/api.go) and include:

* cluster-wide rebalancing (denoted as `ActGlobalReb` in the [API](/cmn/api.go)) that gets triggered when storage targets join or leave the cluster
* LRU-based cache eviction (see [LRU](/docs/storage_svcs.md#lru)) that depends on the remaining free capacity and [configuration](/deploy/dev/local/aisnode_config.sh)
* prefetching batches of objects (or arbitrary size) from the Cloud (see [List/Range Operations](/docs/batch.md))
* consensus voting (when conducting new leader [election](/docs/ha.md#election))
* erasure-encoding objects in a EC-configured bucket (see [Erasure coding](/docs/storage_svcs.md#erasure-coding))
* creating additional local replicas, and reducing number of object replicas in a given locally-mirrored bucket (see [Storage Services](/docs/storage_svcs.md))

There are different actions that may be taken upon xaction.
Actions include stats, start and stop.
List of supported actions can be found in the [API](/cmn/api.go)

Xaction requests are generic for all xactions, but responses from each xaction are different.
See [below](#start-and-stop).
The request looks as follows:

1. Single target request:

    ```console
    $ curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "actiontype", "name": "xactionname", "value":{"bucket":"bucketname"}}' 'http://T/v1/daemon?what=xaction'
    ```

    To simplify the logic, result is always an array, even if there's only one element in the result

2. Proxy request, which executes a request on all targets within the cluster, and responds with list of targets' responses:

    ```console
    $ curl -i -X GET  -H 'Content-Type: application/json' -d '{"action": "actiontype", "name": "xactionname", "value":{"bucket":"bucketname"}}' 'http://G/v1/cluster?what=xaction'
    ```

    Response of a query to proxy is a map of daemonID -> target's response. If any of targets responded with error status code, the proxy's response
    will result in the same error response.


### Start and Stop

For a successful request, the response only contains the HTTP status code. If the request was sent to the proxy and all targets
responded with a successful HTTP code, the proxy would respond with the successful HTTP code. The response body should be omitted.

For an unsuccessful request, the target's response contains the error code and error message. If the request was sent to proxy and at least one
of targets responded with an error code, the proxy will respond with the same error code and error message.

> As always, `G` above (and throughout this entire README) serves as a placeholder for the _real_ gateway's hostname/IP address and `T` serves for placeholder for target's hostname/IP address. More information in [notation section](/docs/http_api.md#notation).

The corresponding [RESTful API](/docs/http_api.md) includes support for querying all xactions including global-rebalancing and prefetch operations.

### Stats

Stats request results in list of requested xactions. Statistics of each xaction share a common base format which looks as follow:

```json
[
   {
      "id":1,
      "kind":"ec-get",
      "bucket":"test",
      "startTime":"2019-04-15T12:40:18.721697505-07:00",
      "endTime":"0001-01-01T00:00:00Z",
      "status":"InProgress"
   },
   {
      "id":2,
      "kind":"ec-put",
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
        "tx.n": 0,
        "tx.size": 0,
        "rx.n": 0,
        "rx.size": 0
      }
    }
]
```

If flag `--all` is provided, stats command will display old, finished xactions, along with currently running ones. If `--all` is not set (default), only
the most recent xactions will be displayed, for each bucket, kind or (bucket, kind)

## References

For xaction-related CLI documentation and examples and supported multi-object (batch) operations, please see:

* [Batch operations](/docs/batch.md)
