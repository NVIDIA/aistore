## Stream Bundle

Stream bundle (`cluster.StreamBundle`) in this package is motivated by the need to broadcast and multicast continuously over a set of long-lived TCP sessions. The scenarios in storage clustering include intra-cluster replication and erasure coding, rebalancing (upon *target-added* and *target-removed* events) and MapReduce-generated flows, and more.

In each specific case, a given clustered node needs to maintain control and/or data flows between itself and multiple other clustered nodes, where each of the flows would be transferring large numbers of control and data objects, or parts of thereof.

The provided implementation aggregates [transport streams](../transport/README.md). A stream (or, a `transport.Stream`) asynchronously transfers *objects* between two HTTP endpoints, whereby an object is defined as a combination of `transport.Header` and an ([io.ReadCloser](https://golang.org/pkg/io/#ReadCloser)) interface. The latter may have a variety of well-known implementations: file, byte array, scatter-gather list of buffers, etc.

For details on transport streams, please refer to the [README](../transport/README.md).

The important distinction, though, is that while transport streams are devoid of any clustering "awareness", a *stream bundle* is fully integrated with a cluster. Internally, the implementation utilizes cluster-level abstractions, such as a *node* (`cluster.Snode`), a *cluster map* (`cluster.Smap`), and more.

The provided API includes `StreamBundle` constructor that allows to establish streams between the local node and (a) all storage argets, (b) all gateways, or (c) all nodes in the cluster - in one shot:

```
NewStreamBundle(
  sowner	Sowner,		// Smap (cluster map) owner interface
  lsnode	*Snode,		// local node
  cl		*http.Client,	// http client
  network	string,		// network, one of `cmn.KnownNetworks`
  trname	string,		// transport endpoint name
  extra		*transport.Extra, // additional stream control parameters
  ntype 	int,		// destination type: all targets, ..., all nodes
  multiplier	...int,		// number of streams per destination, with subsequent round-robin selection
)
```

## A note on connection establishment and termination

>> For each of the invidual transport streams in a bundle, constructing a stream (`transport.Stream`) does not necessarily entail establishing TCP connection. Actual connection establishment is delayed until arrival (via `Send` or `SendV`) of the very first object.

>> The underlying HTTP/TCP session will also terminate after a (configurable) period of inactivity, only to be re-established when (and if) the traffic picks up again.

## API

The two main API methods are `Send` and `SendV`:

* to broadcast via all established streams, use `SendV()` and omit the last argument;
* otherwise, use `SendV()` with the destinations specified as a comma-separated list, or
* use `Send()` with a list of nodes on the receive side.

Other provided APIs include terminating all contained streams - gracefully or instanteneously via `Close`, and more.

Finally, there are two important facts to remember:

* When streaming an object to multiple destinations, `StreamBundle` may call `reader.Open()` multiple times as well. For N object replicas (or N identical notifications) over N streams, the original reader (provided via `Send` or `SendV` - see above) will get reopened (N-1) times.

* Completion callback (`transport.SendCallback`), if provided, is getting called only once per object, independently of the number of the object replicas sent to multiple destinations. The callback is invoked by the completion handler of the very last object replica (for more on completion handling, please see the [transport README](../transport/README.md)).
