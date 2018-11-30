## Stream Bundle

Stream bundle (`cluster.StreamBundle`) in this package is motivated by the need to broadcast and multicast continuously over a set of long-lived TCP sessions. The scenarios in storage clustering include intra-cluster replication and erasure coding, rebalancing (upon *target-added* and *target-removed* events) and MapReduce-generated flows, and more.

In each specific case, a given clustered node needs to maintain control and/or data flows between itself and multiple other clustered nodes, where each of the flows would be transferring large numbers of control and data objects, or parts of thereof.

The provided implementation aggregates [transport streams](../transport/README.md). A stream (or, more exactly, a `transport.Stream`) asynchronously transfers objects between two HTTP endpoints, whereby an object is defined as a combination of `transport.Header` and an ([io.ReadCloser](https://golang.org/pkg/io/#ReadCloser)) interface that may have a variety of well-known implementations. For details on transport streams, please refer to the [README](../transport/README.md).

The important distinction is that, while transport streams are devoid of any clustering "awareness", a *stream bundle* is fully integrated with the cluster utilizing objects that include a node (`cluster.Snode`), a map (`cluster.Smap`), and more.

The provided API includes `StreamBundle` constructor - to establish streams between the local node and (a) all storage argets, (b) all gateways, or (c) all nodes in the cluster:

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

The two main API methods are `Send` and `SendV`:

* to broadcast via all established streams, use `SendV()` and omit the last argument;
* otherwise, use `SendV()` with destinations specified as a comma-separated list
* or, use `Send()` with a slice of nodes for destinations.

Other APIs include terminating all bundled streams - gracefully, via `Fin`, or instanteneously, via `Stop`, and more.

Finally, two important facts to remember:

* When streaming an object to multiple destinations, `StreamBundle` may call `reader.Open()` multiple times as well. For N object replicas (or N identical notifications) over N bundled streams, the original reader (provided via `Send` or `SendV` - see above) will get reopened (N-1) times.

* Completion callback (`transport.SendCallback`), if provided, is getting called only once per object, independently on the number of the object replicas sent to multiple destinations. The callback is invoked by the completion handler of the very last object replica (for more on completion handling, please see the [transport README](../transport/README.md)).
