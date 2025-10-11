Package `transport` provides streaming object-based transport over HTTP for massive intra-AIS data transfers. AIStore utilizes this package for cluster-wide (aka "global") rebalancing, distributed merge-sort and more.

- [Build](#build)
- [Description](#description)
- [Closing and completions](#closing-and-completions)
- [Commented example](#commented-example)
- [Registering HTTP endpoint](#registering-http-endpoint)
- [On the wire](#on-the-wire)
- [Transport statistics](#transport-statistics)
- [Stream Bundle](#stream-bundle)
- [Testing](#testing)
- [Environment](#environment)

## Build

There are two alternative ways to build `transport` package for intra-cluster networking:

1. using Go net/http, or
2. with a 3rd party github.com/valyala/fasthttp aka "fasthttp" (default)

Respectively, the `transport` package includes build-time support for two alternative http clients:

* standard [net/http](https://golang.org/pkg/net/http/)
* 3rd party [github.com/valyala/fasthttp](https://github.com/valyala/fasthttp) aka "fasthttp"

The following is a quick summary:

| Client | Reference | Build Tag | Default |
|--- | --- | --- | ---|
| `net/http` | [golang.org/pkg/net/http](https://golang.org/pkg/net/http/) | `nethttp` | no |
| `fasthttp` | [github.com/valyala/fasthttp](https://github.com/valyala/fasthttp) | n/a  | yes |

> For details, please see [build-tags](https://github.com/NVIDIA/aistore/blob/main/docs/build_tags.md) and top-level [Makefile](https://github.com/NVIDIA/aistore/blob/main/Makefile)

To test with net/http, run:

```console
$ go test -v -tags=nethttp
```

or, the same, with logs redirected to STDERR:

```console
$ go test -v -logtostderr=true -tags=nethttp
```

For more examples, see [testing](#testing) below.

## Description

A **stream** (or, more exactly, a `transport.Stream`) asynchronously transfers **objects** between two HTTP endpoints.
The objects, in turn, are defined by their **headers** (`transport.Header`) and their **readers** ([io.ReadCloser](https://golang.org/pkg/io/#ReadCloser)).

A stream preserves ordering: the objects posted for sending will get *completed* in the same exact order
(for more on *completions*, see below), and certainly transferred to the receiver in the same exact order as well.

| Term | Description | Example |
|--- | --- | ---|
| Stream | A peer-to-peer flow over HTTP where a single HTTP request (and, therefore, a single TCP session) is used to transfer multiple objects | `transport.NewStream(client, "http://example.com", nil)` - creates a stream between the local client and the `example.com` host |
| Stream bundle | Multiple streams (see previous) aggregating completions and preserving FIFO ordering; note that the number of streams in a _bundle_ is configurable - see `bundle_multiplier` | `transport.NewStreamBundle(smap, si, client, transport.SBArgs{Network: transport.cmn.NetworkPublic, Trname: "path-name", Extra: &extra, Ntype: cluster.Targets, ManualResync: false, Multiplier: 4})` |
| Object | Any [io.ReadCloser](https://golang.org/pkg/io/#ReadCloser) that is accompanied by a transport header that specifies, in part, the object's size and the object's (bucket, name) at the destination | `transport.Header{"abc", "X", nil, 1024*1024}` - specifies a 1MB object that will be named `abc/X` at the destination |
| Object Attributes | Objects are often associated with their attributes like size, access time, checksum and version. When sending the object it is often necessary to also send these attributes with the object so the receiver can update the object metadata. | `transport.ObjectAttrs{Atime: time.Now(), Size: 13, CksumType: "xxhash", Chksum: "s0m3ck5um", Version: "2"}`
| Object Header | A `transport.Header` structure that, in addition to bucket name, object name, and object size, carries an arbitrary (*opaque*) sequence of bytes that, for instance, may be a JSON message or anything else. | `transport.Header{"abracadabra", "p/q/s", false, []byte{'1', '2', '3'}, transport.ObjectAttrs{Size: 13}}` - describes a 13-byte object that, in the example, has some application-specific and non-nil *opaque* field in the header |
| Receive callback | A function that has the following signature: `Receive func(http.ResponseWriter, transport.Header, io.Reader)`. Receive callback must be *registered* prior to the very first object being transferred over the stream - see next. | Notice the last parameter in the receive callback: `io.Reader`. Behind this (reading) interface, there's a special type reader supporting, in part, object boundaries. In other words, each callback invocation corresponds to one transferred and received object. Note as well the object header that is also delivered to the receiving endpoint via the same callback. |
| Registering receive callback | An API to establish the one-to-one correspondence between the stream sender and the stream receiver | For instance, to register the same receive callback `foo` with two different HTTP endpoints named "ep1" and "ep2", we could call `transport.Register("n1", "ep1", foo)` and `transport.Register("n1", "ep2", foo)`, where `n1` is an http request multiplexer ("muxer") that corresponds to one of the documented networking options - see [README, section Networking](README.md). The transport will then be calling `foo()` to separately deliver the "ep1" stream to the "ep1" endpoint and "ep2" - to, respectively, "ep2". Needless to say that a per-endpoint callback is also supported and permitted. To allow registering endpoints to different http request multiplexers, one can change network parameter `transport.Register("different-network", "ep1", foo)` |
| Object-has-been-sent callback (not to be confused with the Receive callback above) | A function or a method of the following signature: `SendCallback func(Header, io.ReadCloser, error)`, where `transport.Header` and `io.ReadCloser` represent the object that has been transmitted and error is the send error or nil | This callback can optionally be defined on a) per-stream basis (via NewStream constructor) and/or b) for a given object that is being sent (for instance, to support some sort of batch semantics). Note that object callback *overrides* the per-stream one: when (object callback) is defined i.e., non-nil, the stream callback is ignored and skipped.<br/><br/>**BEWARE:**<br/>Latency of this callback adds to the latency of the entire stream operation on the send side. It is critically important, therefore, that user implementations do not take extra locks, do not execute system calls and, generally, return as soon as possible. |
| Header-only objects | Header-only (data-less) objects are supported - when there's no data to send (that is, when the `transport.Header.Dsize` field is set to zero), the reader (`io.ReadCloser`) is not required and the corresponding argument in the the `Send()` API can be set to nil | Header-only objects can be used to implement L6 control plane over streams, where the header's `Opaque` field gets utilized to transfer the entire (control message's) payload |

## Closing and completions

In streams, the sending pipeline is implemented as a pair (SQ, SCQ) where the former is a send queue
realized as a channel, and the latter is a send completion queue (and a different Go channel).
Together, SQ and SCQ form a FIFO as far as ordering of transmitted objects.

Once an object is put on the wire, the corresponding completion gets queued and eventually gets
processed by the completion handler. The handling **always entails closing of the object reader**.

To reiterate: object reader is always closed by the code that handles `send completions`.
In the case when the callback (`SendCallback`) is provided (i.e., non-nil), the closing is done 
right after invoking the callback.

Note as well that for every transmission of every object there's always a *completion*.
This holds true in all cases including network errors that may cause sudden and instant termination
of the underlying stream(s).

## Commented example

```go
path := transport.Register("n1", "ep1", testReceive) // register receive callback with HTTP endpoint "ep1" to "n1" network
client := &http.Client{Transport: &http.Transport{}} // create default HTTP client
url := "http://example.com/" +  path // combine the hostname with the result of the Register() above

// open a stream (to the http endpoint identified by the url) with burst equal 10 and the capability to cancel at any time
// ("burst" is the number of objects the caller is permitted to post for sending without experiencing any sort of back-pressure)
ctx, cancel := context.WithCancel(context.Background())
stream := transport.NewStream(client, url, &transport.Extra{Burst: 10, Ctx: ctx})

// NOTE: constructing a transport stream does not necessarily entail establishing TCP connection.
// Actual connection establishment is delayed until the very first object gets posted for sending.
// The underlying HTTP/TCP session will also terminate after a (configurable) period of inactivity
// (`Extra.IdleTimeout`), only to be re-established when (and if) the traffic picks up again.

for  {
	hdr := transport.Header{...} 	// next object header
	object := ... 			// next object reader, e.g. os.Open("some file")
	// send the object asynchronously (the 3rd arg specifies an optional "object-has-been-sent" callback)
	stream.Send(hdr, object, nil)
	...
}
stream.Fin() // gracefully close the stream (call it in all cases except after canceling (aborting) the stream)
```

## Registering HTTP endpoint

On the receiving side, each network contains multiple HTTP endpoints, whereby each HTTP endpoint, in turn, may have zero or more stream sessions.
In effect, there are two nested many-to-many relationships whereby you may have multiple logical networks, each containing multiple named transports, etc.

The following:

```go
path, err := transport.Register("public", "myapp", mycallback)
```

adds a transport endpoint named "myapp" to the "public" network (that must already exist), and then registers a user callback with the latter.

The last argument, user-defined callback, must have the following typedef:

```go
Receive func(w http.ResponseWriter, hdr Header, object io.Reader, err error)
```

The callback is being invoked on a per received object basis (note that a single stream may transfer multiple, potentially unlimited, number of objects).
Callback is always invoked in case of an error.

Back to the registration. On the HTTP receiving side, the call to `Register` translates as:

```go
mux.HandleFunc(path, mycallback)
```

where mux is `mux.ServeMux` (fork of `net/http` package) that corresponds to the named network ("public", in this example), and path is a URL path ending with "/myapp".

## On the wire

On the wire, each transmitted object will have the layout:

> `[header length] [header fields including object name and size] [object bytes]`

The size must be known upfront, which is the current limitation.

A stream (the [Stream type](/transport/send.go)) carries a sequence of objects of arbitrary sizes and contents, and overall looks as follows:

> `object1 = (**[header1]**, **[data1]**)` `object2 = (**[header2]**, **[data2]**)`, etc.

Stream termination is denoted by a special marker in the data-size field of the header:

> `header = [object size=7fffffffffffffff]`

## Transport statistics

The API that queries runtime statistics includes:

```go
func (s *Stream) GetStats() (stats Stats)
```

- on the send side, and

```go
func GetNetworkStats(network string) (netstats map[string]EndpointStats, err error)
```

- on receive.

Statistics themselves include the following metrics:

```go
Stats struct {
	Num     int64   // number of transferred objects
	Size    int64   // transferred size, in bytes
	Offset  int64   // stream offset, in bytes
	IdleDur int64   // the time stream was idle since the previous GetStats call
	TotlDur int64   // total time since the previous GetStats
	IdlePct float64 // idle time %
}
```

On the receive side, the `EndpointStats` map contains all the `transport.Stats` structures indexed by (unique) stream IDs for the currently active streams.

For usage examples and details, please see tests in the package directory.

## Stream Bundle

Stream bundle (`transport.StreamBundle`) in this package is motivated by the need to broadcast and multicast continuously over a set of long-lived TCP sessions. The scenarios in storage clustering include intra-cluster replication and erasure coding, rebalancing (upon *target-added* and *target-removed* events) and MapReduce-generated flows and more.

In each specific case, a given clustered node needs to maintain control and/or data flows between itself and multiple other clustered nodes, where each of the flows would be transferring large numbers of control and data objects, or parts of thereof.

The provided implementation aggregates transport streams. A stream (or, a `transport.Stream`) asynchronously transfers *objects* between two HTTP endpoints, whereby an object is defined as a combination of `transport.Header` and an ([io.ReadCloser](https://golang.org/pkg/io/#ReadCloser)) interface. The latter may have a variety of well-known implementations: file, byte array, scatter-gather list of buffers, etc.

The important distinction, though, is that while transport streams are devoid of any clustering "awareness", a *stream bundle* is fully integrated with a cluster. Internally, the implementation utilizes cluster-level abstractions, such as a *node* (`cluster.Snode`), a *cluster map* (`cluster.Smap`), and more.

The provided API includes `StreamBundle` constructor that allows to establish streams between the local node and (a) all storage targets, (b) all gateways, or (c) all nodes in the cluster - in one shot:

```
sbArgs := &SBArgs{
  Network	string,		// network, one of `cmn.KnownNetworks`
  Trname	string,		// transport endpoint name
  Extra		*Extra, // additional stream control parameters
  Ntype 	int,		// destination type: all targets, ..., all nodes
  ManualResync bool,		// if false, establishes/removes connections with new/old nodes when new smap is received
  Multiplier int,		// number of streams per destination, with subsequent round-robin selection
}

NewStreamBundle(
  sowner	cluster.Sowner,		// Smap (cluster map) owner interface
  lsnode	*cluster.Snode,		// local node
  cl		*http.Client,		// http client
  sbArgs	*SbArgs			// additional stream bundle arguments
)
```

### A note on connection establishment and termination

* For each of the individual transport streams in a bundle, constructing a stream (`transport.Stream`) does not necessarily entail establishing TCP connection. Actual connection establishment is delayed until arrival (via `Send` or `SendV`) of the very first object.
* The underlying HTTP/TCP session will also terminate after a (configurable) period of inactivity, only to be re-established when (and if) the traffic picks up again.

### API

The two main API methods are `Send` and `SendV`:

* to broadcast via all established streams, use `SendV()` and omit the last argument;
* otherwise, use `SendV()` with the destinations specified as a comma-separated list, or
* use `Send()` with a list of nodes on the receive side.

Other provided APIs include terminating all contained streams - gracefully or instantaneously via `Close`, and more.

Finally, there are two important facts to remember:

* When streaming an object to multiple destinations, `StreamBundle` may call `reader.Open()` multiple times as well. For N object replicas (or N identical notifications) over N streams, the original reader (provided via `Send` or `SendV` - see above) will get reopened (N-1) times.

* Completion callback (`transport.SendCallback`), if provided, is getting called only once per object, independently of the number of the object replicas sent to multiple destinations. The callback is invoked by the completion handler of the very last object replica (for more on completion handling.

## Testing

* **Run tests matching "Multi" with debug-enabled assertions**:

```console
$ go test -v -run=Multi -tags=debug
```

* **Use `nethttp` build tag to run with net/http, e.g.**:

```console
$ go test -v -tags=nethttp
```

* **The same with fasthttp (the current default)**:

```console
$ go test -v
```

For more examples, please see tests in the package directory.

## Environment

| Environment Variable | Description |
|--- | --- |
| `AIS_STREAM_BURST_NUM` | Max number of objects the caller is permitted to post for sending without experiencing any sort of back-pressure |
| `AIS_STREAM_DRY_RUN` | If enabled, read and immediately discard all read data (can be used to evaluate client-side throughput) |

