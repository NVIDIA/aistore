## Overview

Package transport provides streaming object-based transport over HTTP for massive intra-DFC and DFC-to-DFC data transfers. This transport layer
can be utilized for cluster-wide rebalancing, replication (of any kind), distributed merge-sort operation, and more.

A **stream** (or, more exactly, a `transport.Stream`) asynchronously transfers **objects** between two HTTP endpoints.
The objects, in turn, are defined by their **headers** (`transport.Header`) and their **readers** ([io.ReadCloser](https://golang.org/pkg/io/#ReadCloser)).

A stream preserves ordering: the objects posted for sending will get *completed* in the same exact order
(for more on *completions*, see below), and certainly transferred to the receiver in the same exact order as well.

| Term | Description | Example |
|--- | --- | ---|
| Stream | A point-to-point flow over HTTP where a single HTTP request (and, therefore, a single TCP session) is used to transfer multiple objects | `transport.NewStream(client, "http://example.com", nil)` - creates a stream between the local client and the `example.com` host |
| Object | Any [io.ReadCloser](https://golang.org/pkg/io/#ReadCloser) that is accompanied by a transport header that specifies, in part, the object's size and the object's (bucket, name) at the destination | `transport.Header{"abc", "X", nil, 1024*1024}` - specifies a 1MB object that will be named `abc/X` at the destination |
| Object Header | A `transport.Header` structure that, in addition to bucket name, object name, and object size, carries an arbitrary (*opaque*) sequence of bytes that, for instance, may be a JSON message or anything else. | `transport.Header{"abracadabra", "p/q/s", []byte{'1', '2', '3'}, 13}` - describes a 13-byte object that, in the example, has some application-specific and non-nil *opaque* field in the header |
| Receive callback | A function that has the following signature: `Receive func(http.ResponseWriter, transport.Header, io.Reader)`. Receive callback must be *registered* prior to the very first object being transferred over the stream - see next. | Notice the last parameter in the receive callback: `io.Reader`. Behind this (reading) interface, there's a special type reader supporting, in part, object boundaries. In other words, each callback invocation corresponds to one ransferred and received object. Note as well the object header that is also delivered to the receiving endpoint via the same callback. |
| Registering receive callback | An API to establish the one-to-one correspondence between the stream sender and the stream receiver | For instance, to register the same receive callback `foo` with two different HTTP endpoints named "ep1" and "ep2", we could call `transport.Register("n1", "ep1", foo)` and `transport.Register("n1", "ep2", foo)`, where `n1` is an http request multiplexer ("muxer") that corresponds to one of the documented networking options - see [README, section Networking](README.md). The transport will then be calling `foo()` to separately deliver the "ep1" stream to the "ep1" endpoint and "ep2" - to, respectively, "ep2". Needless to say that a per-endpoint callback is also supported and permitted. To allow registering endpoints to different http request multiplexers, one can change network parameter `transport.Register("different-network", "ep1", foo)` |
| Object-has-been-sent callback (not to be confused with the Receive callback above) | A function or a method of the following signature: `SendCallback func(Header, io.ReadCloser, error)`, where `transport.Header` and `io.ReadCloser` represent the object that has been transmitted and error is the send error or nil | This callback can optionally be defined on a) per-stream basis (via NewStream constructor) and/or b) for a given object that is being sent (for instance, to support some sort of batch semantics). Note that object callback *overrides* the per-stream one: when (object callback) is defined i.e., non-nil, the stream callback is ignored and skipped.<br/><br/>**BEWARE:**<br/>Latency of this callback adds to the latency of the entire stream operation on the send side. It is critically important, therefore, that user implementations do not take extra locks, do not execute system calls and, generally, return as soon as possible. |
| Header-only objects | Header-only (data-less) objects are supported - when there's no data to send (that is, when the `transport.Header.Dsize` field is set to zero), the reader (`io.ReadCloser`) is not required and the corresponding argument in the the `Send()` API can be set to nil | Header-only objects can be used to implement L6 control plane over streams, where the header's `Opaque` field gets utilized to transfer the entire (control message's) payload |
| Stream bundle | A higher-level (cluster level) API to aggregate multiple streams and broadcast objects replicas to all or some of the established nodes of the cluster while aggregating completions and preserving FIFO ordering | *Stream bundle* is provided by the `cluster` package; a [README](../cluster/README.md) is available there as well |

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

## Example with extended comments

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
where mux is `http.ServeMux` that corresponds to the named network ("public", in this example), and path is a URL path ending with "/myapp".

**BEWARE**

>> HTTP request multiplexer matches the URL of each incoming request against a list of registered paths and calls the handler for the path that most closely matches the URL.

>> That is why registering a new endpoint with a given network (and its per-network multiplexer) should not be done concurrently with traffic that utilizes this same network.

>> The limitation is rooted in the fact that, when registering, we insert an entry into the `http.ServeMux` private map of all its URL paths. This map is protected by a private mutex and is read-accessed to route HTTP requests...

PS. Notice the comment in regards to the stream and object callbacks above as well.

## On the wire

On the wire, each transmitted object will have the layout:

>> [header length] [header fields including object name and size] [object bytes]

The size must be known upfront, which is the current limitation.

A stream (the [Stream type](transport/send.go)) carries a sequence of objects of arbitrary sizes and contents, and overall looks as follows:

>> object1 = (**[header1]**, **[data1]**) object2 = (**[header2]**, **[data2]**), etc.

Stream termination is denoted by a special marker in the data-size field of the header:

>> header = [object size=7fffffffffffffff]

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

## Testing

* To run all tests while redirecting log to STDERR:
```
go test -v -logtostderr=true
```

* To run a test with a name matching "Multi", verbose logging and enabled assertions:
```
DFC_STREAM_DEBUG=1 go test -v -run=Multi
```

For more examples, please see tests in the package directory.


## Environment

| Environment Variable | Description |
|--- | --- |
| DFC_STREAM_DEBUG | Enable inline assertions and verbose tracing |
| DFC_STREAM_BURST_NUM | Max number of objects the caller is permitted to post for sending without experiencing any sort of back-pressure |

## See also

* [Stream Bundle](../cluster/README.md)



