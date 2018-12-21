// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/xoshiro256"
)

//
// API types
//
type (
	Receive func(w http.ResponseWriter, hdr Header, object io.Reader, err error)
)

// internal types
type (
	iterator struct {
		trname    string
		body      io.ReadCloser
		headerBuf []byte
	}
	objReader struct {
		body io.ReadCloser
		hdr  Header
		off  int64
	}
	handler struct {
		trname      string
		callback    Receive
		sessions    sync.Map // map[int64]*Stats
		oldSessions sync.Map // map[int64]time.Time
	}
)

// session stats cleanup timeout
const cleanupTimeout = time.Minute

//====================
//
// globals
//
//====================
var (
	muxers   map[string]*http.ServeMux // "mux" stands for HTTP request multiplexer
	handlers map[string]map[string]*handler
	mu       *sync.Mutex
	debug    bool
)

func init() {
	mu = &sync.Mutex{}
	muxers = make(map[string]*http.ServeMux)
	handlers = make(map[string]map[string]*handler)
	debug = os.Getenv("DFC_STREAM_DEBUG") != ""
}

//
// API
//

func SetMux(network string, x *http.ServeMux) {
	if !cmn.NetworkIsKnown(network) {
		glog.Warningf("Unknown network %s, expecting one of: %v", network, cmn.KnownNetworks)
	}
	mu.Lock()
	muxers[network] = x
	handlers[network] = make(map[string]*handler)
	mu.Unlock()
}

// examples resulting URL.Path: /v1/transport/replication, /v1/transport/rebalance, etc.
//
// NOTE:
//
// HTTP request multiplexer matches the URL of each incoming request against a list of registered
// paths and calls the handler for the path that most closely matches the URL.
// That is why registering a new endpoint with a given network (and its per-network multiplexer)
// should not be done concurrently with traffic that utilizes this same network.
//
// The limitation is rooted in the fact that, when registering, we insert an entry into the
// http.ServeMux private map of its URL paths.
// This map is protected by a private mutex and is read-accessed to route HTTP requests.
//
func Register(network, trname string, callback Receive) (path string, err error) {
	mu.Lock()
	mux, ok := muxers[network]
	if !ok {
		err = fmt.Errorf("failed to register path /%s: network %s is unknown", trname, network)
		mu.Unlock()
		return
	}

	h := &handler{trname: trname, callback: callback}
	path = cmn.URLPath(cmn.Version, cmn.Transport, trname)
	mux.HandleFunc(path, h.receive)
	if _, ok = handlers[network][trname]; ok {
		glog.Errorf("Warning: re-registering transport handler '%s'", trname)
	}
	handlers[network][trname] = h
	mu.Unlock()
	return
}

func GetNetworkStats(network string) (netstats map[string]EndpointStats, err error) {
	mu.Lock()
	handlers, ok := handlers[network]
	if !ok {
		err = fmt.Errorf("network %s has no handlers", network)
		mu.Unlock()
		return
	}
	netstats = make(map[string]EndpointStats)
	for trname, h := range handlers {
		eps := make(EndpointStats)
		f := func(key, value interface{}) bool {
			out := &Stats{}
			sessid := key.(int64)
			in := value.(*Stats)
			out.Num = atomic.LoadInt64(&in.Num)
			out.Offset = atomic.LoadInt64(&in.Offset)
			out.Size = atomic.LoadInt64(&in.Size)
			eps[sessid] = out
			return true
		}
		h.sessions.Range(f)
		netstats[trname] = eps
	}
	mu.Unlock()
	return
}

//
// internal methods
//

func (h *handler) receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		cmn.InvalidHandlerDetailed(w, r, fmt.Sprintf("Invalid http method %s", r.Method))
		return
	}
	trname := path.Base(r.URL.Path)
	if trname != h.trname {
		cmn.InvalidHandlerDetailed(w, r, fmt.Sprintf("Invalid transport handler name %s - expecting %s", trname, h.trname))
		return
	}
	it := iterator{trname: trname, body: r.Body, headerBuf: make([]byte, MaxHeaderSize)}
	for {
		var stats *Stats
		objReader, sessid, hl64, err := it.next()
		if sessid != 0 {
			statsif, loaded := h.sessions.LoadOrStore(sessid, &Stats{})
			if !loaded && (bool(glog.V(4)) || debug) {
				glog.Infof("%s[%d]: start-of-stream", trname, sessid)
			}
			stats = statsif.(*Stats)
		}
		if stats != nil && hl64 != 0 {
			off := atomic.AddInt64(&stats.Offset, hl64)
			if bool(glog.V(4)) || debug {
				glog.Infof("%s[%d]: offset=%d, hlen=%d", trname, sessid, off, hl64)
			}
		}
		if objReader != nil {
			h.callback(w, objReader.hdr, objReader, nil)
			num := atomic.AddInt64(&stats.Num, 1)
			if objReader.hdr.Dsize != objReader.off {
				err = fmt.Errorf("%s[%d]: stream breakage type #3: reader offset %d != %d object size, num=%d",
					trname, sessid, objReader.off, objReader.hdr.Dsize, num)
				glog.Errorln(err)
			} else {
				siz := atomic.AddInt64(&stats.Size, objReader.hdr.Dsize)
				off := atomic.AddInt64(&stats.Offset, objReader.hdr.Dsize)
				if bool(glog.V(4)) || debug {
					glog.Infof("%s[%d]: offset=%d, size=%d(%d), num=%d", trname, sessid, off, siz, objReader.hdr.Dsize, num)
				}
				continue
			}
		}
		if err != nil {
			if sessid != 0 {
				// delayed cleanup old sessions
				f := func(key, value interface{}) bool {
					id := key.(int64)
					timeClosed := value.(time.Time)
					if time.Since(timeClosed) > cleanupTimeout {
						h.oldSessions.Delete(id)
						h.sessions.Delete(id)
					}
					return true
				}
				h.oldSessions.Range(f)
				h.oldSessions.Store(sessid, time.Now())
			}
			if err != io.EOF {
				h.callback(w, Header{}, nil, err)
				cmn.InvalidHandlerDetailed(w, r, err.Error())
			}
			return
		}
	}
}

func (it iterator) next() (obj *objReader, sessid, hl64 int64, err error) {
	var (
		n   int
		hdr Header
	)
	n, err = it.body.Read(it.headerBuf[:sizeofI64*2])
	if n < sizeofI64*2 {
		cmn.Assert(err != nil, "expecting an error or EOF as the reason for failing to read 16 bytes")
		if err != io.EOF {
			glog.Errorf("%s: %v", it.trname, err)
		}
		return
	}
	// extract and validate hlen
	_, hl64 = extInt64(0, it.headerBuf)
	hlen := int(hl64)
	if hlen > len(it.headerBuf) {
		err = fmt.Errorf("%s: stream breakage type #1: header length %d", it.trname, hlen)
		return
	}
	_, checksum := extUint64(0, it.headerBuf[sizeofI64:])
	chc := xoshiro256.Hash(uint64(hl64))
	if checksum != chc {
		err = fmt.Errorf("%s: stream breakage type #2: header length %d checksum %x != %x", it.trname, hlen, checksum, chc)
		return
	}
	if debug {
		cmn.Assert(hlen < len(it.headerBuf))
	}
	hl64 += int64(sizeofI64) * 2 // to account for hlen and its checksum
	n, err = it.body.Read(it.headerBuf[:hlen])
	if n == 0 {
		return
	}
	if debug {
		cmn.Assert(n == hlen, fmt.Sprintf("%d != %d", n, hlen))
	}
	hdr, sessid = ExtHeader(it.headerBuf, hlen)
	if hdr.IsLast() {
		if bool(glog.V(4)) || debug {
			glog.Infof("%s[%d]: last", it.trname, sessid)
		}
		err = io.EOF
		return
	}
	if bool(glog.V(4)) || debug {
		glog.Infof("%s[%d]: new object size=%d", it.trname, sessid, hdr.Dsize)
	}
	obj = &objReader{body: it.body, hdr: hdr}
	return
}

func (obj *objReader) Read(b []byte) (n int, err error) {
	rem := obj.hdr.Dsize - obj.off
	if rem < int64(len(b)) {
		b = b[:int(rem)]
	}
	n, err = obj.body.Read(b)
	obj.off += int64(n)
	switch err {
	case nil:
		if obj.off >= obj.hdr.Dsize {
			err = io.EOF
			if debug {
				cmn.Assert(obj.off == obj.hdr.Dsize)
			}
		}
	case io.EOF:
		if obj.off != obj.hdr.Dsize {
			glog.Errorf("size %d != %d Dsize", obj.off, obj.hdr.Dsize)
		}
	default:
		glog.Errorf("err %v", err) // canceled?
	}
	return
}

//
// helpers
//
func ExtHeader(body []byte, hlen int) (hdr Header, sessid int64) {
	var off int
	off, hdr.Bucket = extString(0, body)
	off, hdr.Objname = extString(off, body)
	off, hdr.Opaque = extByte(off, body)
	off, sessid = extInt64(off, body)
	off, hdr.Dsize = extInt64(off, body)
	if debug {
		cmn.Assert(off == hlen, fmt.Sprintf("off %d, hlen %d", off, hlen))
	}
	return
}

func extString(off int, from []byte) (int, string) {
	l := int(binary.BigEndian.Uint64(from[off:]))
	off += sizeofI64
	return off + l, string(from[off : off+l])
}

func extByte(off int, from []byte) (int, []byte) {
	l := int(binary.BigEndian.Uint64(from[off:]))
	off += sizeofI64
	return off + l, from[off : off+l]
}

func extInt64(off int, from []byte) (int, int64) {
	size := int64(binary.BigEndian.Uint64(from[off:]))
	off += sizeofI64
	return off, size
}

func extUint64(off int, from []byte) (int, uint64) {
	size := binary.BigEndian.Uint64(from[off:])
	off += sizeofI64
	return off, size
}
