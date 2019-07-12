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
	"math"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4"
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
		body      io.Reader
		headerBuf []byte
	}
	objReader struct {
		body io.Reader
		off  int64
		hdr  Header
	}
	handler struct {
		trname      string
		callback    Receive
		sessions    sync.Map // map[uint64]*Stats
		oldSessions sync.Map // map[uint64]time.Time
		hkName      string   // house-keeping name
	}
)

const (
	cleanupInterval = time.Minute * 10
)

const (
	pkgName = "transport"
)

//
// globals
//

var (
	muxers   map[string]*mux.ServeMux // "mux" stands for HTTP request multiplexer
	handlers map[string]map[string]*handler
	mu       *sync.Mutex
)

func init() {
	mu = &sync.Mutex{}
	muxers = make(map[string]*mux.ServeMux)
	handlers = make(map[string]map[string]*handler)
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		glog.SetV(glog.SmoduleTransport, logLvl)
	}
}

//
// API
//

func SetMux(network string, x *mux.ServeMux) {
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
func Register(network, trname string, callback Receive) (upath string, err error) {
	mu.Lock()
	mux, ok := muxers[network]
	if !ok {
		err = fmt.Errorf("failed to register path /%s: network %s is unknown", trname, network)
		mu.Unlock()
		return
	}

	h := &handler{trname: trname, callback: callback, hkName: path.Join(network, trname, "oldSessions")}
	upath = cmn.URLPath(cmn.Version, cmn.Transport, trname)
	mux.HandleFunc(upath, h.receive)
	if _, ok = handlers[network][trname]; ok {
		glog.Errorf("Warning: re-registering transport handler '%s'", trname)
	}
	handlers[network][trname] = h
	mu.Unlock()

	hk.Housekeeper.Register(h.hkName, h.cleanupOldSessions)
	return
}

func Unregister(network, trname string) (err error) {
	mu.Lock()
	mux, ok := muxers[network]
	if !ok {
		err = fmt.Errorf("failed to unregister transport endpoint %s: network %s is unknown", trname, network)
		mu.Unlock()
		return
	}
	var h *handler
	if h, ok = handlers[network][trname]; !ok {
		err = fmt.Errorf("cannot unregister: transport endpoint %s is unknown, network %s", trname, network)
		mu.Unlock()
		return
	}
	delete(handlers[network], trname)

	upath := cmn.URLPath(cmn.Version, cmn.Transport, trname)
	mux.Unhandle(upath)
	mu.Unlock()

	hk.Housekeeper.Unregister(h.hkName)
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
			uid := key.(uint64)
			in := value.(*Stats)
			out.Num.Store(in.Num.Load())
			out.Offset.Store(in.Offset.Load())
			out.Size.Store(in.Size.Load())
			eps[uid] = out
			return true
		}
		h.sessions.Range(f)
		netstats[trname] = eps
	}
	mu.Unlock()
	return
}

//
// internal methods -- handler
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
	var (
		reader    io.Reader = r.Body
		lz4Reader *lz4.Reader
	)
	if compressionType := r.Header.Get(cmn.HeaderCompress); compressionType != "" {
		cmn.Assert(compressionType == cmn.LZ4Compression)
		lz4Reader = lz4.NewReader(r.Body)
		reader = lz4Reader
	}
	debug := bool(glog.FastV(4, glog.SmoduleTransport))
	sessID, err := strconv.ParseInt(r.Header.Get(cmn.HeaderSessID), 10, 64)
	if err != nil || sessID == 0 {
		cmn.InvalidHandlerDetailed(w, r, fmt.Sprintf("%s[:%d]: invalid session ID, err %v", trname, sessID, err))
		return
	}
	uid := uniqueID(r, sessID)
	statsif, loaded := h.sessions.LoadOrStore(uid, &Stats{})
	if !loaded && debug {
		xxh, id := UID2SessID(uid)
		cmn.Assert(id == uint64(sessID))
		glog.Infof("%s[%d:%d]: start-of-stream from %s", trname, xxh, sessID, r.RemoteAddr) // r.RemoteAddr => xxh
	}
	stats := statsif.(*Stats)

	it := iterator{trname: trname, body: reader, headerBuf: make([]byte, maxHeaderSize)}
	for {
		objReader, hl64, err := it.next()
		if hl64 != 0 {
			_ = stats.Offset.Add(hl64)
		}
		if objReader != nil {
			er := err
			if er == io.EOF {
				er = nil
			}
			h.callback(w, objReader.hdr, objReader, er)
			hdr := &objReader.hdr
			if hdr.ObjAttrs.Size == objReader.off {
				var (
					num = stats.Num.Inc()
					siz = stats.Size.Add(hdr.ObjAttrs.Size)
					off = stats.Offset.Add(hdr.ObjAttrs.Size)
				)
				if debug {
					xxh, _ := UID2SessID(uid)
					glog.Infof("%s[%d:%d]: off=%d, size=%d(%d), num=%d - %s/%s",
						trname, xxh, sessID, off, siz, hdr.ObjAttrs.Size, num, hdr.Bucket, hdr.Objname)
				}
				continue
			}
			xxh, _ := UID2SessID(uid)
			err = fmt.Errorf("%s[%d:%d]: sbrk #3: err %v, off %d != %d size, num=%d, %s/%s",
				trname, xxh, sessID, err, objReader.off, hdr.ObjAttrs.Size, stats.Num.Load(), hdr.Bucket, hdr.Objname)
		}
		if err != nil {
			h.oldSessions.Store(uid, time.Now())
			if err != io.EOF {
				h.callback(w, Header{}, nil, err)
				cmn.InvalidHandlerDetailed(w, r, err.Error())
			}
			if lz4Reader != nil {
				lz4Reader.Reset(nil)
			}
			return
		}
	}
}

func (h *handler) cleanupOldSessions() time.Duration {
	now := time.Now()
	f := func(key, value interface{}) bool {
		uid := key.(uint64)
		timeClosed := value.(time.Time)
		if now.Sub(timeClosed) > cleanupInterval {
			h.oldSessions.Delete(uid)
			h.sessions.Delete(uid)
		}
		return true
	}
	h.oldSessions.Range(f)
	return cleanupInterval
}

//
// iterator
//

func (it iterator) next() (obj *objReader, hl64 int64, err error) {
	var (
		n   int
		hdr Header
	)
	n, err = it.body.Read(it.headerBuf[:cmn.SizeofI64*2])
	if n < cmn.SizeofI64*2 {
		cmn.Assert(err != nil) // expecting an error or EOF as the reason for failing to read 16 bytes
		if err != io.EOF {
			glog.Errorf("%s: %v", it.trname, err)
		}
		return
	}
	// extract and validate hlen
	_, hl64 = extInt64(0, it.headerBuf)
	hlen := int(hl64)
	if hlen > len(it.headerBuf) {
		err = fmt.Errorf("%s: sbrk #1: header length %d", it.trname, hlen)
		return
	}
	_, checksum := extUint64(0, it.headerBuf[cmn.SizeofI64:])
	chc := xoshiro256.Hash(uint64(hl64))
	if checksum != chc {
		err = fmt.Errorf("%s: sbrk #2: header length %d checksum %x != %x", it.trname, hlen, checksum, chc)
		return
	}
	cmn.Dassert(hlen < len(it.headerBuf), pkgName)
	hl64 += int64(cmn.SizeofI64) * 2 // to account for hlen and its checksum
	n, err = it.body.Read(it.headerBuf[:hlen])
	if n == 0 {
		return
	}
	if _, ok := cmn.CheckDebug(pkgName); ok {
		cmn.AssertMsg(n == hlen, fmt.Sprintf("%d != %d", n, hlen))
	}
	hdr = ExtHeader(it.headerBuf, hlen)
	if hdr.IsLast() {
		err = io.EOF
		return
	}
	obj = &objReader{body: it.body, hdr: hdr}
	return
}

//
// objReader
//

func (obj *objReader) Read(b []byte) (n int, err error) {
	rem := obj.hdr.ObjAttrs.Size - obj.off
	if rem < int64(len(b)) {
		b = b[:int(rem)]
	}
	n, err = obj.body.Read(b)
	obj.off += int64(n)
	switch err {
	case nil:
		if obj.off >= obj.hdr.ObjAttrs.Size {
			err = io.EOF
			cmn.Dassert(obj.off == obj.hdr.ObjAttrs.Size, pkgName)
		}
	case io.EOF:
		if obj.off != obj.hdr.ObjAttrs.Size {
			glog.Errorf("actual off: %d, expected: %d", obj.off, obj.hdr.ObjAttrs.Size)
		}
	default:
		glog.Errorf("err %v\n", err) // canceled?
	}
	return
}

//
// helpers
//
func ExtHeader(body []byte, hlen int) (hdr Header) {
	var off int
	off, hdr.Bucket = extString(0, body)
	off, hdr.Objname = extString(off, body)
	off, hdr.IsLocal = extBool(off, body)
	off, hdr.Opaque = extByte(off, body)
	off, hdr.ObjAttrs = extAttrs(off, body)
	if _, ok := cmn.CheckDebug(pkgName); ok {
		cmn.AssertMsg(off == hlen, fmt.Sprintf("off %d, hlen %d", off, hlen))
	}
	return
}

func extString(off int, from []byte) (int, string) {
	off, bt := extByte(off, from)
	return off, string(bt)
}

func extBool(off int, from []byte) (int, bool) {
	off, bt := extByte(off, from)
	return off, bt[0] != 0
}

func extByte(off int, from []byte) (int, []byte) {
	l := int(binary.BigEndian.Uint64(from[off:]))
	off += cmn.SizeofI64
	return off + l, from[off : off+l]
}

func extInt64(off int, from []byte) (int, int64) {
	off, val := extUint64(off, from)
	return off, int64(val)
}

func extUint64(off int, from []byte) (int, uint64) {
	size := binary.BigEndian.Uint64(from[off:])
	off += cmn.SizeofI64
	return off, size
}

func extAttrs(off int, from []byte) (n int, attr ObjectAttrs) {
	off, attr.Size = extInt64(off, from)
	off, attr.Atime = extInt64(off, from)
	off, attr.CksumType = extString(off, from)
	off, attr.CksumValue = extString(off, from)
	off, attr.Version = extString(off, from)
	return off, attr
}

//
// sessID => unique ID
//
func uniqueID(r *http.Request, sessID int64) uint64 {
	x := xxhash.ChecksumString64S(r.RemoteAddr, cmn.MLCG32)
	return (x&math.MaxUint32)<<32 | uint64(sessID)
}

func UID2SessID(uid uint64) (xxh, sessID uint64) {
	xxh, sessID = uid>>32, uid&math.MaxUint32
	return
}
