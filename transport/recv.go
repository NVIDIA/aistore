/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
//
// Package transport provides streaming object-based transport over http for
// massive intra-DFC or DFC-to-DFC data transfers.
//
// See README for details and usage examples.
//
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/xoshiro256"
)

//
// API types
//
type (
	Receive func(w http.ResponseWriter, hdr Header, object io.Reader)
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
		hlen int
	}
	handler struct {
		trname   string
		callback Receive
	}
)

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
	knownNetworks := []string{common.NetworkPublic, common.NetworkIntra, common.NetworkReplication}
	if !common.StringInSlice(network, knownNetworks) {
		glog.Warningf("unknown network: %s, expected: %v", network, knownNetworks)
	}

	mu.Lock()
	muxers[network] = x
	handlers[network] = make(map[string]*handler)
	mu.Unlock()
}

// examples resulting URL.Path: /v1/transport/replication, /v1/transport/rebalance, etc.
func Register(network, trname string, callback Receive) (path string) {
	path = api.URLPath(api.Version, api.Transport, trname)
	h := &handler{trname, callback}
	mu.Lock()
	mux, ok := muxers[network]
	if !ok {
		glog.Errorf("no mux was set for this network: %s; registering was not successful for: %s", network, trname)
		return
	}
	mux.HandleFunc(path, h.receive)
	if !strings.HasSuffix(path, "/") {
		mux.HandleFunc(path+"/", h.receive)
	}
	if _, ok = handlers[network][trname]; ok {
		glog.Errorf("Warning: re-registering transport handler '%s'", trname)
	}
	handlers[network][trname] = h
	mu.Unlock()
	return
}

func (h *handler) receive(w http.ResponseWriter, r *http.Request) {
	var numcur, sizecur int64 // TODO: use session ID to track numtot and sizetot
	if r.Method != http.MethodPut {
		common.InvalidHandlerDetailed(w, r, fmt.Sprintf("Invalid http method %s", r.Method))
		return
	}
	trname := path.Base(r.URL.Path)
	if trname != h.trname {
		common.InvalidHandlerDetailed(w, r, fmt.Sprintf("Invalid transport handler name %s - expecting %s", trname, h.trname))
		return
	}
	it := iterator{trname: trname, body: r.Body, headerBuf: make([]byte, MaxHeaderSize)}
	for {
		objReader, err := it.next()
		if objReader != nil {
			if glog.V(4) {
				glog.Infof("%s: receiving num=%d, size=(%d/%d)", trname, numcur+1, objReader.hdr.Dsize, sizecur)
			}
			h.callback(w, objReader.hdr, objReader)
			numcur++
			sizecur += objReader.hdr.Dsize
			continue
		}
		if err == io.EOF {
			glog.Infof("%s: Done num=%d, size=%d", trname, numcur, sizecur)
			return
		}
		if err != nil {
			common.InvalidHandlerDetailed(w, r, err.Error())
			return
		}
	}
}

//
// internal
//

func (it iterator) next() (obj *objReader, err error) {
	n, err := it.body.Read(it.headerBuf[:sizeofI64*2])
	if n < sizeofI64*2 {
		glog.Infof("%s: next %v", it.trname, err)
		return nil, err
	}

	// extract and validate hlen
	_, hl64 := extInt64(0, it.headerBuf)
	hlen := int(hl64)
	if hlen > len(it.headerBuf) {
		return nil, fmt.Errorf("%s: stream breakage type #1: header length %d", it.trname, hlen)
	}
	_, checksum := extUint64(0, it.headerBuf[sizeofI64:])
	chc := xoshiro256.Hash(uint64(hl64))
	if checksum != chc {
		return nil, fmt.Errorf("%s: stream breakage type #2: header length %d checksum %x != %x", it.trname, hlen, checksum, chc)
	}

	if debug {
		common.Assert(hlen < len(it.headerBuf))
	}
	n, err = it.body.Read(it.headerBuf[:hlen])
	if n > 0 {
		if debug {
			common.Assert(n == hlen)
		}
		hdr, sessid := ExtHeader(it.headerBuf, hlen)
		if hdr.IsLast() {
			glog.Infof("%s[%d]: last", sessid, it.trname)
			return nil, io.EOF
		}
		if glog.V(4) {
			glog.Infof("%s[%d]: new object size=%d", sessid, it.trname, hdr.Dsize)
		}
		obj = &objReader{body: it.body, hdr: hdr}
		return
	}
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
				common.Assert(obj.off == obj.hdr.Dsize)
			}
		}
	case io.EOF:
		if obj.off != obj.hdr.Dsize {
			glog.Errorf("size %d != %d Dsize", obj.off, obj.hdr.Dsize)
		}
	default:
		glog.Errorf("err %v", err)
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
		common.Assert(off == hlen, fmt.Sprintf("off %d, hlen %d", off, hlen))
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
