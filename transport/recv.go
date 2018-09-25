/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package transport provides L5.5 transport over http for intra-DFC or DFC-to-DFC rebalancing,
// replication, and more.
//
// * On the wire, each transmitted object will have the layout:
//
//   [header length] [header fields including object size}] [object bytes])
//
// * The size must be known upfront, which is the current limitation.
//
// * A stream (the Stream type below) carries a sequence of objects of arbitrary length
//   and overall looks as follows:
//
//   object1 = ([header1], [data1]) object2 = ([header2], [data2]), etc.
//
// * Stream termination is denoted by a special marker in the data-size field of the header:
//
//   header = [object size=^uint64(0) >> 1 (7fffffffffffffff)]
//
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
)

// FIXME: copy-paste invalmsghdlr

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
	mux      *http.ServeMux
	handlers map[string]*handler
	mu       *sync.Mutex
	debug    bool
)

func init() {
	mu = &sync.Mutex{}
	handlers = make(map[string]*handler)
	debug = os.Getenv("DFC_STREAM_DEBUG") != ""
}

//
// API
//
func SetMux(x *http.ServeMux) { mux = x }

// examples resulting URL.Path: /v1/transport/replication, /v1/transport/rebalance, etc.
func Register(trname string, callback Receive) (path string) {
	path = api.URLPath(api.Version, api.Transport, trname)
	h := &handler{trname, callback}
	mux.HandleFunc(path, h.receive)
	if !strings.HasSuffix(path, "/") {
		mux.HandleFunc(path+"/", h.receive)
	}
	mu.Lock()
	_, ok := handlers[trname]
	if ok {
		glog.Errorf("Warning: re-registering transport handler '%s'", trname)
	}
	handlers[trname] = h
	mu.Unlock()
	return
}

func (h *handler) receive(w http.ResponseWriter, r *http.Request) {
	var numcur, sizecur int64 // TODO: add stream session ID to support numtot and sizetot
	if r.Method != http.MethodPut {
		invalmsghdlr(w, r, fmt.Sprintf("Invalid http method %s", r.Method))
		return
	}
	trname := path.Base(r.URL.Path)
	if trname != h.trname {
		invalmsghdlr(w, r, fmt.Sprintf("Invalid transport handler name %s - expecting %s", trname, h.trname))
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
			invalmsghdlr(w, r, err.Error())
			return
		}
	}
}

//
// internal
//

func (it iterator) next() (obj *objReader, err error) {
	n, err := it.body.Read(it.headerBuf[:sizeofI64])
	if n == 0 {
		glog.Infof("%s: next %v", it.trname, err)
		return nil, err
	}
	if debug {
		common.Assert(n == sizeofI64)
	}
	_, hl64 := extInt64(0, it.headerBuf[:sizeofI64])
	hlen := int(hl64)
	if debug {
		common.Assert(hlen < len(it.headerBuf))
	}
	n, err = it.body.Read(it.headerBuf[:hlen])
	if n > 0 {
		if debug {
			common.Assert(n == hlen)
		}
		hdr := ExtHeader(it.headerBuf, hlen)
		if hdr.IsLast() {
			glog.Infof("%s: last", it.trname)
			return nil, io.EOF
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

//////////////////////////////////////////////////
//
// COPY-PASTE
//
//////////////////////////////////////////////////
func errHTTP(r *http.Request, msg string, status int) string {
	return http.StatusText(status) + ": " + msg + ": " + r.Method + " " + r.URL.Path
}

func invalmsghdlr(w http.ResponseWriter, r *http.Request, msg string, other ...interface{}) {
	status := http.StatusBadRequest
	if len(other) > 0 {
		status = other[0].(int)
	}
	s := errHTTP(r, msg, status)
	if _, file, line, ok := runtime.Caller(1); ok {
		if !strings.Contains(msg, ".go, #") {
			f := filepath.Base(file)
			s += fmt.Sprintf("(%s, #%d)", f, line)
		}
	}
	glog.Errorln(s)
	http.Error(w, s, status)
	// h.statsif.add("err.n", 1)
}

//
// helpers
//
func ExtHeader(body []byte, hlen int) (hdr Header) {
	var off int
	off, hdr.Bucket = extString(0, body)
	off, hdr.Objname = extString(off, body)
	off, hdr.Opaque = extByte(off, body)
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
