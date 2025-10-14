// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package cmn

import (
	"bytes"
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const maxBuffer = 4 * cos.KiB

var (
	errPool = sync.Pool{
		New: func() any { return new(ErrHTTP) },
	}
	bufPool = sync.Pool{
		New: func() any { return new(bytes.Buffer) },
	}

	err0 ErrHTTP
)

var (
	hraPool = sync.Pool{
		New: func() any { return new(HreqArgs) },
	}
	hra0 HreqArgs
)

var (
	hpool = sync.Pool{
		New: func() any { return new(http.Request) },
	}
	hmap sync.Map
	req0 http.Request
)

func AllocHra() *HreqArgs {
	return hraPool.Get().(*HreqArgs)
}

func FreeHra(a *HreqArgs) {
	*a = hra0
	hraPool.Put(a)
}

func allocHterr() *ErrHTTP {
	return errPool.Get().(*ErrHTTP)
}

func FreeHterr(a *ErrHTTP) {
	trace := a.trace
	*a = err0
	if trace != nil {
		a.trace = trace[:0]
	}
	errPool.Put(a)
}

func NewBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func FreeBuffer(buf *bytes.Buffer) {
	if buf.Cap() > maxBuffer { // too big - not keeping
		return
	}
	buf.Reset()
	bufPool.Put(buf)
}

func hreqAlloc() *http.Request {
	return hpool.Get().(*http.Request)
}

func HreqFree(r *http.Request) {
	hdr := r.Header
	*r = req0
	clear(hdr)
	r.Header = hdr
	hpool.Put(r)
}
