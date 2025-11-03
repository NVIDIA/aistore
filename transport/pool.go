// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn/debug"
)

//////////////
// sendPool //
//////////////

var (
	sendPool = sync.Pool{
		New: func() any { return new(Obj) },
	}
	sobj0 Obj
)

func AllocSend() *Obj {
	return sendPool.Get().(*Obj)
}

func freeSend(obj *Obj) { // sendobj & stream_bundle
	*obj = sobj0
	sendPool.Put(obj)
}

//////////////
// recvPool //
//////////////

var (
	recvPool = sync.Pool{
		New: func() any { return new(objReader) },
	}
	robj0 objReader
)

func allocRecv() *objReader {
	return recvPool.Get().(*objReader)
}

func FreeRecv(object io.Reader) {
	if object == nil {
		return
	}
	obj, ok := object.(*objReader)
	debug.Assert(ok && obj != nil)
	*obj = robj0
	recvPool.Put(obj)
}
