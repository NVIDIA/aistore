// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	sendPool sync.Pool
	sobj0    Obj
)

func AllocSend() (obj *Obj) {
	if v := sendPool.Get(); v != nil {
		obj = v.(*Obj)
	} else {
		obj = &Obj{}
	}
	return
}

func freeSend(obj *Obj) { // <== sendobj & stream_bundle
	*obj = sobj0
	sendPool.Put(obj)
}

//////////////
// recvPool //
//////////////

var (
	recvPool sync.Pool
	robj0    objReader
)

func allocRecv() (obj *objReader) {
	if v := recvPool.Get(); v != nil {
		obj = v.(*objReader)
	} else {
		obj = &objReader{}
	}
	return
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
