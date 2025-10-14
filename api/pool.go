// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	msgpBufSize = 16 * cos.KiB
	numQparams  = 4 // typically, less or equal
)

var (
	qpool = sync.Pool{
		New: func() any {
			return make(url.Values, numQparams)
		},
	}

	msgpPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 0, msgpBufSize)
			return &buf
		},
	}

	reqParamPool = sync.Pool{
		New: func() any { return new(ReqParams) },
	}
	reqParams0 ReqParams
)

func qalloc() url.Values {
	return qpool.Get().(url.Values)
}

func qfree(v url.Values) {
	clear(v)
	qpool.Put(v)
}

func allocMbuf() (buf []byte) {
	v := msgpPool.Get().(*[]byte)
	buf = *v

	if cap(buf) < msgpBufSize {
		buf = make([]byte, 0, msgpBufSize)
	} else {
		buf = buf[:0]
	}
	return
}

func freeMbuf(buf []byte) { msgpPool.Put(&buf) }

func AllocRp() *ReqParams {
	return reqParamPool.Get().(*ReqParams)
}

func FreeRp(reqParams *ReqParams) {
	*reqParams = reqParams0
	reqParamPool.Put(reqParams)
}
