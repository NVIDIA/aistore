// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"sync"
)

type buffif interface {
	alloc() []byte
	free(buf []byte)
}

type buffers struct {
	pool      *sync.Pool
	fixedsize int64
}

func newbuffers(fixedsize int64) *buffers {
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, fixedsize)
		},
	}
	return &buffers{pool, fixedsize}
}

func (buffers *buffers) alloc() []byte {
	return buffers.pool.Get().([]byte)
}

func (buffers *buffers) free(buf []byte) {
	buffers.pool.Put(buf)
}
