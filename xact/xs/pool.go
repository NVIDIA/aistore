// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

const maxEntries = apc.MaxPageSizeAIS

var (
	lstPool = sync.Pool{
		New: func() any {
			return new(cmn.LsoEntries)
		},
	}
	entry0 cmn.LsoEnt
)

var (
	coiPool = sync.Pool{
		New: func() any {
			return new(CoiParams)
		},
	}
	coi0 CoiParams
)

func allocLsoEntries() cmn.LsoEntries {
	p := lstPool.Get().(*cmn.LsoEntries)
	entries := *p
	return entries
}

func freeLsoEntries(entries cmn.LsoEntries) {
	// gc
	l := min(len(entries), maxEntries)
	entries = entries[:cap(entries)]
	for i := l; i < cap(entries); i++ {
		entries[i] = nil
	}
	// truncate
	entries = entries[:l]
	// cleanup
	for _, e := range entries {
		*e = entry0
	}
	// recycle
	lstPool.Put(&entries)
}

func AllocCOI() *CoiParams {
	return coiPool.Get().(*CoiParams)
}

func FreeCOI(a *CoiParams) {
	*a = coi0
	coiPool.Put(a)
}
