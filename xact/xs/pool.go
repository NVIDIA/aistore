// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

//
// Lso
//

const maxEntries = apc.MaxPageSizeAIS

var (
	lstPool = sync.Pool{
		New: func() any {
			return new(cmn.LsoEntries)
		},
	}
	entry0 cmn.LsoEnt
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

//
// COI
//

var (
	coiPool = sync.Pool{
		New: func() any {
			return new(CoiParams)
		},
	}
	coi0 CoiParams
)

func AllocCOI() *CoiParams {
	return coiPool.Get().(*CoiParams)
}

func FreeCOI(a *CoiParams) {
	*a = coi0
	coiPool.Put(a)
}

//
// Moss
//

var (
	mossPool = sync.Pool{
		New: func() any {
			return new(basewi)
		},
	}
	basewi0 basewi
)

func allocMossWi() *basewi {
	return mossPool.Get().(*basewi)
}

// TODO: reuse resp.Out cap
func freeMossWi(a *basewi) {
	a.clean.Store(true)
	rxents := a.recv.m
	*a = basewi0 //nolint:govet // reset; atomic.noCopy does not apply
	a.clean.Store(true)
	a.recv.m = rxents
	mossPool.Put(a)
}
