// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/debug"
)

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
	basewi0 basewi // basewi0.clean == true via Tinit (pooled wi-s start in "already cleaned" state)
)

func allocMossWi() *basewi {
	return mossPool.Get().(*basewi)
}

// TODO: reuse resp.Out cap
func freeMossWi(a *basewi) {
	debug.Assert(basewi0.clean.Load()) // (see Tinit)
	rxents := a.recv.m
	*a = basewi0 //nolint:govet // reset; atomic.noCopy does not apply
	a.recv.m = rxents
	mossPool.Put(a)
}
