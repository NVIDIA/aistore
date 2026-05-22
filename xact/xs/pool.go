// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"
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
// TODO: support partial-and-safe mem-pooling for x-moss
//

func allocMossWi() *basewi { return &basewi{} }
func freeMossWi(*basewi)   {}
