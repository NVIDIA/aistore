// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/debug"
)

var (
	woPool = sync.Pool{
		New: func() any {
			return new(workOrder)
		},
	}
	wo0 workOrder
)

func allocWO(op int) *workOrder {
	wo := woPool.Get().(*workOrder)
	debug.Assert(wo.op == opFree)
	wo.op = op
	return wo
}

func freeWO(wo *workOrder) {
	// work orders with SGLs are freed via wo2Free
	if wo.sgl != nil || wo.op == opFree {
		return
	}

	var batch []string
	if wo.op == opGetBatch {
		batch = wo.batch[:0]
	}

	*wo = wo0
	wo.batch = batch
	woPool.Put(wo)
}
