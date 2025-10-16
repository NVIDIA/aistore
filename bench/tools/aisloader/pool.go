// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/debug"
)

var (
	woPool = sync.Pool{
		New: func() any {
			return new(workOrder)
		},
	}
	wo0   workOrder
	moss0 apc.MossReq
)

func allocWO(op int) *workOrder {
	wo := woPool.Get().(*workOrder)
	debug.Assert(wo.op == opFree)
	wo.op = op
	return wo
}

func allocGetBatchWO(num int) *workOrder {
	wo := woPool.Get().(*workOrder)
	debug.Assert(wo.op == opFree)
	if wo.moss == nil || cap(wo.moss.In) < num {
		wo.moss = new(apc.MossReq)
		wo.moss.In = make([]apc.MossIn, num)
	}
	wo.moss.In = wo.moss.In[:num]
	wo.op = opGetBatch
	return wo
}

func freeWO(wo *workOrder) {
	// work orders with SGLs are freed via wo2Free
	if wo.sgl != nil || wo.op == opFree {
		return
	}

	if wo.op == opGetBatch {
		freeGetBatchWO(wo)
		return
	}
	*wo = wo0
	woPool.Put(wo)
}

// cleanup while retaining capacity
func freeGetBatchWO(wo *workOrder) {
	req, in := wo.moss, wo.moss.In
	*req = moss0
	for i := range in {
		in[i] = apc.MossIn{}
	}
	req.In = in[:0]
	*wo = wo0
	wo.moss = req
	woPool.Put(wo)
}
